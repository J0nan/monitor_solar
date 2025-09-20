# app/main.py
import os
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from .db import get_engine, SessionLocal
from .models import Base, Measurement
from .clients.iberdrola import IberdrolaClient
from .clients.esios import EsiosClient
from datetime import datetime, timezone, timedelta

# Config
HISTORICAL_DAYS = int(os.environ.get("HISTORICAL_DAYS", "30"))
POLL_5S_ENABLED = True

app = FastAPI()
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Create DB tables if needed
engine = get_engine()
Base.metadata.create_all(bind=engine)

# Clients
ib_client = IberdrolaClient()
esios_client = EsiosClient()

# WebSocket connections
connections = set()
ws_lock = asyncio.Lock()

# Poll lock ensures only one poll occurs at a time in this process
poll_lock = asyncio.Lock()

# Keep last measurement in memory for quick broadcasts
_last_measurement = None
last_measurement_lock = asyncio.Lock()

# Utility functions
def db_session():
    return SessionLocal()

async def save_measurement_to_db(meas):
    """
    meas is dict with keys:
      timestamp (datetime UTC),
      main_w, solar_w, battery_w, total_consumption_w,
      energy_consumed_kwh, energy_generated_kwh, energy_exported_kwh,
      price_eur_per_kwh
    """
    session = db_session()
    try:
        m = Measurement(
            timestamp=meas["timestamp"],
            power_consumption_w=meas.get("main_w", 0.0),
            power_generation_w=meas.get("solar_w", 0.0),
            power_battery_w=meas.get("battery_w", 0.0),
            total_consumption_w=meas.get("total_consumption_w", 0.0),
            energy_consumed_kwh=meas.get("energy_consumed_kwh", 0.0),
            energy_generated_kwh=meas.get("energy_generated_kwh", 0.0),
            energy_exported_kwh=meas.get("energy_exported_kwh", 0.0),
            price_eur_per_kwh=meas.get("price_eur_per_kwh")
        )
        session.add(m)
        session.commit()
    finally:
        session.close()

async def fetch_prices_for_hour(hour_dt):
    """
    hour_dt: datetime (UTC) aligned to hour
    returns price €/kWh (float) or None
    """
    prices = await esios_client.get_pvpc_for_range(hour_dt, hour_dt + timedelta(hours=1))
    return prices.get(hour_dt)

async def poll_once(interval_seconds=5):
    """
    Single poll that fetches realtime data, computes energy delta since last saved sample
    using interval_seconds as the duration (for realtime). For historical imports the
    importer will compute energy from sample timestamps directly.
    """
    global _last_measurement
    async with poll_lock:
        now = datetime.now(timezone.utc)
        rt = await ib_client.get_realtime()
        # rt contains timestamp and instantaneous powers
        ts = rt["timestamp"]
        main_w = rt["main_w"]
        solar_w = rt["solar_w"]
        battery_w = rt["battery_w"]
        total_w = rt["total_consumption_w"]

        # compute kWh for this interval
        # kWh = (W / 1000) * (seconds / 3600)
        seconds = interval_seconds
        energy_consumed = (main_w / 1000.0) * (seconds / 3600.0)
        energy_generated = (solar_w / 1000.0) * (seconds / 3600.0)
        # exported = generated - consumed_by_house (approx); more accurate if meter provides exported explicitly,
        # but we approximate exported as positive generation > consumption:
        exported = max(0.0, energy_generated - energy_consumed)

        # price for the hour containing ts
        hour = ts.replace(minute=0, second=0, microsecond=0)
        price = await fetch_prices_for_hour(hour)

        meas = {
            "timestamp": ts,
            "main_w": main_w,
            "solar_w": solar_w,
            "battery_w": battery_w,
            "total_consumption_w": total_w,
            "energy_consumed_kwh": energy_consumed,
            "energy_generated_kwh": energy_generated,
            "energy_exported_kwh": exported,
            "price_eur_per_kwh": price
        }

        # store
        await save_measurement_to_db(meas)

        # update last measurement memory
        async with last_measurement_lock:
            _last_measurement = meas

        return meas

async def historical_bootstrap(days=30):
    """
    Fetch historical samples from Iberdrola and store them.
    We will request in chunks (max 7 days per request when using minutes/quarters to avoid enormous payloads).
    time_unit choice:
      - if days > 365 -> months/day
      - if range <= 60 days -> minutes allowed (but careful); we'll request 'hours' by default for robustness.
    We'll fetch in day-chunks and request time_unit='hours' to get hourly samples which are safe and available.
    """
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)

    # choose time_unit: 'hours' is a safe default (minutes only allowed for last 2 months)
    time_unit = "hours"

    chunk_days = 7
    current_start = start
    while current_start < end:
        current_end = min(end, current_start + timedelta(days=chunk_days))
        start_iso = current_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_iso = current_end.strftime("%Y-%m-%dT%H:%M:%SZ")
        print(start_iso, flush=True)
        print(end_iso, flush=True)
        samples = await ib_client.get_historical(start_iso, end_iso, time_unit=time_unit)
        # samples is list of {'timestamp','main_w','solar_w',...}; compute kWh by integrating between consecutive timestamps
        for i in range(len(samples)):
            s = samples[i]
            ts = s["timestamp"]
            # For interval length: if next sample exists use difference, else approximate by 1 hour
            if i + 1 < len(samples):
                next_ts = samples[i + 1]["timestamp"]
                delta_s = (next_ts - ts).total_seconds()
            else:
                delta_s = 3600  # fallback 1 hour
            energy_consumed = (s["main_w"] / 1000.0) * (delta_s / 3600.0)
            energy_generated = (s["solar_w"] / 1000.0) * (delta_s / 3600.0)
            exported = max(0.0, energy_generated - energy_consumed)
            hour = ts.replace(minute=0, second=0, microsecond=0)
            price = await fetch_prices_for_hour(hour)
            meas = {
                "timestamp": ts,
                "main_w": s["main_w"],
                "solar_w": s["solar_w"],
                "battery_w": s.get("battery_w", 0.0),
                "total_consumption_w": s.get("total_consumption_w", 0.0),
                "energy_consumed_kwh": energy_consumed,
                "energy_generated_kwh": energy_generated,
                "energy_exported_kwh": exported,
                "price_eur_per_kwh": price
            }
            await save_measurement_to_db(meas)
        current_start = current_end

# Background tasks
async def hourly_task():
    while True:
        try:
            await poll_once(interval_seconds=3600)
        except Exception as e:
            print("Hourly poll failed:", e)
        await asyncio.sleep(3600)

async def active_poller():
    """
    While any WebSocket client is connected, poll every 5s and broadcast results.
    """
    while True:
        try:
            if len(connections) > 0 and POLL_5S_ENABLED:
                meas = await poll_once(interval_seconds=5)
                # broadcast
                if meas:
                    await broadcast({"type": "measurement", "data": measurement_to_json(meas)})
                await asyncio.sleep(5)
            else:
                await asyncio.sleep(1)
        except Exception as e:
            print("Active poller error:", e)
            await asyncio.sleep(1)

async def broadcast(obj):
    text = obj
    async with ws_lock:
        to_remove = []
        for ws in list(connections):
            try:
                await ws.send_json(text)
            except Exception:
                to_remove.append(ws)
        for ws in to_remove:
            try:
                connections.remove(ws)
            except Exception:
                pass

def measurement_to_json(meas):
    # Convert datetimes to iso strings
    out = dict(meas)
    ts = out["timestamp"]
    out["timestamp"] = ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    return out

@app.on_event("startup")
async def startup():
    # Start clients automatically created above
    # Bootstrap historical if DB empty
    session = SessionLocal()
    try:
        exists = session.query(Measurement).first()
    finally:
        session.close()
    if not exists:
        days = HISTORICAL_DAYS
        print(f"Bootstrapping historical data for last {days} days...")
        try:
            await historical_bootstrap(days=days)
            print("Historical bootstrap complete.")
        except Exception as e:
            print("Historical bootstrap failed:", e)

    # start background loops
    loop = asyncio.get_event_loop()
    loop.create_task(hourly_task())
    loop.create_task(active_poller())

@app.on_event("shutdown")
async def shutdown():
    await ib_client.close()
    await esios_client.close()

# API endpoints
@app.get("/api/latest")
async def api_latest():
    session = SessionLocal()
    try:
        m = session.query(Measurement).order_by(Measurement.timestamp.desc()).first()
        if not m:
            return JSONResponse(status_code=204, content={})
        return {
            "timestamp": m.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "power_consumption_w": m.power_consumption_w,
            "power_generation_w": m.power_generation_w,
            "power_battery_w": m.power_battery_w,
            "energy_consumed_kwh": m.energy_consumed_kwh,
            "energy_generated_kwh": m.energy_generated_kwh,
            "energy_exported_kwh": m.energy_exported_kwh,
            "price_eur_per_kwh": m.price_eur_per_kwh
        }
    finally:
        session.close()

@app.get("/api/history")
async def api_history(limit: int = 200):
    session = SessionLocal()
    try:
        rows = session.query(Measurement).order_by(Measurement.timestamp.desc()).limit(limit).all()
        out = []
        for m in rows:
            out.append({
                "timestamp": m.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "power_consumption_w": m.power_consumption_w,
                "power_generation_w": m.power_generation_w,
                "energy_consumed_kwh": m.energy_consumed_kwh,
                "energy_generated_kwh": m.energy_generated_kwh,
                "energy_exported_kwh": m.energy_exported_kwh,
                "price_eur_per_kwh": m.price_eur_per_kwh
            })
        return out
    finally:
        session.close()

@app.websocket("/ws/updates")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    async with ws_lock:
        connections.add(ws)
        # send last measurement if present
        async with last_measurement_lock:
            if _last_measurement:
                try:
                    await ws.send_json({"type":"measurement", "data": measurement_to_json(_last_measurement)})
                except:
                    pass
    try:
        while True:
            # receive keepalive messages from client; if client disconnects this will raise
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        async with ws_lock:
            if ws in connections:
                connections.remove(ws)

# Serve simple frontend
@app.get("/")
async def index():
    return FileResponse("app/static/index.html")
