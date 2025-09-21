# app/main.py
import os
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from .db import get_engine, SessionLocal
from .models import Base, Measurement, PriceHour
from .clients.iberdrola import IberdrolaClient
from .clients.esios import EsiosClient
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

# Config
HISTORICAL_DAYS = int(os.environ.get("HISTORICAL_DAYS", "30"))
APP_TZ_NAME = os.environ.get("APP_TZ", "Europe/Madrid")
APP_TZ = ZoneInfo(APP_TZ_NAME)

app = FastAPI()
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Create DB tables if needed (new deploy)
engine = get_engine()
Base.metadata.create_all(bind=engine)

# Clients
ib_client = IberdrolaClient()
esios_client = EsiosClient()

# WebSocket connections
connections = set()
ws_lock = asyncio.Lock()

# Poll lock
poll_lock = asyncio.Lock()

# Last measurement cache
_last_measurement = None
last_measurement_lock = asyncio.Lock()

def db_session():
    return SessionLocal()

def hour_floor_utc(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)

async def ensure_prices_for_day(day_any_tz: datetime):
    """
    Ensure buy/sell prices cached for the full local day (APP_TZ) and stored by UTC hour.
    """
    # Determine local day bounds and convert to UTC fetch window
    local_day_start = day_any_tz.astimezone(APP_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    local_day_end = local_day_start + timedelta(days=1)
    day_start_utc = local_day_start.astimezone(timezone.utc)
    day_end_utc = local_day_end.astimezone(timezone.utc)

    # Check 24 rows for that local day range
    session = db_session()
    try:
        existing_count = session.query(PriceHour).filter(
            PriceHour.hour_utc >= day_start_utc,
            PriceHour.hour_utc < day_end_utc
        ).count()
        if existing_count >= 24:
            return
    finally:
        session.close()

    # Fetch prices in UTC window
    buy_map = await esios_client.get_buy_prices_for_range(day_start_utc, day_end_utc)
    sell_map = await esios_client.get_sell_prices_for_range(day_start_utc, day_end_utc)

    session = db_session()
    try:
        hours = sorted(set(buy_map.keys()) | set(sell_map.keys()))
        for h in hours:
            buy = buy_map.get(h)
            sell = sell_map.get(h)
            row = session.query(PriceHour).get(h)
            if not row:
                row = PriceHour(hour_utc=h)
            row.price_buy_eur_per_kwh = buy
            row.price_sell_eur_per_kwh = sell
            row.source = "esios"
            session.merge(row)
        session.commit()
    finally:
        session.close()

async def get_prices_for_hour(hour_dt_utc: datetime):
    h = hour_floor_utc(hour_dt_utc)
    session = db_session()
    try:
        row = session.query(PriceHour).get(h)
    finally:
        session.close()
    if not row:
        try:
            await ensure_prices_for_day(h)
        except Exception as e:
            print("ensure_prices_for_day failed:", e)
            return None, None
        session = db_session()
        try:
            row = session.query(PriceHour).get(h)
        finally:
            session.close()
    if not row:
        return None, None
    return row.price_buy_eur_per_kwh, row.price_sell_eur_per_kwh

async def save_measurement_to_db(meas: dict):
    session = db_session()
    try:
        m = Measurement(
            timestamp=meas["timestamp"],
            power_consumption_w=meas.get("power_consumption_w", 0.0),
            power_generation_w=meas.get("power_generation_w", 0.0),
            power_battery_w=meas.get("power_battery_w", 0.0),
            total_consumption_w=meas.get("total_consumption_w", 0.0),
            energy_consumed_wh=meas.get("energy_consumed_wh", 0.0),
            energy_generated_wh=meas.get("energy_generated_wh", 0.0),
            energy_exported_wh=meas.get("energy_exported_wh", 0.0),
            price_sell_eur_per_kwh=meas.get("price_sell_eur_per_kwh"),
            price_buy_eur_per_kwh=meas.get("price_buy_eur_per_kwh")
        )
        session.add(m)
        session.commit()
    finally:
        session.close()

async def poll_once(interval_seconds=3):
    """
    Fetch realtime, compute interval energy (Wh), store and return the measurement dict.
    """
    global _last_measurement
    async with poll_lock:
        rt = await ib_client.get_realtime()
        ts = rt["timestamp"]
        main_w = float(rt["main_w"] or 0.0)          # >0 import, <0 export
        solar_w = float(rt["solar_w"] or 0.0)
        battery_w = float(rt["battery_w"] or 0.0)
        total_w = float(rt["total_consumption_w"] or 0.0)

        seconds = interval_seconds

        # Split grid exchange by sign so Wh are always >= 0
        import_w = max(0.0, main_w)
        export_w = max(0.0, -main_w)

        energy_consumed_wh = import_w * (seconds / 3600.0)
        energy_exported_wh = export_w * (seconds / 3600.0)
        energy_generated_wh = solar_w  * (seconds / 3600.0)

        hour = ts.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
        price_buy, price_sell = await get_prices_for_hour(hour)

        meas = {
            "timestamp": ts,
            "power_consumption_w": main_w,
            "power_generation_w": solar_w,
            "power_battery_w": battery_w,
            "total_consumption_w": total_w,
            "energy_consumed_wh": energy_consumed_wh,   # >= 0 (grid import)
            "energy_generated_wh": energy_generated_wh, # >= 0 (solar)
            "energy_exported_wh": energy_exported_wh,   # >= 0 (grid export)
            "price_buy_eur_per_kwh": price_buy,
            "price_sell_eur_per_kwh": price_sell
        }

        await save_measurement_to_db(meas)
        async with last_measurement_lock:
            _last_measurement = meas
        return meas

async def historical_bootstrap(days=30):
    """
    Fetch historical samples and store them in Wh.
    """
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    time_unit = "hours"

    chunk_days = 7
    current_start = start
    while current_start < end:
        current_end = min(end, current_start + timedelta(days=chunk_days))
        start_iso = current_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_iso = current_end.strftime("%Y-%m-%dT%H:%M:%SZ")
        samples = await ib_client.get_historical(start_iso, end_iso, time_unit=time_unit)

        ensured_days = set()
        for i in range(len(samples)):
            s = samples[i]
            ts = s["timestamp"]
            if i + 1 < len(samples):
                next_ts = samples[i + 1]["timestamp"]
                delta_s = (next_ts - ts).total_seconds()
            else:
                delta_s = 3600

            # seconds for the interval
            cons_wh_from_import = max(0.0, float(s["main_w"])) * (delta_s / 3600.0)
            exp_wh_from_export  = max(0.0, -float(s["main_w"])) * (delta_s / 3600.0)
            gen_wh              = max(0.0, float(s["solar_w"])) * (delta_s / 3600.0)

            local_day = ts.astimezone(APP_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
            if local_day not in ensured_days:
                try:
                    await ensure_prices_for_day(local_day)
                except Exception as e:
                    print("ensure_prices_for_day (historical) failed:", e)
                ensured_days.add(local_day)

            hour_utc = ts.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
            price_buy, price_sell = await get_prices_for_hour(hour_utc)

            meas = {
                "timestamp": ts,
                "power_consumption_w": s["main_w"],
                "power_generation_w": s["solar_w"],
                "power_battery_w": s.get("battery_w", 0.0),
                "total_consumption_w": s.get("total_consumption_w", 0.0),
                "energy_consumed_wh": cons_wh_from_import,
                "energy_generated_wh": gen_wh,
                "energy_exported_wh": exp_wh_from_export,
                "price_buy_eur_per_kwh": price_buy,
                "price_sell_eur_per_kwh": price_sell
            }
            await save_measurement_to_db(meas)
        current_start = current_end

async def hourly_task():
    while True:
        try:
            await poll_once(interval_seconds=3600)
        except Exception as e:
            print("Hourly poll failed:", e)
        await asyncio.sleep(3600)

async def active_poller():
    while True:
        try:
            if len(connections) > 0:
                meas = await poll_once(interval_seconds=3)
                if meas:
                    await broadcast({"type": "measurement", "data": measurement_to_json(meas)})
                await asyncio.sleep(3)
            else:
                await asyncio.sleep(1)
        except Exception as e:
            print("Active poller error:", e)
            await asyncio.sleep(1)

def local_day_bounds(dt_any: datetime):
    # Return the start/end of the local day (APP_TZ), as UTC instants for querying
    local = dt_any.astimezone(APP_TZ)
    start_local = local.replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc), start_local, end_local

@app.get("/api/daily-summary")
async def api_daily_summary(day: str | None = None):
    """
    Daily totals for the local day (APP_TZ).
    - energy_* in Wh
    - cost_eur is negative (money paid to buy): -(kWh_buy * price_buy)
    - revenue_eur may be negative (paying to export)
    - net_eur:
        if revenue < 0:  abs(cost) + abs(revenue)
        else:            abs(cost) - abs(revenue)
    """
    if day:
        try:
            y, m, d = map(int, day.split("-"))
            base = datetime(y, m, d, tzinfo=APP_TZ)
        except Exception:
            base = datetime.now(APP_TZ)
    else:
        base = datetime.now(APP_TZ)

    start_utc, end_utc, start_local, end_local = local_day_bounds(base)

    session = SessionLocal()
    try:
        rows = session.query(Measurement).filter(
            Measurement.timestamp >= start_utc,
            Measurement.timestamp < end_utc
        ).all()
        purchased_wh = 0.0
        sold_wh = 0.0
        cost_eur = 0.0
        revenue_eur = 0.0
        for r in rows:
            cw = float(r.energy_consumed_wh or 0.0)
            sw = float(r.energy_exported_wh or 0.0)
            pb = r.price_buy_eur_per_kwh
            ps = r.price_sell_eur_per_kwh
            purchased_wh += cw
            sold_wh += sw
            if pb is not None:
                cost_eur += -((cw / 1000.0) * pb)  # negative cost
            if ps is not None:
                revenue_eur += (sw / 1000.0) * ps  # may be negative

        if revenue_eur < 0:
            net_eur = abs(cost_eur) + abs(revenue_eur)
        else:
            net_eur = abs(cost_eur) - abs(revenue_eur)

        return {
            "day_start_local": start_local.isoformat(),
            "day_end_local": end_local.isoformat(),
            "energy_purchased_wh": round(purchased_wh, 3),
            "energy_sold_wh": round(sold_wh, 3),
            "cost_eur": round(cost_eur, 4),
            "revenue_eur": round(revenue_eur, 4),
            "net_eur": round(net_eur, 4)
        }
    finally:
        session.close()

@app.get("/api/daily-prices")
async def api_daily_prices(day: str | None = None):
    """
    Hourly buy/sell prices for the local day (APP_TZ).
    Returns UTC hour, ISO local hour (+offset), and a naive local string for safe frontend parsing.
    """
    if day:
        try:
            y, m, d = map(int, day.split("-"))
            base = datetime(y, m, d, tzinfo=APP_TZ)
        except Exception:
            base = datetime.now(APP_TZ)
    else:
        base = datetime.now(APP_TZ)

    start_utc, end_utc, start_local, _ = local_day_bounds(base)

    try:
        await ensure_prices_for_day(start_local)
    except Exception as e:
        print("ensure_prices_for_day (daily-prices) failed:", e)

    session = SessionLocal()
    try:
        rows = (
            session.query(PriceHour)
            .filter(PriceHour.hour_utc >= start_utc, PriceHour.hour_utc < end_utc)
            .order_by(PriceHour.hour_utc.asc())
            .all()
        )
        out = []
        for r in rows:
            # Force UTC tzinfo if stored naive, then convert to local
            h_utc = r.hour_utc
            if h_utc.tzinfo is None:
                h_utc = h_utc.replace(tzinfo=timezone.utc)
            else:
                h_utc = h_utc.astimezone(timezone.utc)

            hour_local = h_utc.astimezone(APP_TZ)
            out.append({
                "hour_utc": h_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "hour_local": hour_local.isoformat(),
                "hour_local_naive": hour_local.strftime("%Y-%m-%dT%H:%M:%S"),
                "price_buy_eur_per_kwh": r.price_buy_eur_per_kwh,
                "price_sell_eur_per_kwh": r.price_sell_eur_per_kwh
            })
        return out
    finally:
        session.close()

async def broadcast(obj):
    async with ws_lock:
        to_remove = []
        for ws in list(connections):
            try:
                await ws.send_json(obj)
            except Exception:
                to_remove.append(ws)
        for ws in to_remove:
            try:
                connections.remove(ws)
            except Exception:
                pass

def measurement_to_json(meas: dict):
    out = dict(meas)
    ts = out["timestamp"]
    out["timestamp"] = ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return out

@app.on_event("startup")
async def startup():
    # Bootstrap if DB empty
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
    # Pre-cache today prices (local day)
    try:
        await ensure_prices_for_day(datetime.now(APP_TZ))
    except Exception as e:
        print("Price pre-cache failed:", e)
    loop = asyncio.get_event_loop()
    loop.create_task(hourly_task())
    loop.create_task(active_poller())

@app.on_event("shutdown")
async def shutdown():
    await ib_client.close()
    await esios_client.close()

@app.get("/api/latest")
async def api_latest():
    session = SessionLocal()
    try:
        m = session.query(Measurement).order_by(Measurement.timestamp.desc()).first()
        if not m:
            return JSONResponse(status_code=204, content={})
        hour_utc = m.timestamp.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
        price_buy, price_sell = await get_prices_for_hour(hour_utc)
        return {
            "timestamp": m.timestamp.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "power_consumption_w": m.power_consumption_w,
            "power_generation_w": m.power_generation_w,
            "power_battery_w": m.power_battery_w,
            "total_consumption_w": m.total_consumption_w,
            "energy_consumed_wh": m.energy_consumed_wh,
            "energy_generated_wh": m.energy_generated_wh,
            "energy_exported_wh": m.energy_exported_wh,
            "price_buy_eur_per_kwh": price_buy,
            "price_sell_eur_per_kwh": price_sell
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
            hour_utc = m.timestamp.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
            price_buy, price_sell = await get_prices_for_hour(hour_utc)
            out.append({
                "timestamp": m.timestamp.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "power_consumption_w": m.power_consumption_w,
                "power_generation_w": m.power_generation_w,
                "power_battery_w": m.power_battery_w,
                "total_consumption_w": m.total_consumption_w,
                "energy_consumed_wh": m.energy_consumed_wh,
                "energy_generated_wh": m.energy_generated_wh,
                "energy_exported_wh": m.energy_exported_wh,
                "price_buy_eur_per_kwh": price_buy,
                "price_sell_eur_per_kwh": price_sell
            })
        return out
    finally:
        session.close()

@app.websocket("/ws/updates")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    async with ws_lock:
        connections.add(ws)
    # Send latest measurement right away (so flow/prices update even before active poller)
    try:
        session = SessionLocal()
        try:
            m = session.query(Measurement).order_by(Measurement.timestamp.desc()).first()
            if m:
                hour_utc = m.timestamp.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
                price_buy, price_sell = await get_prices_for_hour(hour_utc)
                initial = {
                    "timestamp": m.timestamp.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "power_consumption_w": m.power_consumption_w,
                    "power_generation_w": m.power_generation_w,
                    "power_battery_w": m.power_battery_w,
                    "total_consumption_w": m.total_consumption_w,
                    "energy_consumed_wh": m.energy_consumed_wh,
                    "energy_generated_wh": m.energy_generated_wh,
                    "energy_exported_wh": m.energy_exported_wh,
                    "price_buy_eur_per_kwh": price_buy,
                    "price_sell_eur_per_kwh": price_sell
                }
                await ws.send_json({"type": "measurement", "data": initial})
        finally:
            session.close()
    except Exception as e:
        print("WS initial send failed:", e)

    try:
        while True:
            # Keep the connection alive; ignore any client pings
            try:
                await ws.receive_text()
            except Exception:
                break
    except WebSocketDisconnect:
        pass
    finally:
        async with ws_lock:
            if ws in connections:
                connections.remove(ws)

@app.get("/")
async def index():
    return FileResponse("app/static/index.html")
