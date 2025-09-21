# app/main.py
import os
import asyncio
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Response
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from .db import get_engine, SessionLocal
from .models import Base, Measurement, PriceHour
from .clients.iberdrola import IberdrolaClient
from .clients.esios import EsiosClient
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

# Config
HISTORICAL_DAYS = int(os.environ.get("HISTORICAL_DAYS", "730"))  # default: last 2 years
APP_TZ_NAME = os.environ.get("APP_TZ", "Europe/Madrid")
APP_TZ = ZoneInfo(APP_TZ_NAME)

# Logging
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger("monitor_solar")

app = FastAPI()
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Create DB tables if needed (new deploy)
engine = get_engine()
Base.metadata.create_all(bind=engine)

# Clients
ib_client = IberdrolaClient()
try:
    esios_client = EsiosClient()
except Exception as e:
    esios_client = None
    logger.warning("ESIOS client disabled: %s", e)

# WebSocket connections
connections = set()
ws_lock = asyncio.Lock()

# Poll lock
poll_lock = asyncio.Lock()

# Last measurement cache
_last_measurement = None
last_measurement_lock = asyncio.Lock()

# One-at-a-time bootstrap
bootstrap_lock = asyncio.Lock()

def db_session():
    return SessionLocal()

def hour_floor_utc(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)

async def ensure_prices_for_day(day_any_tz: datetime):
    """
    Ensure buy/sell prices cached for the full local day (APP_TZ) and stored by UTC hour.
    """
    if esios_client is None:
        return

    local_day_start = day_any_tz.astimezone(APP_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    local_day_end = local_day_start + timedelta(days=1)
    day_start_utc = local_day_start.astimezone(timezone.utc)
    day_end_utc = local_day_end.astimezone(timezone.utc)

    session = db_session()
    try:
        existing_count = session.query(PriceHour).filter(
            PriceHour.hour_utc >= day_start_utc,
            PriceHour.hour_utc < day_end_utc
        ).count()
        if existing_count >= 23:  # handle DST days (23/25h)
            return
    finally:
        session.close()

    try:
        buy_map = await esios_client.get_buy_prices_for_range(day_start_utc, day_end_utc)
        sell_map = await esios_client.get_sell_prices_for_range(day_start_utc, day_end_utc)
    except Exception as e:
        logger.warning("Failed to fetch ESIOS prices: %s", e)
        return

    session = db_session()
    try:
        hours = sorted(set(buy_map.keys()) | set(sell_map.keys()))
        for h in hours:
            buy = buy_map.get(h)
            sell = sell_map.get(h)
            row = session.get(PriceHour, h)
            if not row:
                row = PriceHour(hour_utc=h)
            row.price_buy_eur_per_kwh = buy
            row.price_sell_eur_per_kwh = sell
            row.source = "esios"
            session.merge(row)
        session.commit()
    finally:
        session.close()

async def ensure_prices_for_range(start_local_day: datetime, end_local_day: datetime):
    """
    Ensure buy/sell prices cached for [start_local_day 00:00, end_local_day 00:00) in APP_TZ.
    """
    if esios_client is None:
        return

    start_local = start_local_day.astimezone(APP_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = end_local_day.astimezone(APP_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    if end_local <= start_local:
        end_local = start_local + timedelta(days=1)

    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)

    # Fetch once for the entire range
    try:
        buy_map = await esios_client.get_buy_prices_for_range(start_utc, end_utc)
        sell_map = await esios_client.get_sell_prices_for_range(start_utc, end_utc)
    except Exception as e:
        logger.warning("Failed to fetch ESIOS prices for range: %s", e)
        return

    session = db_session()
    try:
        hours = sorted(set(buy_map.keys()) | set(sell_map.keys()))
        for h in hours:
            buy = buy_map.get(h)
            sell = sell_map.get(h)
            row = session.get(PriceHour, h)
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
        row = session.get(PriceHour, h)
    finally:
        session.close()
    if not row:
        try:
            await ensure_prices_for_day(h)
        except Exception as e:
            logger.warning("ensure_prices_for_day failed: %s", e)
            return None, None
        session = db_session()
        try:
            row = session.get(PriceHour, h)
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

async def save_measurements_bulk(meas_list: list[dict]):
    if not meas_list:
        return
    session = db_session()
    try:
        objs = []
        for meas in meas_list:
            objs.append(Measurement(
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
            ))
        session.add_all(objs)
        session.commit()
    finally:
        session.close()

async def poll_once(interval_seconds: float | None = 3):
    """
    Fetch realtime, compute interval energy (Wh), store and return the measurement dict.
    If interval_seconds is None, auto-compute from previous timestamp (clamped 1..3600s).
    """
    global _last_measurement
    async with poll_lock:
        rt = await ib_client.get_realtime()
        ts = rt["timestamp"]
        main_w = float(rt["main_w"] or 0.0)          # >0 import, <0 export
        solar_w = float(rt["solar_w"] or 0.0)
        battery_w = float(rt["battery_w"] or 0.0)
        total_w = float(rt["total_consumption_w"] or 0.0)

        seconds = None
        if interval_seconds is not None:
            seconds = float(interval_seconds)
        else:
            prev_ts = None
            async with last_measurement_lock:
                if _last_measurement:
                    prev_ts = _last_measurement.get("timestamp")
            if prev_ts:
                seconds = (ts - prev_ts).total_seconds()
                if not (seconds > 0):
                    seconds = 3.0
            else:
                seconds = 3.0
        # Clamp to sane bounds
        seconds = max(1.0, min(3600.0, seconds))

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

async def historical_bootstrap(days=HISTORICAL_DAYS):
    """
    Download historical hourly data for the last N days (default ~2 years) and store as Wh.
    Prefetches price ranges per chunk to speed up.
    """
    end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(days=days)
    chunk_days = 30

    current_start = start
    while current_start < end:
        current_end = min(end, current_start + timedelta(days=chunk_days))

        # Prefetch ESIOS prices covering the local-day span for this chunk
        start_local_day = current_start.astimezone(APP_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
        end_local_day = current_end.astimezone(APP_TZ).replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        try:
            await ensure_prices_for_range(start_local_day, end_local_day)
        except Exception as e:
            logger.warning("ensure_prices_for_range failed: %s", e)

        # Fetch Iberdrola historical for this chunk
        samples = await ib_client.get_historical_range(current_start, current_end, time_unit="hours")
        if not samples:
            logger.info("No samples for %s..%s", current_start, current_end)
            current_start = current_end
            continue

        # Build measurements using Iberdrola Wh if provided; otherwise integrate avg W over Δt
        meas_list = []
        for i in range(len(samples)):
            s = samples[i]
            ts = s["timestamp"]

            if i + 1 < len(samples):
                next_ts = samples[i + 1]["timestamp"]
                delta_s = max(1.0, (next_ts - ts).total_seconds())
            else:
                delta_s = 3600.0

            # Prefer explicit Wh from API
            cons_wh_from_import = s.get("purchased_wh")
            exp_wh_from_export  = s.get("exported_wh")
            gen_wh              = s.get("generated_wh")

            if cons_wh_from_import is None or exp_wh_from_export is None or gen_wh is None:
                main_w = float(s.get("main_w", 0.0))
                solar_w = float(s.get("solar_w", 0.0))
                cons_wh_from_import = cons_wh_from_import if cons_wh_from_import is not None else max(0.0, main_w) * (delta_s / 3600.0)
                exp_wh_from_export  = exp_wh_from_export  if exp_wh_from_export  is not None else max(0.0, -main_w) * (delta_s / 3600.0)
                gen_wh              = gen_wh              if gen_wh              is not None else max(0.0, solar_w) * (delta_s / 3600.0)

            main_w = float(s.get("main_w", (cons_wh_from_import or 0.0) - (exp_wh_from_export or 0.0)))
            solar_w = float(s.get("solar_w", gen_wh or 0.0))
            battery_w = float(s.get("battery_w", (s.get("from_battery_wh") or 0.0) - (s.get("to_battery_wh") or 0.0)))
            total_w = float(s.get("total_consumption_w", s.get("total_consumption_wh") or 0.0))

            hour_utc = ts.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
            price_buy, price_sell = await get_prices_for_hour(hour_utc)

            meas_list.append({
                "timestamp": ts,
                "power_consumption_w": main_w,
                "power_generation_w": solar_w,
                "power_battery_w": battery_w,
                "total_consumption_w": total_w,
                "energy_consumed_wh": cons_wh_from_import,
                "energy_generated_wh": gen_wh,
                "energy_exported_wh": exp_wh_from_export,
                "price_buy_eur_per_kwh": price_buy,
                "price_sell_eur_per_kwh": price_sell
            })

        await save_measurements_bulk(meas_list)
        logger.info("Imported %d samples for %s..%s", len(meas_list), current_start.date(), (current_end - timedelta(seconds=1)).date())
        current_start = current_end

async def hourly_task():
    while True:
        try:
            # Let poller auto-compute delta to avoid overcounting
            await poll_once(interval_seconds=None)
        except Exception as e:
            logger.error("Hourly poll failed: %s", e)
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
            logger.error("Active poller error: %s", e)
            await asyncio.sleep(1)

def local_day_bounds(dt_any: datetime):
    local = dt_any.astimezone(APP_TZ)
    start_local = local.replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc), start_local, end_local

async def serialize_measurement(m: Measurement) -> dict:
    """
    Uniform payload for REST/WS responses.
    Prefer stored prices; fallback to cache lookup.
    """
    pb = m.price_buy_eur_per_kwh
    ps = m.price_sell_eur_per_kwh
    if pb is None or ps is None:
        hour_utc = m.timestamp.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
        pb2, ps2 = await get_prices_for_hour(hour_utc)
        pb = pb if pb is not None else pb2
        ps = ps if ps is not None else ps2
    return {
        "timestamp": m.timestamp.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "power_consumption_w": m.power_consumption_w,
        "power_generation_w": m.power_generation_w,
        "power_battery_w": m.power_battery_w,
        "total_consumption_w": m.total_consumption_w,
        "energy_consumed_wh": m.energy_consumed_wh,
        "energy_generated_wh": m.energy_generated_wh,
        "energy_exported_wh": m.energy_exported_wh,
        "price_buy_eur_per_kwh": pb,
        "price_sell_eur_per_kwh": ps
    }

@app.get("/api/daily-summary")
async def api_daily_summary(day: str | None = None):
    """
    Daily totals for the local day (APP_TZ).
    - energy_* in Wh
    - cost_eur is negative (money paid to buy): -(kWh_buy * price_buy)
    - revenue_eur may be negative (paying to export)
    - net_eur: abs(cost) -/+ revenue depending on sign
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
        logger.warning("ensure_prices_for_day (daily-prices) failed: %s", e)

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
    ts = out.get("timestamp")
    if isinstance(ts, datetime):
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
    if not exists and HISTORICAL_DAYS > 0:
        logger.info("Bootstrapping historical data for last %s days...", HISTORICAL_DAYS)
        try:
            async with bootstrap_lock:
                await historical_bootstrap(days=HISTORICAL_DAYS)
            logger.info("Historical bootstrap complete.")
        except Exception as e:
            logger.error("Historical bootstrap failed: %s", e)
    # Pre-cache today prices (local day) if client is available
    if esios_client is not None:
        try:
            await ensure_prices_for_range(datetime.now(APP_TZ), datetime.now(APP_TZ) + timedelta(days=1))
        except Exception as e:
            logger.warning("Price pre-cache failed: %s", e)

    asyncio.create_task(hourly_task())
    asyncio.create_task(active_poller())

@app.on_event("shutdown")
async def shutdown():
    await ib_client.close()
    if esios_client is not None:
        await esios_client.close()

@app.get("/health")
async def health():
    return {"ok": True}

@app.get("/api/latest")
async def api_latest():
    session = SessionLocal()
    try:
        m = session.query(Measurement).order_by(Measurement.timestamp.desc()).first()
        if not m:
            return Response(status_code=204)
        return await serialize_measurement(m)
    finally:
        session.close()

@app.get("/api/history")
async def api_history(limit: int = 200):
    limit = max(1, min(limit, 5000))
    session = SessionLocal()
    try:
        rows = session.query(Measurement).order_by(Measurement.timestamp.desc()).limit(limit).all()
        return [await serialize_measurement(m) for m in rows]
    finally:
        session.close()

@app.websocket("/ws/updates")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    async with ws_lock:
        connections.add(ws)
    # Send latest measurement right away
    try:
        session = SessionLocal()
        try:
            m = session.query(Measurement).order_by(Measurement.timestamp.desc()).first()
            if m:
                initial = await serialize_measurement(m)
                await ws.send_json({"type": "measurement", "data": initial})
        finally:
            session.close()
    except Exception as e:
        logger.warning("WS initial send failed: %s", e)

    try:
        while True:
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

@app.post("/api/bootstrap")
async def api_bootstrap(days: int = 730, force: bool = False):
    """
    Manually trigger historical bootstrap.
    - days: how many days back from now (default 730 = ~2y)
    - force: run even if DB already has data
    """
    if days <= 0:
        return JSONResponse({"ok": False, "error": "days must be > 0"}, status_code=400)

    session = SessionLocal()
    try:
        has_data = session.query(Measurement).first() is not None
    finally:
        session.close()
    if has_data and not force:
        return JSONResponse({"ok": False, "error": "DB not empty; pass force=true to override"}, status_code=409)

    async with bootstrap_lock:
        await historical_bootstrap(days=days)
    return {"ok": True, "imported_days": days}
