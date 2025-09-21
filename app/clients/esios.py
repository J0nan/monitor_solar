# app/clients/esios.py
import aiohttp
import os
from datetime import datetime, timezone

ESIOS_BASE = "https://api.esios.ree.es/"

ESIOS_API_KEY = os.environ.get("ESIOS_API_KEY")
ESIOS_INDICATOR_ID = int(os.environ.get("ESIOS_INDICATOR_ID", "1001"))

class EsiosClient:
    def __init__(self):
        if not ESIOS_API_KEY:
            raise ValueError("ESIOS_API_KEY environment variable is not set")
        timeout = aiohttp.ClientTimeout(total=15)
        self.session = aiohttp.ClientSession(headers={
            "Accept": "application/json; application/vnd.esios-api-v2+json",
            "Content-Type": "application/json",
            "x-api-key": ESIOS_API_KEY,
        }, timeout=timeout)

    async def close(self):
        await self.session.close()

    async def _get_indicator_for_range(self, indicator_id: int, start_dt, end_dt):
        """
        Generic fetch for an indicator over [start_dt, end_dt).
        Returns dict: {hour_dt_utc: price_eur_per_kwh}
        """
        params = {
            "start_date": start_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_date": end_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "time_trunc": "hour",
        }
        url = ESIOS_BASE + f"indicators/{indicator_id}"
        async with self.session.get(url, params=params) as resp:
            resp.raise_for_status()
            js = await resp.json(content_type=None)

        vals = js.get("indicator", {}).get("values") or js.get("values") or []
        out = {}
        for entry in vals:
            dt = entry.get("datetime") or entry.get("date")
            if not dt:
                continue
            if dt.endswith("Z"):
                dt = dt.replace("Z", "+00:00")
            try:
                dt_obj = datetime.fromisoformat(dt).astimezone(timezone.utc)
            except Exception:
                continue
            hour = dt_obj.replace(minute=0, second=0, microsecond=0)
            val = entry.get("value")
            if val is None:
                continue
            try:
                price = float(val) / 1000.0  # €/MWh => €/kWh
            except Exception:
                try:
                    price = float(val)
                except Exception:
                    continue
            out[hour] = price
        return out

    async def get_pvpc_for_range(self, start_dt, end_dt):
        return await self._get_indicator_for_range(ESIOS_INDICATOR_ID, start_dt, end_dt)

    async def get_buy_prices_for_range(self, start_dt, end_dt):
        return await self._get_indicator_for_range(1001, start_dt, end_dt)

    async def get_sell_prices_for_range(self, start_dt, end_dt):
        return await self._get_indicator_for_range(1739, start_dt, end_dt)
