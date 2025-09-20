# app/clients/esios.py
import aiohttp
import os
from datetime import datetime, timezone

ESIOS_BASE = "https://api.esios.ree.es/"

ESIOS_API_KEY = os.environ.get("ESIOS_API_KEY")
ESIOS_INDICATOR_ID = int(os.environ.get("ESIOS_INDICATOR_ID", "1001"))

class EsiosClient:
    def __init__(self):
        self.session = aiohttp.ClientSession(headers={
            "Accept": "application/json; application/vnd.esios-api-v1+json",
            "x-api-key": f'"{ESIOS_API_KEY}"'
        })

    async def close(self):
        await self.session.close()

    async def get_pvpc_for_range(self, start_dt, end_dt):
        """
        Get PVPC prices for [start_dt, end_dt). Returns dict: {hour_dt_utc: price_eur_per_kwh}
        This calls /indicators/{id}?start_date=...&end_date=...
        The ESIOS API often returns values in €/MWh; we convert to €/kWh by dividing by 1000.
        """
        params = {
            "start_date": start_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_date": end_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        url = ESIOS_BASE + f"indicators/{ESIOS_INDICATOR_ID}"
        async with self.session.get(url, params=params) as resp:
            resp.raise_for_status()
            js = await resp.json()

        vals = js.get("indicator", {}).get("values") or js.get("values") or []
        out = {}
        for entry in vals:
            dt = entry.get("datetime") or entry.get("date")
            if not dt:
                continue
            if dt.endswith("Z"):
                dt = dt.replace("Z", "+00:00")
            dt_obj = datetime.fromisoformat(dt).astimezone(timezone.utc)
            hour = dt_obj.replace(minute=0, second=0, microsecond=0)
            val = entry.get("value")
            if val is None:
                continue
            try:
                price = float(val) / 1000.0
            except Exception:
                price = float(val)
            out[hour] = price
        return out
