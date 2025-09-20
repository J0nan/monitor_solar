# app/clients/iberdrola.py
import aiohttp
import os
from datetime import datetime, timezone
import asyncio

BASE = os.environ.get("IBERDROLA_BASE", "https://monitorizacion.iberdrola.com")
LOGIN_PATH = "/api/3/login"
LAST_SOLAR_PATH_TEMPLATE = "/api/auth/3/buildings/{building_id}/last_solar"
SOLAR_DATA_PATH_TEMPLATE = "/api/auth/3/buildings/{building_id}/solar_data"

EMAIL = os.environ.get("IBERDROLA_EMAIL")
PASSWORD = os.environ.get("IBERDROLA_PASSWORD")
BUILDING_ID = os.environ.get("BUILDING_ID")

class IberdrolaClient:
    def __init__(self):
        self.base = BASE.rstrip("/")
        self._token = None
        self._session = aiohttp.ClientSession()
        self._lock = asyncio.Lock()

    async def close(self):
        await self._session.close()

    async def login(self):
        url = self.base + LOGIN_PATH
        body = {"email": EMAIL, "password": PASSWORD}
        async with self._session.post(url, json=body) as resp:
            resp.raise_for_status()
            js = await resp.json()
            # Per your info, it returns a token
            token = js.get("token") or js.get("access_token") or js.get("data", {}).get("token")
            if not token:
                raise RuntimeError("Login did not return token: %s" % js)
            self._token = "Bearer " + token
            return token

    async def _ensure_token(self):
        if not self._token:
            await self.login()

    def _auth_headers(self):
        # Authorization header uses the token directly (per your description)
        if not self._token:
            return {}
        return {"Authorization": self._token}

    async def get_realtime(self):
        """
        Calls last_solar endpoint and returns normalized structure:
          {
            "timestamp": datetime (UTC),
            "main_w": float,
            "solar_w": float,
            "battery_w": float,
            "total_consumption_w": float
          }
        We'll take the last element of P/time arrays as the latest sample.
        """
        async with self._lock:
            await self._ensure_token()
            url = (self.base + LAST_SOLAR_PATH_TEMPLATE).format(building_id=BUILDING_ID)
            headers = self._auth_headers()
            async with self._session.get(url, headers=headers) as resp:
                if resp.status == 401:
                    # token expired? re-login once
                    await self.login()
                    headers = self._auth_headers()
                    async with self._session.get(url, headers=headers) as resp2:
                        resp2.raise_for_status()
                        body = await resp2.json()
                else:
                    resp.raise_for_status()
                    body = await resp.json()

        # Parse body; per your example, body has keys 'battery', 'main', 'solar', 'total_consumption'
        def last_of(obj):
            if not obj:
                return None, None
            P = obj.get("P") or []
            times = obj.get("time") or []
            if len(P) == 0:
                return None, None
            p = P[-1]
            t = times[-1] if len(times) >= 1 else None
            return p, t

        main_p, main_t = last_of(body.get("main", {}))
        solar_p, solar_t = last_of(body.get("solar", {}))
        battery_p, battery_t = last_of(body.get("battery", {}))
        total_p, total_t = last_of(body.get("total_consumption", {}))

        # Prefer the most recent timestamp available among fields; otherwise use now
        ts_strs = [s for s in (main_t, solar_t, battery_t, total_t) if s]
        if ts_strs:
            # ISO 8601 -> to datetime
            try:
                ts = datetime.fromisoformat(ts_strs[-1].replace("Z", "+00:00")).astimezone(timezone.utc)
            except Exception:
                ts = datetime.now(timezone.utc)
        else:
            ts = datetime.now(timezone.utc)

        # Return normalized numbers (floats). The API likely returns numbers (watts).
        return {
            "timestamp": ts,
            "main_w": float(main_p or 0.0),
            "solar_w": float(solar_p or 0.0),
            "battery_w": float(battery_p or 0.0),
            "total_consumption_w": float(total_p or 0.0)
        }

    async def get_historical(self, start_iso: str, end_iso: str, time_unit: str = "hours"):
        """
        Calls solar_data with query params start, end, time_unit.
        Returns list of samples: [{'timestamp': datetime, 'main_w':..., 'solar_w':..., 'battery_w':...}, ...]
        The API returns arrays of P and time; we will expand them into aligned samples.
        """
        async with self._lock:
            await self._ensure_token()
            url = (self.base + SOLAR_DATA_PATH_TEMPLATE).format(building_id=BUILDING_ID)
            headers = self._auth_headers()
            params = {"start": start_iso, "end": end_iso, "time_unit": time_unit}
            async with self._session.get(url, headers=headers, params=params) as resp:
                if resp.status == 401:
                    await self.login()
                    headers = self._auth_headers()
                    async with self._session.get(url, headers=headers, params=params) as resp2:
                        resp2.raise_for_status()
                        body = await resp2.json()
                else:
                    resp.raise_for_status()
                    body = await resp.json()

        # The response may contain nested data; try to find arrays:
        # Expect a JSON with same keys ('battery','main','solar','total_consumption') each having 'P' and 'time' arrays.
        battery = body.get("battery", {})
        main = body.get("main", {})
        solar = body.get("solar", {})
        total_consumption = body.get("total_consumption", {})

        # Determine the canonical time array: prefer main.time, then solar.time, etc.
        time_arr = main.get("time") or solar.get("time") or battery.get("time") or total_consumption.get("time") or []
        main_p = main.get("P") or []
        solar_p = solar.get("P") or []
        battery_p = battery.get("P") or []
        total_p = total_consumption.get("P") or []

        samples = []
        for idx, tstr in enumerate(time_arr):
            try:
                ts = datetime.fromisoformat(tstr.replace("Z", "+00:00")).astimezone(timezone.utc)
            except Exception:
                continue
            samp = {
                "timestamp": ts,
                "main_w": float(main_p[idx]) if idx < len(main_p) else 0.0,
                "solar_w": float(solar_p[idx]) if idx < len(solar_p) else 0.0,
                "battery_w": float(battery_p[idx]) if idx < len(battery_p) else 0.0,
                "total_consumption_w": float(total_p[idx]) if idx < len(total_p) else 0.0
            }
            samples.append(samp)
        return samples
