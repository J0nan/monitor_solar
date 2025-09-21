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
        self._timeout = aiohttp.ClientTimeout(total=15)
        self._session = aiohttp.ClientSession(timeout=self._timeout)
        self._lock = asyncio.Lock()

    async def close(self):
        await self._session.close()

    async def login(self):
        if not EMAIL or not PASSWORD:
            raise RuntimeError("Missing IBERDROLA_EMAIL or IBERDROLA_PASSWORD")
        url = self.base + LOGIN_PATH
        body = {"email": EMAIL, "password": PASSWORD}
        async with self._session.post(url, json=body) as resp:
            resp.raise_for_status()
            js = await resp.json(content_type=None)
            token = js.get("token") or js.get("access_token") or js.get("data", {}).get("token")
            if not token:
                raise RuntimeError(f"Login did not return token: {js}")
            self._token = "Bearer " + token
            return token

    def _auth_headers(self):
        return {"Authorization": self._token} if self._token else {}

    async def _request_with_reauth(self, method: str, url: str, *, params=None):
        await self._ensure_token()
        headers = self._auth_headers()
        async with self._session.request(method, url, headers=headers, params=params) as resp:
            if resp.status == 401:
                # token expired -> re-login once
                await self.login()
                headers = self._auth_headers()
                async with self._session.request(method, url, headers=headers, params=params) as resp2:
                    resp2.raise_for_status()
                    return await resp2.json(content_type=None)
            resp.raise_for_status()
            return await resp.json(content_type=None)

    async def _ensure_token(self):
        if not self._token:
            await self.login()
        if not BUILDING_ID:
            raise RuntimeError("Missing BUILDING_ID")

    async def get_realtime(self):
        async with self._lock:
            url = (self.base + LAST_SOLAR_PATH_TEMPLATE).format(building_id=BUILDING_ID)
            body = await self._request_with_reauth("GET", url)

        def last_of(obj):
            if not obj:
                return None, None
            P = obj.get("P") or []
            times = obj.get("time") or []
            if not P:
                return None, None
            p = P[-1]
            t = times[-1] if times else None
            return p, t

        main_p, main_t = last_of(body.get("main", {}))
        solar_p, solar_t = last_of(body.get("solar", {}))
        battery_p, battery_t = last_of(body.get("battery", {}))
        total_p, total_t = last_of(body.get("total_consumption", {}))

        ts_strs = [s for s in (main_t, solar_t, battery_t, total_t) if s]
        if ts_strs:
            try:
                ts = datetime.fromisoformat(ts_strs[-1].replace("Z", "+00:00")).astimezone(timezone.utc)
            except Exception:
                ts = datetime.now(timezone.utc)
        else:
            ts = datetime.now(timezone.utc)

        return {
            "timestamp": ts,
            "main_w": float(main_p or 0.0),
            "solar_w": float(solar_p or 0.0),
            "battery_w": float(battery_p or 0.0),
            "total_consumption_w": float(total_p or 0.0)
        }

    async def _fetch_solar_data(self, start_iso: str, end_iso: str, time_unit: str):
        """
        Call Iberdrola solar_data using only the documented params: start, end, time_unit.
        Example:
          https://monitorizacion.iberdrola.com/api/auth/3/buildings/{id}/solar_data?start=...&end=...&time_unit=hours
        """
        url = (self.base + SOLAR_DATA_PATH_TEMPLATE).format(building_id=BUILDING_ID)
        params = {"start": start_iso, "end": end_iso, "time_unit": time_unit}

        # Small retry loop for rate limiting
        for attempt in range(3):
            try:
                body = await self._request_with_reauth("GET", url, params=params)
                # Only accept the new shape (data + time)
                if isinstance(body, dict) and isinstance(body.get("data"), dict) and isinstance(body.get("time"), list) and body.get("time"):
                    return body
                return {}
            except aiohttp.ClientResponseError as e:
                if e.status == 429 and attempt < 2:
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue
                raise
            except Exception:
                if attempt < 2:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                return {}

    def _parse_historical_samples(self, body: dict):
        """
        Parse Iberdrola's new historical shape:
        {
          "data": { "total_consumption": [...], "purchased_energy": [...], ... },
          "time": ["2025-01-01T10:00:00Z", ...]
        }
        Returns list of dicts with:
          timestamp (UTC datetime),
          main_w, solar_w, battery_w, total_consumption_w (avg W),
          purchased_wh, generated_wh, exported_wh, total_consumption_wh, from_battery_wh, to_battery_wh.
        """
        samples = []

        data = body.get("data")
        time_arr = body.get("time") or []
        if not (isinstance(data, dict) and isinstance(time_arr, list) and len(time_arr) > 0):
            return samples  # empty

        tc = data.get("total_consumption") or []
        pe = data.get("purchased_energy") or []
        ge = data.get("generated_energy") or []
        ee = data.get("exported_energy") or []
        fb = data.get("from_battery") or []
        tb = data.get("to_battery") or []

        n = len(time_arr)
        for i in range(n):
            try:
                ts = datetime.fromisoformat(str(time_arr[i]).replace("Z", "+00:00")).astimezone(timezone.utc)
            except Exception:
                continue
            # Wh series (hourly)
            purchased_wh = float(pe[i]) if i < len(pe) and pe[i] is not None else 0.0
            generated_wh = float(ge[i]) if i < len(ge) and ge[i] is not None else 0.0
            exported_wh = float(ee[i]) if i < len(ee) and ee[i] is not None else 0.0
            total_wh = float(tc[i]) if i < len(tc) and tc[i] is not None else 0.0
            from_batt_wh = float(fb[i]) if i < len(fb) and fb[i] is not None else 0.0
            to_batt_wh = float(tb[i]) if i < len(tb) and tb[i] is not None else 0.0

            # Average signed main W = import - export
            main_w = purchased_wh - exported_wh
            solar_w = generated_wh
            total_w = total_wh
            battery_w = from_batt_wh - to_batt_wh

            samples.append({
                "timestamp": ts,
                "main_w": main_w,
                "solar_w": solar_w,
                "battery_w": battery_w,
                "total_consumption_w": total_w,
                "purchased_wh": purchased_wh,
                "generated_wh": generated_wh,
                "exported_wh": exported_wh,
                "total_consumption_wh": total_wh,
                "from_battery_wh": from_batt_wh,
                "to_battery_wh": to_batt_wh,
            })
        return samples

    async def get_historical_range(self, start_dt, end_dt, time_unit: str = "hours"):
        """
        Fetch historical samples in the given [start_dt, end_dt) UTC interval using the new shape.
        """
        start_iso = start_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        end_iso = end_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        body = await self._fetch_solar_data(start_iso, end_iso, time_unit)
        return self._parse_historical_samples(body)

    async def get_historical(self, start_iso: str, end_iso: str, time_unit: str = "hours"):
        """
        Backward-compatible wrapper; prefer get_historical_range.
        """
        try:
            start_dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
            end_dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            start_dt = datetime.now(timezone.utc)
            end_dt = datetime.now(timezone.utc)
        return await self.get_historical_range(start_dt, end_dt, time_unit="hours")
