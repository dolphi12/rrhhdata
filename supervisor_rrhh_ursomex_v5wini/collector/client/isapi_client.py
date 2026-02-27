from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.auth import HTTPDigestAuth
from zoneinfo import ZoneInfo


class ISAPIError(RuntimeError):
    pass


class ISAPIClient:


    def __init__(
        self,
        ip: str,
        port: int,
        user: str,
        password: str,
        endpoints: Optional[List[str]] = None,
        timeout: int = 15,
        device_timezone: str = "America/Tijuana",
    ):
        self.ip = ip
        self.port = int(port)
        self.user = user
        self.password = password
        self.endpoints = endpoints or ["/ISAPI/AccessControl/AcsEvent?format=json"]
        self.timeout = timeout


        self.device_tz = ZoneInfo(device_timezone)

        self._base = f"http://{self.ip}:{self.port}"
        self._auth = HTTPDigestAuth(self.user, self.password)

    def _post_json(self, path: str, body: Dict[str, Any]) -> Tuple[int, Any]:
        url = self._base + path
        r = requests.post(url, json=body, auth=self._auth, timeout=self.timeout)
        try:
            return r.status_code, r.json()
        except Exception:
            return r.status_code, r.text

    def ping(self) -> bool:

        url = self._base + "/ISAPI/System/status"
        try:
            r = requests.get(url, auth=self._auth, timeout=self.timeout)
            return r.status_code == 200
        except Exception:
            return False

    def pull_acs_events_page(
        self,
        start_time: str,
        end_time: str,
        page_no: int,
        page_size: int,
        search_id: Optional[str] = None,
        retry_attempts: int = 3,
        retry_delay: int = 3,
    ) -> Dict[str, Any]:


        def _to_device_local_isapi(ts: str) -> str:

            if not ts:
                return "1970-01-01T00:00:00"
            raw = ts.strip()

            if " " in raw and "T" not in raw:

                parts = raw.split()
                if len(parts) == 2:
                    d, hm = parts
                    if len(hm) == 5:
                        raw = f"{d}T{hm}:00"
                    else:
                        raw = f"{d}T{hm}"

            if raw.endswith("Z"):
                raw = raw[:-1] + "+00:00"
            try:
                dt = datetime.fromisoformat(raw)
            except Exception:

                return raw[:19].replace(" ", "T")

            if dt.tzinfo is None:

                return dt.strftime("%Y-%m-%dT%H:%M:%S")

            dt_local = dt.astimezone(self.device_tz).replace(tzinfo=None)
            return dt_local.strftime("%Y-%m-%dT%H:%M:%S")


        start_pos = (page_no - 1) * page_size
        return self.pull_acs_events_offset(
            start_time=start_time,
            end_time=end_time,
            start_pos=start_pos,
            max_results=page_size,
            search_id=search_id,
            retry_attempts=retry_attempts,
            retry_delay=retry_delay,
        )

    def pull_acs_events_offset(
        self,
        start_time: str,
        end_time: str,
        start_pos: int,
        max_results: int,
        search_id: Optional[str] = None,
        retry_attempts: int = 3,
        retry_delay: int = 3,
    ) -> Dict[str, Any]:


        def _to_device_local_isapi(ts: str) -> str:
            if not ts:
                return "1970-01-01T00:00:00"
            raw = ts.strip()
            if " " in raw and "T" not in raw:
                parts = raw.split()
                if len(parts) == 2:
                    d, hm = parts
                    raw = f"{d}T{hm}:00" if len(hm) == 5 else f"{d}T{hm}"
            if raw.endswith("Z"):
                raw = raw[:-1] + "+00:00"
            try:
                dt = datetime.fromisoformat(raw)
            except Exception:
                return raw[:19].replace(" ", "T")
            if dt.tzinfo is None:
                return dt.strftime("%Y-%m-%dT%H:%M:%S")
            dt_local = dt.astimezone(self.device_tz).replace(tzinfo=None)
            return dt_local.strftime("%Y-%m-%dT%H:%M:%S")

        sid = search_id or str(int(time.time() * 1000))
        st = _to_device_local_isapi(start_time)
        et = _to_device_local_isapi(end_time)

        cond_base = {
            "searchID": sid,
            "searchResultPosition": int(start_pos),
            "maxResults": int(max_results),
            "startTime": st,
            "endTime": et,
        }
        cond_variants = [dict(cond_base), {**cond_base, "major": 5}, {**cond_base, "major": 5, "minor": 0}]

        last_err: Optional[str] = None
        for _ in range(max(1, retry_attempts)):
            for endpoint in self.endpoints:
                for cond in cond_variants:
                    body = {"AcsEventCond": cond}
                    try:
                        code, data = self._post_json(endpoint, body)
                        if code == 200 and isinstance(data, dict):
                            return data
                        last_err = f"HTTP {code} from {endpoint}: {str(data)[:240]}"
                        if code != 400:
                            break
                    except Exception as e:
                        last_err = f"Request error {endpoint}: {e}"
                        break
            time.sleep(max(0, retry_delay))

        raise ISAPIError(last_err or "Unknown ISAPI error")
