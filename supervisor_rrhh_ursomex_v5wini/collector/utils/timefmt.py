from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

UTC = ZoneInfo("UTC")
def utc_naive_to_device_time(dt_utc_naive: datetime, device_tz: ZoneInfo) -> str:

    if dt_utc_naive.tzinfo is not None:

        dt_utc_naive = dt_utc_naive.astimezone(UTC).replace(tzinfo=None)

    dt_local = dt_utc_naive.replace(tzinfo=UTC).astimezone(device_tz)
    return dt_local.strftime("%Y-%m-%dT%H:%M:%S")


def device_time_now_iso(device_tz: ZoneInfo) -> str:

    return datetime.now(device_tz).date().isoformat()
