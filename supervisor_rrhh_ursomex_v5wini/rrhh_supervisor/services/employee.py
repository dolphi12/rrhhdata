from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, time
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from rrhh_supervisor.storage.db import DB


def _cutoff_time(hhmm: str) -> time:
    s = (hhmm or "03:00").strip()
    h, m = s.split(":")
    return time(int(h), int(m))


def operational_bounds_now(local_tz: str, cutoff_hhmm: str) -> Tuple[datetime, datetime, str]:
    tz = ZoneInfo(local_tz)
    now = datetime.now(tz=tz)
    cut = _cutoff_time(cutoff_hhmm)
    base = now.date()
    start = datetime.combine(base, cut, tzinfo=tz)
    if now.time() < cut:
        start = start - timedelta(days=1)
    end = start + timedelta(days=1)
    op_date = (start.date()).isoformat()
    return start, end, op_date


def calendar_bounds_today(local_tz: str) -> Tuple[datetime, datetime, str]:
    tz = ZoneInfo(local_tz)
    now = datetime.now(tz=tz)
    base = now.date()
    start = datetime.combine(base, time(0, 0), tzinfo=tz)
    end = start + timedelta(days=1)
    return start, end, base.isoformat()


def to_utc_iso(dt_local: datetime) -> str:
    return dt_local.astimezone(ZoneInfo("UTC")).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def employee_day_view(db: DB, employee_id: str, local_tz: str, cutoff_hhmm: str) -> Dict[str, Any]:
    op_start, op_end, op_date = operational_bounds_now(local_tz, cutoff_hhmm)
    cal_start, cal_end, cal_date = calendar_bounds_today(local_tz)

    op_events = db.get_employee_events_utc_range(employee_id, to_utc_iso(op_start), to_utc_iso(op_end))
    cal_events = db.get_employee_events_utc_range(employee_id, to_utc_iso(cal_start), to_utc_iso(cal_end))

    wide_start = op_start - timedelta(hours=18)
    wide_end = op_end + timedelta(hours=18)
    wide_events = db.get_employee_events_utc_range(employee_id, to_utc_iso(wide_start), to_utc_iso(wide_end))
    opdate_events = [e for e in wide_events if str(e.get("op_date") or "") == str(op_date)]

    return {
        "employee_id": employee_id,
        "op_date": op_date,
        "calendar_date": cal_date,
        "events_operational": op_events,
        "events_calendar": cal_events,
        "events_by_op_date": opdate_events,
    }


def estimate_next_event(
    status: str,
    last_event_utc: str,
    jornada_start_utc: str,
    profile: Optional[Dict[str, Any]],
    local_tz: str,
) -> Dict[str, Any]:
    tz = ZoneInfo(local_tz)
    out = {"expected_role": None, "expected_local": None, "confidence": "BAJA"}
    if not last_event_utc or last_event_utc == "-":
        return out
    try:
        last_dt = datetime.fromisoformat(last_event_utc.replace("Z", "+00:00")).astimezone(tz)
    except Exception:
        return out

    if status == "LABORANDO":
        out["expected_role"] = "OUT"
        median_h = None
        if profile and profile.get("hours", {}).get("median") is not None:
            median_h = float(profile["hours"]["median"])
        if median_h is None:
            return out
        try:
            st = datetime.fromisoformat(jornada_start_utc.replace("Z", "+00:00")).astimezone(tz)
        except Exception:
            st = last_dt
        expected = st + timedelta(hours=median_h)
        out["expected_local"] = expected.replace(microsecond=0).isoformat()
        out["confidence"] = "MEDIA"
        return out

    if status == "PAUSA":
        out["expected_role"] = "IN"
        expected = last_dt + timedelta(minutes=45)
        out["expected_local"] = expected.replace(microsecond=0).isoformat()
        out["confidence"] = "BAJA"
        return out

    return out
