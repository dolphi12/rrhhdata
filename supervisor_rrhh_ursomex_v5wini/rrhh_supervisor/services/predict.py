from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple
from zoneinfo import ZoneInfo

from rrhh_supervisor.storage.db import DB


def _parse_utc(iso_z: str) -> Optional[datetime]:
    if not iso_z:
        return None
    s = iso_z.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _median(vals: list[float]) -> Optional[float]:
    if not vals:
        return None
    vals = sorted(vals)
    n = len(vals)
    if n % 2 == 1:
        return float(vals[n // 2])
    return float((vals[n // 2 - 1] + vals[n // 2]) / 2.0)


def _fmt_local(dt: Optional[datetime], tz: ZoneInfo) -> str:
    if dt is None:
        return ""
    return dt.astimezone(tz).replace(tzinfo=None).isoformat(sep=" ", timespec="minutes")


@dataclass(frozen=True)
class Prediction:
    employee_id: str
    basis: str
    expected_role: str
    expected_time_utc: str
    expected_time_local: str
    confidence: str
    samples: int


def predict_next_event(
    db: DB,
    employee_id: str,
    local_tz: str,
    window_days: int,
    entry_window_minutes: int,
    confidence_min_samples: int,
) -> Optional[Prediction]:
    emp = (employee_id or "").strip()
    if not emp:
        return None
    tz = ZoneInfo(local_tz)
    now_utc = datetime.now(tz=ZoneInfo("UTC"))
    start_utc = now_utc - timedelta(days=int(window_days))
    start_iso = start_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    end_iso = now_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    last = db.get_latest_event_for_employee(emp)
    if not last:
        return None
    role = str(last.get("role") or "").upper()
    last_dt_utc = _parse_utc(str(last.get("event_time_utc") or ""))
    if last_dt_utc is None:
        return None

    open_j = db.get_open_jornada_for_employee(emp)

    jornadas = db.list_jornadas_closed_range(emp, start_iso, end_iso)
    dur_hours = [float(j.get("duration_minutes") or 0) / 60.0 for j in jornadas if (j.get("duration_minutes") or 0) > 0]
    med_dur_h = _median(dur_hours)

    confidence = "BAJA"
    if len(dur_hours) >= max(30, int(confidence_min_samples)):
        confidence = "ALTA"
    elif len(dur_hours) >= int(confidence_min_samples):
        confidence = "MEDIA"

    if open_j and role == "OUT":
        events = db.get_employee_events_utc_range(emp, start_iso, end_iso)
        deltas = []
        prev_role = None
        prev_dt = None
        for e in events:
            r = str(e.get("role") or "").upper()
            dt = _parse_utc(str(e.get("event_time_utc") or ""))
            if dt is None:
                continue
            if prev_role == "OUT" and r == "IN":
                delta = (dt - prev_dt).total_seconds() / 60.0 if prev_dt else None
                if delta is not None and 1 <= delta <= 720:
                    deltas.append(delta)
            prev_role = r
            prev_dt = dt
        med_break = _median(deltas)
        if med_break is None:
            med_break = 60.0
        expected = last_dt_utc + timedelta(minutes=float(med_break))
        conf = confidence
        if len(deltas) >= max(30, int(confidence_min_samples)):
            conf = "ALTA"
        elif len(deltas) >= int(confidence_min_samples):
            conf = "MEDIA"
        else:
            conf = "BAJA"
        return Prediction(
            employee_id=emp,
            basis="PAUSA",
            expected_role="IN",
            expected_time_utc=expected.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            expected_time_local=_fmt_local(expected, tz),
            confidence=conf,
            samples=len(deltas),
        )

    if open_j and role == "IN":
        st = _parse_utc(str(open_j.get("start_time_utc") or ""))
        if st is None:
            st = last_dt_utc
        if med_dur_h is None:
            med_dur_h = 8.0
            confidence = "BAJA"
        expected = st + timedelta(hours=float(med_dur_h))
        if expected < now_utc:
            expected = now_utc + timedelta(minutes=30)
        return Prediction(
            employee_id=emp,
            basis="JORNADA",
            expected_role="OUT",
            expected_time_utc=expected.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            expected_time_local=_fmt_local(expected, tz),
            confidence=confidence,
            samples=len(dur_hours),
        )

    prof = db.get_employee_profile(emp, int(window_days))
    entry_median = ""
    if prof and isinstance(prof.get("typical_times"), dict):
        entry_median = str(prof.get("typical_times", {}).get("entry_median") or "")
    hh, mm = 8, 0
    try:
        if entry_median and ":" in entry_median:
            hh = int(entry_median.split(":")[0])
            mm = int(entry_median.split(":")[1])
    except Exception:
        hh, mm = 8, 0

    now_local = now_utc.astimezone(tz)
    candidate = now_local.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if candidate + timedelta(minutes=int(entry_window_minutes)) < now_local:
        candidate = candidate + timedelta(days=1)
    expected_utc = candidate.astimezone(ZoneInfo("UTC"))

    conf = "BAJA" if not prof else "MEDIA"
    return Prediction(
        employee_id=emp,
        basis="ENTRADA",
        expected_role="IN",
        expected_time_utc=expected_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        expected_time_local=_fmt_local(expected_utc, tz),
        confidence=conf,
        samples=int(prof.get("sample_jornadas") or 0) if prof else 0,
    )
