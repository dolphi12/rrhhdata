from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from rrhh_supervisor.storage.db import DB


@dataclass(frozen=True)
class PresenceRow:
    employee_id: str
    employee_name: str
    status: str
    last_role: str
    last_event_utc: str
    minutes_since_last_event: Optional[int]
    jornada_id: str


def _parse_utc(iso_z: str) -> Optional[datetime]:
    if not iso_z:
        return None
    s = iso_z.strip()
    if s.endswith("Z"):
        s = s[:-1]
    try:
        dt = datetime.fromisoformat(s)
        return dt.replace(tzinfo=ZoneInfo("UTC"))
    except Exception:
        return None


def compute_presence(
    collector: DB,
    local_tz: str,
    stale_after_minutes: int,
) -> List[PresenceRow]:
    tz = ZoneInfo(local_tz)
    now_utc = datetime.now(tz=ZoneInfo("UTC"))
    latest = {r["employee_id"]: r for r in collector.get_latest_event_per_employee() if r.get("employee_id")}
    open_j = {r["employee_id"]: r for r in collector.get_open_jornadas() if r.get("employee_id")}
    all_emp_ids = sorted(set(latest.keys()) | set(open_j.keys()))
    rows: List[PresenceRow] = []
    for emp in all_emp_ids:
        le = latest.get(emp)
        oj = open_j.get(emp)
        name = ""
        role = ""
        last_utc = ""
        if le:
            name = le.get("employee_name") or ""
            role = (le.get("role") or "").upper()
            last_utc = le.get("event_time_utc") or ""
        if oj and not name:
            name = oj.get("employee_name") or ""
        status = "FUERA"
        jid = oj.get("jornada_id") if oj else ""
        mins = None
        dt_utc = _parse_utc(last_utc)
        if dt_utc is not None:
            mins = int((now_utc - dt_utc).total_seconds() // 60)
        if oj:
            if role == "IN":
                status = "LABORANDO"
            elif role == "OUT":
                status = "PAUSA"
            else:
                status = "INCIERTO"
        else:
            if role in ("IN", "OUT"):
                status = "FUERA"
            else:
                status = "SIN_DATOS"
        if mins is not None and mins > stale_after_minutes and status in ("LABORANDO", "PAUSA", "INCIERTO"):
            status = "INCIERTO"
        rows.append(PresenceRow(emp, name, status, role or "-", last_utc or "-", mins, jid or "-"))
    return rows


def summarize_presence(rows: List[PresenceRow]) -> Dict[str, Any]:
    out = {"LABORANDO": 0, "PAUSA": 0, "FUERA": 0, "INCIERTO": 0, "SIN_DATOS": 0, "TOTAL": len(rows)}
    for r in rows:
        if r.status in out:
            out[r.status] += 1
        else:
            out["INCIERTO"] += 1
    return out
