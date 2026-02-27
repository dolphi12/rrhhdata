from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from rrhh_supervisor.storage.db import DB


def _parse_any_iso(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _percentile(sorted_vals: List[float], p: float) -> Optional[float]:
    if not sorted_vals:
        return None
    if p <= 0:
        return float(sorted_vals[0])
    if p >= 1:
        return float(sorted_vals[-1])
    k = (len(sorted_vals) - 1) * p
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return float(sorted_vals[f])
    d = k - f
    return float(sorted_vals[f] * (1 - d) + sorted_vals[c] * d)


def build_employee_profile(
    db: DB,
    employee_id: str,
    local_tz: str,
    window_days: int,
    min_jornadas: int,
) -> Optional[Dict[str, Any]]:
    tz = ZoneInfo(local_tz)
    now_utc = datetime.now(tz=ZoneInfo("UTC"))
    start_utc = now_utc - timedelta(days=int(window_days))
    start_iso = start_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    end_iso = now_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    jornadas = db.list_jornadas_closed_range(employee_id, start_iso, end_iso)
    # Permitir perfiles con muestras pequeñas: el reporte puede generar KPIs aunque haya pocos días.
    if len(jornadas) == 0:
        return None

    sample_warning = len(jornadas) < int(min_jornadas)

    durations = [float(j.get("duration_minutes") or 0) / 60.0 for j in jornadas if (j.get("duration_minutes") or 0) > 0]
    durations.sort()
    starts = []
    ends = []
    incid = {}
    for j in jornadas:
        st = _parse_any_iso(j.get("start_time") or "")
        en = _parse_any_iso(j.get("end_time") or "")
        if st:
            stl = st.astimezone(tz)
            starts.append(stl.hour * 60 + stl.minute)
        if en:
            enl = en.astimezone(tz)
            ends.append(enl.hour * 60 + enl.minute)
        for x in (j.get("incidencias") or []):
            k = str(x)
            incid[k] = incid.get(k, 0) + 1

    starts.sort()
    ends.sort()

    def fmt_hhmm(m: Optional[float]) -> Optional[str]:
        if m is None:
            return None
        mm = int(round(m))
        hh = (mm // 60) % 24
        mi = mm % 60
        return f"{hh:02d}:{mi:02d}"

    profile = {
        "employee_id": str(employee_id),
        "employee_name": str(jornadas[-1].get("employee_name") or ""),
        "window_days": int(window_days),
        "sample_warning": bool(sample_warning),
        "sample_jornadas": len(jornadas),
        "hours": {
            "median": _percentile(durations, 0.5),
            "p75": _percentile(durations, 0.75),
            "p90": _percentile(durations, 0.90),
            "max": float(durations[-1]) if durations else None,
            "pct_over_8": (sum(1 for h in durations if h >= 8.0) / len(durations)) if durations else None,
            "pct_over_10": (sum(1 for h in durations if h >= 10.0) / len(durations)) if durations else None,
            "pct_over_12": (sum(1 for h in durations if h >= 12.0) / len(durations)) if durations else None,
            "pct_over_16": (sum(1 for h in durations if h >= 16.0) / len(durations)) if durations else None,
        },
        "typical_times": {
            "entry_median": fmt_hhmm(_percentile(starts, 0.5) if starts else None),
            "entry_p25": fmt_hhmm(_percentile(starts, 0.25) if starts else None),
            "entry_p75": fmt_hhmm(_percentile(starts, 0.75) if starts else None),
            "exit_median": fmt_hhmm(_percentile(ends, 0.5) if ends else None),
            "exit_p25": fmt_hhmm(_percentile(ends, 0.25) if ends else None),
            "exit_p75": fmt_hhmm(_percentile(ends, 0.75) if ends else None),
        },
        "incidencias": incid,
        "attendance": {
            "days_present": len({j.get("op_date") for j in jornadas if j.get("op_date")}),
            "days_range": int(window_days),
        },
    }
    return profile
