from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo


@dataclass
class WorkCalc:
    net_minutes: int
    lunch_minutes_actual: int = 0
    lunch_adjust_minutes: int = 0
    segments_work: int = 0
    segments_break: int = 0


def _parse_utc(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        # supports "Z"
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None


def compute_net_minutes_from_events(events: List[Dict[str, Any]]) -> WorkCalc:
    """Calcula minutos netos con reglas RRHH:
    - Alterna IN/OUT (toma el primer IN y luego el siguiente OUT, etc.)
    - Comida = primer OUT->IN. Si dura <=60 descuenta fijo 30 (ajuste vs tiempo real).
    - Otros OUT->IN descuentan real.
    - Evita doble descuento porque usa segmentos IN->OUT como base.
    """
    evs = []
    for e in (events or []):
        role = str(e.get("role") or "").upper().strip()
        if role not in ("IN", "OUT"):
            continue
        dt = _parse_utc(str(e.get("event_time_utc") or e.get("event_time") or ""))
        if dt is None:
            continue
        evs.append((dt, role))
    evs.sort(key=lambda x: x[0])
    if not evs:
        return WorkCalc(net_minutes=0)

    # Find first IN
    i = 0
    while i < len(evs) and evs[i][1] != "IN":
        i += 1
    if i >= len(evs):
        return WorkCalc(net_minutes=0)

    work_segments: List[Tuple[datetime, datetime]] = []
    break_segments: List[Tuple[datetime, datetime]] = []

    cur_in: Optional[datetime] = evs[i][0]
    i += 1

    # Find next OUT after each IN, and next IN after each OUT (ignore duplicates)
    while i < len(evs):
        dt, role = evs[i]
        if cur_in is not None:
            # we are looking for OUT
            if role == "OUT" and dt > cur_in:
                work_segments.append((cur_in, dt))
                cur_in = None  # now waiting for IN (break)
            # else ignore
        else:
            # waiting for IN after OUT (break)
            # find last OUT time to start break
            # break start is end of last work segment
            last_out = work_segments[-1][1] if work_segments else None
            if role == "IN" and last_out and dt > last_out:
                break_segments.append((last_out, dt))
                cur_in = dt
            # else ignore
        i += 1

    # net is sum of work segments
    net = 0
    for a, b in work_segments:
        mins = int((b - a).total_seconds() // 60)
        if mins > 0:
            net += mins

    lunch_actual = 0
    lunch_adjust = 0
    if break_segments:
        a, b = break_segments[0]
        lunch_actual = max(0, int((b - a).total_seconds() // 60))
        if lunch_actual <= 60:
            # net currently excluded actual break; should exclude 30 instead
            lunch_adjust = lunch_actual - 30
            net += lunch_adjust

    return WorkCalc(
        net_minutes=max(0, net),
        lunch_minutes_actual=lunch_actual,
        lunch_adjust_minutes=lunch_adjust,
        segments_work=len(work_segments),
        segments_break=len(break_segments),
    )


def build_events_by_jornada_id(db, employee_id: str, start_utc_iso: str, end_utc_iso: str) -> Dict[str, List[Dict[str, Any]]]:
    """Obtiene eventos en rango y los agrupa por jornada_id."""
    out: Dict[str, List[Dict[str, Any]]] = {}
    evs = db.get_employee_events_utc_range(employee_id, start_utc_iso, end_utc_iso)
    for e in evs:
        jid = str(e.get("jornada_id") or "").strip()
        if not jid:
            continue
        out.setdefault(jid, []).append(e)
    return out
