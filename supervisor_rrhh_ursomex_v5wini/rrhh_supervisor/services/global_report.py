from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from rrhh_supervisor.storage.db import DB
from rrhh_supervisor.services.worktime import compute_net_minutes_from_events, build_events_by_jornada_id


def _days_in_range(start_date: str, end_date: str) -> List[str]:
    s = datetime.strptime(start_date, "%Y-%m-%d").date()
    e = datetime.strptime(end_date, "%Y-%m-%d").date()
    out = []
    d = s
    while d <= e:
        out.append(d.isoformat())
        d = d + timedelta(days=1)
    return out


def _median(vals: List[float]) -> Optional[float]:
    if not vals:
        return None
    vals = sorted(vals)
    n = len(vals)
    if n % 2 == 1:
        return float(vals[n // 2])
    return float((vals[n // 2 - 1] + vals[n // 2]) / 2.0)


@dataclass(frozen=True)
class GlobalReportData:
    start_date: str
    end_date: str
    roster_total: int
    op_days: List[str]
    present_by_day: Dict[str, int]
    durations_hours: List[float]
    employee_rates: List[Tuple[str, str, float, int]]
    top_overtime: List[Tuple[str, str, float]]


def build_global_report_data(
    collector: DB,
    store: DB,
    start_date: str,
    end_date: str,
    roster_active_only: bool = True,
) -> GlobalReportData:
    roster = store.list_roster(active_only=roster_active_only)
    roster_ids = [r.get("employee_id") for r in roster if r.get("employee_id")]
    roster_names = {r.get("employee_id"): r.get("employee_name") or "" for r in roster if r.get("employee_id")}

    op_days = _days_in_range(start_date, end_date)
    jornadas = collector.list_jornadas_closed_opdate_range(start_date, end_date)

    # --- Cálculo RRHH (neto) consistente ---
    # Prepara rangos por empleado para extraer eventos y calcular neto por jornada
    emp_bounds: Dict[str, Dict[str, str]] = {}
    for jrow in jornadas:
        eid = str(jrow.get('employee_id') or '').strip()
        if not eid:
            continue
        su = str(jrow.get('start_time_utc') or '')
        eu = str(jrow.get('end_time_utc') or '')
        b = emp_bounds.get(eid)
        if b is None:
            b = {'min': su or '', 'max': eu or ''}
            emp_bounds[eid] = b
        if su and (not b['min'] or su < b['min']):
            b['min'] = su
        if eu and (not b['max'] or eu > b['max']):
            b['max'] = eu

    events_by_jid_all: Dict[str, List[Dict[str, Any]]] = {}
    for eid, b in emp_bounds.items():
        if not b.get('min') or not b.get('max'):
            continue
        try:
            events_by_jid_all.update(build_events_by_jornada_id(collector, eid, b['min'], b['max']))
        except Exception:
            continue

    net_minutes_by_jid: Dict[str, int] = {}
    for jrow in jornadas:
        jid = str(jrow.get('jornada_id') or '').strip()
        if not jid:
            continue
        calc = compute_net_minutes_from_events(events_by_jid_all.get(jid, []))
        nm = int(calc.net_minutes or 0)
        if nm <= 0:
            nm = int(jrow.get('duration_minutes') or 0)
        net_minutes_by_jid[jid] = nm


    present_by_day: Dict[str, set] = {d: set() for d in op_days}
    per_emp_days: Dict[str, set] = {}
    per_emp_dur: Dict[str, List[float]] = {}

    durations_hours: List[float] = []
    overtime_hours: Dict[str, float] = {}

    for j in jornadas:
        emp = str(j.get("employee_id") or "").strip()
        od = str(j.get("op_date") or "").strip()
        if not emp or not od or od not in present_by_day:
            continue
        present_by_day[od].add(emp)
        per_emp_days.setdefault(emp, set()).add(od)
        jid = str(j.get("jornada_id") or "").strip()
        h = float(net_minutes_by_jid.get(jid, int(j.get("duration_minutes") or 0))) / 60.0
        if h > 0:
            durations_hours.append(h)
            per_emp_dur.setdefault(emp, []).append(h)
            ot = max(0.0, h - 8.0)
            if ot > 0:
                overtime_hours[emp] = overtime_hours.get(emp, 0.0) + ot

    present_count_by_day = {d: len(present_by_day[d]) for d in op_days}

    employee_rates: List[Tuple[str, str, float, int]] = []
    total_days = len(op_days)
    base_ids = roster_ids if roster_ids else sorted(set(per_emp_days.keys()))

    for emp in base_ids:
        days_present = len(per_emp_days.get(emp, set()))
        rate = (days_present / total_days) if total_days > 0 else 0.0
        name = roster_names.get(emp) or ""
        if not name:
            for x in jornadas:
                if str(x.get("employee_id") or "").strip() == emp:
                    name = str(x.get("employee_name") or "").strip()
                    break
        employee_rates.append((emp, name, rate, days_present))

    employee_rates.sort(key=lambda t: (t[2], t[0]))

    top_overtime: List[Tuple[str, str, float]] = []
    for emp, ot in overtime_hours.items():
        name = roster_names.get(emp) or ""
        if not name:
            for x in jornadas:
                if str(x.get("employee_id") or "").strip() == emp:
                    name = str(x.get("employee_name") or "").strip()
                    break
        top_overtime.append((emp, name, float(ot)))
    top_overtime.sort(key=lambda t: (-t[2], t[0]))

    return GlobalReportData(
        start_date=start_date,
        end_date=end_date,
        roster_total=len(roster_ids) if roster_ids else len(set(per_emp_days.keys())),
        op_days=op_days,
        present_by_day=present_count_by_day,
        durations_hours=durations_hours,
        employee_rates=employee_rates,
        top_overtime=top_overtime[:15],
    )
