from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

from reportlab.lib.pagesizes import landscape, letter
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfgen import canvas
from reportlab.platypus import Table, TableStyle
from reportlab.lib import colors

from rrhh_supervisor.reports.layout import draw_footer, draw_header
from rrhh_supervisor.services.worktime import compute_net_minutes_from_events, build_events_by_jornada_id
from rrhh_supervisor.reports.theme import TEAL, GRID, MARGIN_X, MARGIN_BOTTOM, SUBINK
from rrhh_supervisor.reports.text_utils import ellipsize_by_width
from rrhh_supervisor.reports.motivation import quote_of_day
from rrhh_supervisor.reports.i18n_es import day_label_es, range_es


WEEK_START_DOW = 2  # Wednesday (Mon=0)

# Alturas fijas (pt) para que el interlineado/aire coincida con la plantilla
MAIN_HEADER_H = 30.0
MAIN_BODY_H = 19.0
SUM_HEADER_H = 22.0
SUM_BODY_H = 18.0


def _parse_utc(iso: str) -> Optional[datetime]:
    s = (iso or "").strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(ZoneInfo("UTC"))
    except Exception:
        return None


def operational_week_bounds(ref: date) -> Tuple[date, date]:
    d = ref
    while d.weekday() != WEEK_START_DOW:
        d = d - timedelta(days=1)
    start = d
    end = start + timedelta(days=6)
    return start, end


def build_week_days(start: date) -> List[date]:
    return [start + timedelta(days=i) for i in range(7)]


def _hours(dur_minutes: Any) -> float:
    try:
        return float(dur_minutes or 0) / 60.0
    except Exception:
        return 0.0


def _split_name(name: str, max_width_pt: float, font_name: str = "Helvetica", font_size: float = 9.0) -> str:
    """Trunca el nombre para que NO se desborde del ancho de columna (usa width real en puntos)."""
    s = (name or "").strip()
    if not s:
        return s
    if pdfmetrics.stringWidth(s, font_name, font_size) <= max_width_pt:
        return s

    ell = "…"
    ell_w = pdfmetrics.stringWidth(ell, font_name, font_size)
    # recorta carácter por carácter hasta que quepa con elipsis
    out = s
    while out and pdfmetrics.stringWidth(out, font_name, font_size) + ell_w > max_width_pt:
        out = out[:-1]
    return (out + ell) if out else ell


def render_weekly_attendance_pdf(
    out_path: str,
    start: date,
    end: date,
    roster: List[Dict[str, Any]],
    jornadas_rows: List[Dict[str, Any]],
    local_tz: str,
    db: Any = None,
    permissions_map: Optional[Dict[str, set]] = None,
    title_note: str = "Lista de asistencia - Semana operativa (Miércoles a Martes)",
):
    """
    Plantilla tipo URSOMEX (paisaje carta), inspirada en la plantilla v2 del usuario.
    - Resumen ejecutivo (marcas presentes, ausencias, cierres D+1)
    - Tabla principal por empleado (Mié a Mar) con P/A y horas (P 8.5*)
    - Resumen por día al final
    """
    permissions_map = permissions_map or {}

    out_dir = os.path.dirname(out_path) or "."
    os.makedirs(out_dir, exist_ok=True)

    week_days = build_week_days(start)
    day_keys = [d.isoformat() for d in week_days]

    # ---- Medidas (para truncado y tabla): se calcula antes de armar filas ----
    w, h = landscape(letter)
    x0 = 0.75 * inch  # margen real (evita recortes)
    avail_w = w - 2 * x0
    base_w = [44.64, 216.0] + [54.0] * 7 + [36.0, 36.0, 42.96]
    base_total = sum(base_w)
    scale = min(1.0, avail_w / base_total) if base_total > 0 else 1.0
    col_widths = [bw * scale for bw in base_w]
    id_w, name_w = col_widths[0], col_widths[1]

    # roster map (FUENTE ÚNICA): solo empleados del roster (no agrega IDs detectados en la BD)
    emp_map: Dict[str, Dict[str, Any]] = {}
    for r in roster or []:
        eid = str(r.get("employee_id") or "").strip()
        if not eid:
            continue
        emp_map[eid] = {"employee_id": eid, "employee_name": str(r.get("employee_name") or "").strip()}

    roster_ids = set(emp_map.keys())
    roster_is_empty = (len(roster_ids) == 0)

    # Consolidación por empleado-día


    # --- Cálculo RRHH (neto) consistente ---
    # Si se pasa `db`, recalcula neto desde eventos IN/OUT (regla RRHH). Si no, usa duration_minutes.
    net_minutes_by_jid: Dict[str, int] = {}

    if db is not None:
        # Rango por empleado para traer eventos solo donde hacen falta
        emp_bounds: Dict[str, Dict[str, str]] = {}
        for jrow in jornadas_rows:
            eid = str(jrow.get("employee_id") or "").strip()
            if (not roster_is_empty) and eid not in roster_ids:
                continue
            if not eid:
                continue
            su = str(jrow.get("start_time_utc") or "")
            eu = str(jrow.get("end_time_utc") or "")
            b = emp_bounds.get(eid)
            if b is None:
                b = {"min": su or "", "max": eu or ""}
                emp_bounds[eid] = b
            if su and (not b["min"] or su < b["min"]):
                b["min"] = su
            if eu and (not b["max"] or eu > b["max"]):
                b["max"] = eu

        events_by_jid_all: Dict[str, List[Dict[str, Any]]] = {}
        for eid, b in emp_bounds.items():
            if not b.get("min") or not b.get("max"):
                continue
            try:
                events_by_jid_all.update(build_events_by_jornada_id(db, eid, b["min"], b["max"]))
            except Exception:
                continue

        for jrow in jornadas_rows:
            jid = str(jrow.get("jornada_id") or "").strip()
            if not jid:
                continue
            calc = compute_net_minutes_from_events(events_by_jid_all.get(jid, []))
            nm = int(calc.net_minutes or 0)
            if nm <= 0:
                nm = int(jrow.get("duration_minutes") or 0)
            net_minutes_by_jid[jid] = nm
    else:
        for jrow in jornadas_rows:
            jid = str(jrow.get("jornada_id") or "").strip()
            if not jid:
                continue
            net_minutes_by_jid[jid] = int(jrow.get("duration_minutes") or 0)


    by_emp_day: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for j in jornadas_rows or []:
        eid = str(j.get("employee_id") or "").strip()
        if (not roster_is_empty) and eid not in roster_ids:
            continue
        od = str(j.get("op_date") or "").strip()
        if not eid or not od:
            continue
        if od < start.isoformat() or od > end.isoformat():
            continue

        key = (eid, od)
        rec = by_emp_day.get(key)
        if rec is None:
            rec = {"hours": 0.0, "cross": False}
            by_emp_day[key] = rec
        jid = str(j.get("jornada_id") or "").strip()
        rec["hours"] = float(rec.get("hours") or 0.0) + (float(net_minutes_by_jid.get(jid, int(j.get("duration_minutes") or 0))) / 60.0)

        # cruce D+1
        end_iso = str(j.get("end_time_utc") or "")
        dt_utc = _parse_utc(end_iso)
        if dt_utc is not None:
            end_local_date = dt_utc.astimezone(ZoneInfo(local_tz)).date().isoformat()
            if end_local_date and end_local_date > od:
                rec["cross"] = True

    # Orden de empleados: por orden del roster (más mantenible y coincide con RRHH)
    employees: List[Dict[str, Any]] = []
    if roster and not roster_is_empty:
        seen: set[str] = set()
        roster_order_ids: List[str] = []
        for r in roster:
            eid = str(r.get("employee_id") or "").strip()
            if not eid or eid in seen:
                continue
            if eid in emp_map:
                roster_order_ids.append(eid)
                seen.add(eid)
        employees = [emp_map[eid] for eid in roster_order_ids]
    else:
        # Fallback: si no hay roster, ordena por ID numérico
        def _eid_num(eid: str) -> int:
            s = (eid or "").strip()
            return int(s) if s.isdigit() else 10**12
        employees = sorted(
            emp_map.values(),
            key=lambda x: (
                _eid_num(str(x.get("employee_id") or "")),
                str(x.get("employee_id") or ""),
                str(x.get("employee_name") or ""),
            ),
        )


    # Totales ejecutivos
    total_emp = len(employees)
    total_slots = total_emp * 7
    present_by_day = [0] * 7
    absent_by_day = [0] * 7
    perm_by_day = [0] * 7
    cross_by_day = [0] * 7

    # Construye filas de la tabla (sin header todavía)
    body_rows: List[List[str]] = []
    for emp in employees:
        eid = str(emp.get("employee_id") or "")
        ename = str(emp.get("employee_name") or "")
        p_count = 0
        a_count = 0
        day_cells: List[str] = []
        for i, dk in enumerate(day_keys):
            v = by_emp_day.get((eid, dk))
            if v is None:
                # Permiso (día no laborable justificado) -> no cuenta como ausencia
                if dk in permissions_map.get(eid, set()):
                    perm_by_day[i] += 1
                    day_cells.append("PR")
                else:
                    a_count += 1
                    absent_by_day[i] += 1
                    day_cells.append("A")
            else:
                p_count += 1
                present_by_day[i] += 1
                hh = float(v.get("hours") or 0.0)
                cross = bool(v.get("cross"))
                if cross:
                    cross_by_day[i] += 1
                s = f"P {hh:.1f}" if hh > 0 else "P"
                if cross:
                    s += "*"
                day_cells.append(s)

        denom = (p_count + a_count)
        pct = (p_count / float(denom) * 100.0) if denom else 0.0
        eid_disp = _split_name(eid, id_w - 10, "Helvetica", 9.0)
        ename_disp = _split_name(ename, name_w - 10, "Helvetica", 9.0)
        body_rows.append([eid_disp, ename_disp] + day_cells + [str(p_count), str(a_count), f"{pct:.0f}%"])

    present_marks = sum(present_by_day)
    absent_marks = sum(absent_by_day)
    perm_marks = sum(perm_by_day)
    cross_total = sum(cross_by_day)
    effective_slots = (present_marks + absent_marks)
    attendance_pct = (present_marks / float(effective_slots) * 100.0) if effective_slots else 0.0

    # ---- PDF layout ----
    c = canvas.Canvas(out_path, pagesize=landscape(letter))
    w, h = landscape(letter)
    title = "URSOMEX | Supervisor RRHH"
    subtitle = "Lista de asistencia - Semana operativa"
    range_str = f"Semana: {range_es(start, end)}"

    teal = TEAL
    grid = GRID
    zebra = colors.HexColor("#F6F8FA")

    x0 = 0.75 * inch  # margen real (evita recortes)
    avail_w = w - 2 * x0

    # Base (pt) medidos de la plantilla original; se escalan para respetar márgenes
    base_w = [44.64, 216.0] + [54.0] * 7 + [36.0, 36.0, 42.96]
    base_total = sum(base_w)  # 753.6pt
    scale = min(1.0, avail_w / base_total) if base_total > 0 else 1.0

    col_widths = [bw * scale for bw in base_w]
    id_w, name_w = col_widths[0], col_widths[1]

    # headers de día: "Mié\n03-04"
    day_headers = [f"{day_label_es(d, full=False)}\n{d.strftime('%m-%d')}" for d in week_days]
    header = ["ID", "Empleado"] + day_headers + ["P", "A", "%"]

    # Tabla principal
    data = [header] + body_rows
    row_heights = [MAIN_HEADER_H] + [MAIN_BODY_H] * len(body_rows)
    tbl = Table(data, colWidths=col_widths, rowHeights=row_heights, repeatRows=1)
    tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), teal),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 9.2),
                ("ALIGN", (0, 0), (-1, 0), "CENTER"),
                ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 1), (-1, -1), 9.0),
                ("GRID", (0, 0), (-1, -1), 0.6, grid),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("ALIGN", (0, 1), (0, -1), "LEFT"),
                ("ALIGN", (1, 1), (1, -1), "LEFT"),
                ("ALIGN", (2, 1), (-1, -1), "CENTER"),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, zebra]),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                    ("TOPPADDING", (0, 0), (-1, 0), 4),
                    ("BOTTOMPADDING", (0, 0), (-1, 0), 4),
                    ("TOPPADDING", (0, 1), (-1, -1), 3),
                    ("BOTTOMPADDING", (0, 1), (-1, -1), 3),
                
                
            ]
        )
    )

    def draw_page_top(page_no: int, with_exec: bool):
        draw_header(c, w, h, title, subtitle, page_no)
        draw_footer(c, w, h, page_no, quote_of_day(local_tz))

        # Línea superior (semana/página), como la plantilla
        c.setFont("Helvetica", 9)
        c.drawString(x0, h - 0.95 * inch, range_str)

        # Título principal
        c.setFont("Helvetica-Bold", 16)
        c.drawString(x0, h - 1.35 * inch, title_note)

        if with_exec:
            c.setFont("Helvetica-Bold", 11)
            c.drawString(x0, h - 1.70 * inch, "Resumen ejecutivo")
            c.setFont("Helvetica", 10)
            c.drawString(
                x0,
                h - 1.92 * inch,
                f"Empleados (roster): {total_emp}   Marcas presentes: {present_marks}/{present_marks + absent_marks} ({attendance_pct:.1f}%)   "
                f"Ausencias: {absent_marks}   Jornadas con cierre en D+1: {cross_total}",
            )
            c.setFont("Helvetica", 9)
            c.drawString(
                x0,
                h - 2.12 * inch,
                "Leyenda: P = Presente (horas de jornada), A = Ausente, PR = Permiso, * = jornada con cierre en D+1.",
            )
            return h - 2.35 * inch
        else:
            return h - 1.65 * inch

    # Dibujar tabla con split en páginas
    page_no = 1
    y_top = draw_page_top(page_no, with_exec=True)
    bottom = 0.95 * inch
    max_h = y_top - bottom

    parts = tbl.split(avail_w, max_h) or [tbl]
    for i, part in enumerate(parts):
        tw, th = part.wrap(avail_w, max_h)
        part.drawOn(c, x0, y_top - th)
        y_top = y_top - th - 0.18 * inch

        if i < len(parts) - 1:
            c.showPage()
            page_no += 1
            y_top = draw_page_top(page_no, with_exec=False)
            max_h = y_top - bottom

    # ---- Resumen por día (en la última página) ----
    def day_summary_table() -> Table:
        rows = [["Día", "Fecha", "Presentes", "Ausentes", "Asistencia", "Jornadas con D+1"]]
        for i, d in enumerate(week_days):
            pres = present_by_day[i]
            absn = absent_by_day[i]
            pct = (pres / float(max(1, pres + absn))) * 100.0
            rows.append([day_label_es(d, full=False), d.isoformat(), str(pres), str(absn), f"{pct:.1f}%", str(cross_by_day[i])])

        sum_row_heights = [SUM_HEADER_H] + [SUM_BODY_H] * (len(rows) - 1)
        t = Table(
            rows,
            colWidths=[0.55 * inch, 1.25 * inch, 0.95 * inch, 0.95 * inch, 1.05 * inch, 1.35 * inch],
            rowHeights=sum_row_heights,
        )
        t.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), teal),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, 0), 9.2),
                    ("ALIGN", (0, 0), (-1, 0), "CENTER"),
                    ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                    ("FONTSIZE", (0, 1), (-1, -1), 9.0),
                    ("GRID", (0, 0), (-1, -1), 0.6, grid),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("ALIGN", (2, 1), (-1, -1), "CENTER"),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, zebra]),
                ]
            )
        )
        return t

    sum_tbl = day_summary_table()

    # Espacio disponible para resumen en la última página
    # Si no cabe, crea nueva página (manteniendo plantilla)
    tw, th = sum_tbl.wrap(avail_w, 9999)
    min_needed = th + 0.65 * inch
    if y_top - min_needed < bottom:
        c.showPage()
        page_no += 1
        y_top = draw_page_top(page_no, with_exec=False)

    c.setFont("Helvetica-Bold", 11)
    c.drawString(x0, y_top, "Resumen por día (semana operativa)")
    y_top -= 0.18 * inch
    sum_tbl.drawOn(c, x0, y_top - th)
    y_top -= (th + 0.20 * inch)

    c.setFont("Helvetica", 9)
    note = (
        "Nota: Este reporte muestra la asistencia por semana operativa (Miércoles a Martes). "
        "Los valores 'P x.x*' indican jornadas con cierre registrado en D+1, pero asociadas al op_date correspondiente."
    )
    # wrap para que no se corte en el margen derecho
    max_w = w - 2 * x0
    words = note.split(" ")
    line = ""
    yy = y_top
    for w0 in words:
        cand = (line + " " + w0).strip()
        if pdfmetrics.stringWidth(cand, "Helvetica", 9) <= max_w:
            line = cand
        else:
            c.drawString(x0, yy, line)
            yy -= 0.16 * inch
            line = w0
    if line:
        c.drawString(x0, yy, line)

    c.save()
