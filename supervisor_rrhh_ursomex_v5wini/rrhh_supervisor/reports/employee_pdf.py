from __future__ import annotations

import os
import re
import textwrap
import math
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import matplotlib.pyplot as plt
from matplotlib.textpath import TextPath
from matplotlib.font_manager import FontProperties

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
from reportlab.platypus import Table, TableStyle, Paragraph
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

from rrhh_supervisor.services.worktime import compute_net_minutes_from_events, build_events_by_jornada_id
from rrhh_supervisor.reports.text_utils import humanize_code
from rrhh_supervisor.reports.theme import TEAL, GRID

_humanize_code = humanize_code  # alias interno para compatibilidad

from rrhh_supervisor.reports.layout import draw_footer, draw_header, draw_section_title, draw_kpi_card, draw_badge
from rrhh_supervisor.reports.motivation import quote_of_day
from rrhh_supervisor.reports.i18n_es import parse_iso_date, date_es


# -----------------------------
# Helpers
# -----------------------------
UTC = ZoneInfo("UTC")


def _safe(v: Any) -> str:
    return "" if v is None else str(v)


def _parse_dt_iso(s: str) -> Optional[datetime]:
    ss = (s or "").strip()
    if not ss:
        return None
    # soporta "Z"
    if ss.endswith("Z"):
        ss = ss[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(ss)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def _to_local_dt(utc_iso: str, local_tz: str) -> Optional[datetime]:
    dt_utc = _parse_dt_iso(utc_iso)
    if not dt_utc:
        return None
    try:
        return dt_utc.astimezone(ZoneInfo(local_tz))
    except Exception:
        return dt_utc


def _to_local_hhmm(utc_iso: str, local_tz: str) -> str:
    dt_local = _to_local_dt(utc_iso, local_tz)
    if not dt_local:
        return "--:--"
    return f"{dt_local.hour:02d}:{dt_local.minute:02d}"


def _to_local_date(utc_iso: str, local_tz: str) -> str:
    dt_local = _to_local_dt(utc_iso, local_tz)
    if not dt_local:
        return ""
    return dt_local.date().isoformat()


def format_minutes_hhmm(mins: int) -> str:
    m = int(mins or 0)
    if m < 0:
        m = 0
    hh = m // 60
    mm = m % 60
    return f"{hh:02d}:{mm:02d}"


def hours_to_hhmm(v_hours: Any) -> str:
    try:
        v = float(v_hours)
    except Exception:
        return "--:--"
    if v < 0:
        v = 0.0
    mins = int(v * 60.0 + 1e-6)  # trunc (sin redondeos)
    return format_minutes_hhmm(mins)


def _fmt_pct(x: Any) -> str:
    try:
        if x is None:
            return ""
        if isinstance(x, str) and not x.strip():
            return ""
        v = float(x)
        if 0.0 <= v <= 1.0:
            v *= 100.0
        return f"{v:.1f}%"
    except Exception:
        return _safe(x)


def humanize_code(code: str) -> str:
    """Solo presentación en PDF (no afecta lógica)."""
    s = (code or "").strip()
    if not s:
        return ""
    s = s.replace("-", "_")
    s = re.sub(r"\s+", "_", s)

    MAP = {
        "FALTA_SALIDA": "Falta salida",
        "FALTA_ENTRADA": "Falta entrada",
        "EVENTO_SUELTO": "Evento suelto",
        "PAUSA_LARGA": "Pausa larga",
        "JORNADA_LARGA_O_ABIERTA": "Jornada larga o abierta",
        "JORNADA_LARGA": "Jornada larga",
        "PATRON_EXTRA_PERMISOS": "Extra / permisos",
        "PATRON_2": "Patrón 2",
        "PATRON_4_COMIDA": "Patrón 4: comida",
        "PATRON_5_CENA": "Patrón 5: cena",
        "PATRON_CIERRE_D1": "Cierre en día siguiente",
    }
    if s in MAP:
        return MAP[s]

    m = re.match(r"^PATRON_(\d+)(?:_(.*))?$", s)
    if m:
        num = m.group(1)
        rest = (m.group(2) or "").replace("_", " ").strip().lower()
        return f"Patrón {num}" + (f": {rest}" if rest else "")

    pretty = s.replace("_", " ").strip().lower()
    pretty = re.sub(r"\s+", " ", pretty)
    if pretty:
        pretty = pretty[0].upper() + pretty[1:]
    return pretty


# -----------------------------
# Charts
# -----------------------------
def _save_fig(path: str, dpi: int = 170) -> None:
    fig = plt.gcf()
    try:
        fig.tight_layout()
    except Exception:
        pass
    fig.savefig(path, dpi=dpi, bbox_inches="tight", pad_inches=0.06)
    plt.close(fig)


def _plot_hours_series(op_labels: List[str], hours: List[float], out_png: str):
    plt.figure(figsize=(7.2, 3.0))
    plt.plot(op_labels, hours, marker="o")
    plt.title("Horas netas por día operativo")
    plt.ylabel("Horas")
    plt.xticks(rotation=45, ha="right", fontsize=8)
    plt.grid(True, axis="y", alpha=0.25)
    _save_fig(out_png, dpi=170)


def _plot_start_hist(start_minutes: List[int], out_png: str):
    """Distribución de hora de entrada (primera checada) – estilo legible.
    - Bins de 30 min (0-24)
    - Marca mediana y p25/p75
    """
    if not start_minutes:
        return
    vals = [m / 60.0 for m in start_minutes]

    # Estadísticos
    s = sorted(vals)
    n = len(s)
    def _pct(p: float) -> float:
        if n == 0:
            return 0.0
        k = (n - 1) * p
        f = int(math.floor(k))
        c = int(math.ceil(k))
        if f == c:
            return float(s[f])
        return float(s[f] + (s[c] - s[f]) * (k - f))

    p25 = _pct(0.25)
    med = _pct(0.50)
    p75 = _pct(0.75)

    def _fmt(h: float) -> str:
        hh = int(h) % 24
        mm = int(round((h - int(h)) * 60.0))
        if mm >= 60:
            hh = (hh + 1) % 24
            mm -= 60
        return f"{hh:02d}:{mm:02d}"

    # Bins 30 min
    bins = [i / 2.0 for i in range(0, 49)]  # 0.0..24.0 step 0.5
    plt.figure(figsize=(7.2, 2.6))
    ax = plt.gca()
    ax.hist(vals, bins=bins, edgecolor="white", linewidth=0.8)

    ax.set_title("Distribución de hora de entrada (primera checada)")
    ax.set_xlabel("Hora (local)")
    ax.set_ylabel("Días")

    # Ticks cada 1 hora
    ax.set_xlim(0, 24)
    xt = list(range(0, 25, 1))
    ax.set_xticks(xt)
    ax.set_xticklabels([f"{h:02d}:00" for h in xt], rotation=0, fontsize=8)

    # Líneas de referencia
    ax.axvline(med, linestyle="--", linewidth=1.2)
    ax.axvline(p25, linestyle=":", linewidth=1.0)
    ax.axvline(p75, linestyle=":", linewidth=1.0)

    # Etiqueta esquina (más entendible)
    ax.text(
        0.99,
        0.95,
        f"p25 { _fmt(p25) }   mediana { _fmt(med) }   p75 { _fmt(p75) }",
        transform=ax.transAxes,
        ha="right",
        va="top",
        fontsize=8.5,
    )

    ax.grid(True, axis="y", alpha=0.22)
    _save_fig(out_png, dpi=170)


def _plot_line(op_labels: List[str], yvals: List[float], ylabel: str, out_png: str, title: str):
    plt.figure(figsize=(7.2, 2.25))
    plt.plot(op_labels, yvals, marker="o")
    plt.title(title)
    plt.ylabel(ylabel)
    plt.xticks(rotation=45, ha="right", fontsize=8)
    plt.grid(True, axis="y", alpha=0.25)
    _save_fig(out_png, dpi=170)


def _fmt_clock(h: float) -> str:
    mins = int(round(float(h) * 60.0))
    plus = 0
    if mins >= 24 * 60:
        plus = mins // (24 * 60)
        mins -= plus * 24 * 60
    hh = (mins // 60) % 24
    mm = mins % 60
    return f"{hh:02d}:{mm:02d}" + (f"(+{plus})" if plus else "")


def _fmt_dur(h: float) -> str:
    mins = max(0, int(round(float(h) * 60.0)))
    return f"{mins // 60:02d}:{mins % 60:02d}"


def _text_px_width(text: str, fontsize: float) -> float:
    fp = FontProperties(family="DejaVu Sans", size=fontsize)
    try:
        tp = TextPath((0, 0), text, prop=fp, size=fontsize)
        return float(tp.get_extents().width)
    except Exception:
        return float(len(text)) * fontsize * 0.55


def _plot_gantt_timeline(op_labels: List[str], start_h: List[float], end_h: List[float], out_png: str):
    """Gantt azul con texto auto-ajustable dentro de las barras."""
    if not op_labels:
        return

    n = len(op_labels)
    plt.figure(figsize=(7.2, max(2.9, 0.23 * n)))
    ax = plt.gca()
    ys = list(range(n))
    ax.set_xlim(0, 30)

    color = "#1f77b4"
    for i in range(n):
        s = float(start_h[i])
        e = float(end_h[i])
        w = max(0.0, e - s)
        ax.barh(ys[i], w, left=s, color=color, alpha=1.0)

    fig = plt.gcf()
    fig.canvas.draw()
    xmin, xmax = ax.get_xlim()
    px_per_unit = ax.bbox.width / max(1e-6, (xmax - xmin))

    for i in range(n):
        s = float(start_h[i])
        e = float(end_h[i])
        w = max(0.0, e - s)

        st = _fmt_clock(s)
        en = _fmt_clock(e)
        dur = _fmt_dur(w)

        full = f"{st}–{en}  ({dur})"
        mid = f"{st}–{en}"
        short = f"{dur}"

        pad_px = 10.0
        avail_px = max(0.0, w * px_per_unit - pad_px)

        if w >= 1.6 and avail_px >= 18.0:
            base_fs = 7.2
            min_fs = 5.2
            preferred = full if w >= 3.0 else mid
            for cand in (preferred, mid, short):
                fs = base_fs
                while fs >= min_fs and _text_px_width(cand, fs) > avail_px:
                    fs -= 0.2
                if fs >= min_fs and _text_px_width(cand, fs) <= avail_px:
                    ax.text(s + w / 2.0, ys[i], cand, va="center", ha="center", fontsize=fs, color="white")
                    break
            else:
                ax.text(e + 0.15, ys[i], short, va="center", ha="left", fontsize=7.0, color="#111111")
        else:
            ax.text(e + 0.15, ys[i], short, va="center", ha="left", fontsize=7.0, color="#111111")

    ax.set_yticks(ys)
    ax.set_yticklabels(op_labels, fontsize=8)
    ax.invert_yaxis()
    ax.set_xlabel("Hora (0-24) / +24 si cruza medianoche")
    ax.grid(True, axis="x", alpha=0.22)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)

    _save_fig(out_png, dpi=170)


def _plot_compare_employee_global(out_png: str, employee_label: str, emp: Dict[str, float], glob: Dict[str, float]):
    cats = ["Duración\n(mediana)", "Entrada\n(típica)", "Salida\n(típica)", "Extra\n(prom/día)", ">10h\n(días)"]
    emp_vals = [emp.get("dur_med_h", 0.0), emp.get("entry_h", 0.0), emp.get("exit_h", 0.0), emp.get("extra_avg_h", 0.0), emp.get("days_over_10", 0.0)]
    glob_vals = [glob.get("dur_med_h", 0.0), glob.get("entry_h", 0.0), glob.get("exit_h", 0.0), glob.get("extra_avg_h", 0.0), glob.get("days_over_10", 0.0)]

    def _lab(i: int, v: float) -> str:
        if i in (1, 2):
            return _fmt_clock(v)
        if i == 4:
            return f"{v:.1f}d" if abs(v - round(v)) > 1e-6 else f"{int(round(v))}d"
        return f"{v:.1f}h"

    x = list(range(len(cats)))
    width = 0.38
    plt.figure(figsize=(7.2, 2.25))
    plt.bar([i - width / 2 for i in x], glob_vals, width, label="Global", color="#A9B0B8")
    plt.bar([i + width / 2 for i in x], emp_vals, width, label=employee_label, color="#1F77B4")
    plt.xticks(x, cats)
    plt.ylabel("Horas / Hora del día / Días")
    plt.title("Comparativo: empleado vs global")
    plt.grid(True, axis="y", alpha=0.25)
    plt.legend(loc="upper left", fontsize=8)

    ax = plt.gca()
    ymin, ymax = ax.get_ylim()
    ax.set_ylim(ymin, ymax * 1.10 if ymax > 0 else 1)

    for i, v in enumerate(glob_vals):
        plt.text(i - width / 2, v + 0.10, _lab(i, float(v)), ha="center", va="bottom", fontsize=8)
    for i, v in enumerate(emp_vals):
        plt.text(i + width / 2, v + 0.10, _lab(i, float(v)), ha="center", va="bottom", fontsize=8)

    plt.figtext(0.02, 0.01, "Entrada/Salida = hora local (0-24; +1 si cruza). Extra = promedio por día. >10h = días con neto ≥ 10:00.",
                fontsize=7.3, ha="left")
    _save_fig(out_png, dpi=170)


# -----------------------------
# Events formatting
# -----------------------------
def _build_events_by_opdate(db, employee_id: str, jornadas_rows: List[Dict[str, Any]], local_tz: str) -> Dict[str, List[Dict[str, Any]]]:
    # Rango UTC basado en jornadas
    start_utc = None
    end_utc = None
    for r in jornadas_rows:
        st = _parse_dt_iso(str(r.get("start_time_utc") or ""))
        en = _parse_dt_iso(str(r.get("end_time_utc") or ""))
        if st:
            start_utc = st if start_utc is None else min(start_utc, st)
        if en:
            end_utc = en if end_utc is None else max(end_utc, en)
    if start_utc is None or end_utc is None:
        return {}
    # agrega margen
    start_utc = (start_utc - timedelta(hours=6)).replace(microsecond=0)
    end_utc = (end_utc + timedelta(hours=6)).replace(microsecond=0)

    start_iso = start_utc.isoformat().replace("+00:00", "Z")
    end_iso = end_utc.isoformat().replace("+00:00", "Z")

    events = db.get_employee_events_utc_range(employee_id, start_iso, end_iso)
    out: Dict[str, List[Dict[str, Any]]] = {}
    for ev in events:
        op = str(ev.get("op_date") or "").strip()
        if not op:
            continue
        out.setdefault(op, []).append(ev)
    return out


def _intermediate_events_str(evs: List[Dict[str, Any]], local_tz: str) -> str:
    if not evs or len(evs) <= 2:
        return ""
    mids = evs[1:-1]
    parts = []
    for e in mids:
        hhmm = _to_local_hhmm(str(e.get("event_time_utc") or ""), local_tz)
        role = str(e.get("role") or "")
        parts.append(f"{hhmm} {role}")
    return " | ".join(parts)


# -----------------------------
# Guide page (last)
# -----------------------------
def _draw_kpi_guide_page(c, w: float, h: float, page_no: int, title: str, subtitle: str, local_tz: str):
    draw_header(c, w, h, title, subtitle, page_no)
    draw_footer(c, w, h, page_no, quote_of_day(local_tz))

    c.setFont("Helvetica-Bold", 14)
    c.drawString(0.75 * inch, h - 1.20 * inch, "Guía de KPIs y lectura del reporte")
    c.setFont("Helvetica", 10)
    c.setFillColorRGB(0.25, 0.3, 0.35)
    c.drawString(0.75 * inch, h - 1.45 * inch, "Definiciones y metodología usada para calcular neto y horas extra.")
    c.setFillColorRGB(0, 0, 0)

    x = 0.75 * inch
    y = h - 1.75 * inch

    # Cajas
    box_gap = 0.35 * inch
    box_w = (w - 2 * x - box_gap) / 2
    box_h = 4.30 * inch

    def box(x0: float, y0: float, title_txt: str, bullets: List[str]):
        c.setStrokeColor(colors.HexColor("#D7DEE2"))
        c.setFillColor(colors.white)
        c.roundRect(x0, y0 - box_h, box_w, box_h, 8, stroke=1, fill=1)
        c.setFillColor(colors.HexColor("#111111"))
        c.setFont("Helvetica-Bold", 11)
        c.drawString(x0 + 0.25 * inch, y0 - 0.40 * inch, title_txt)
        c.setFont("Helvetica", 9.4)
        c.setFillColor(colors.HexColor("#334155"))
        yy = y0 - 0.80 * inch
        for b in bullets:
            for ln in textwrap.wrap(b, width=52):
                c.drawString(x0 + 0.30 * inch, yy, f"• {ln}")
                yy -= 0.18 * inch
            yy -= 0.06 * inch

    bullets_kpi = [
        "Días con asistencia: días operativos con al menos una jornada registrada.",
        "Horas (mediana): valor central de las horas netas por día.",
        "p75 / p90: percentiles de horas netas.",
        "Máx: mayor duración neta registrada en un día.",
        "Entrada típica / Salida típica: mediana de primera/última checada (hora local).",
        ">8h, >10h, >12h: porcentaje de días que superan esos umbrales netos.",
    ]
    bullets_met = [
        "Regla IN/OUT: 1ª checada = IN; luego alterna OUT→IN→OUT…",
        "Comida: siempre el primer par OUT→IN del día.",
        "Si la comida dura ≤ 60 min: se descuenta fijo 00:30.",
        "Si dura > 60 min: se descuenta el tiempo real.",
        "Los demás OUT→IN (cena, permisos, etc.) se descuentan tiempo real.",
        "Horas extra: neto − 08:00 (si es positivo).",
    ]
    box(x, y, "Definición de KPIs", bullets_kpi)
    box(x + box_w + box_gap, y, "Metodología RRHH", bullets_met)

    # Nota final
    y2 = y - box_h - 0.45 * inch
    c.setFont("Helvetica-Bold", 11)
    c.setFillColor(colors.HexColor("#111111"))
    c.drawString(x, y2, "Notas e incidencias")
    c.setFont("Helvetica", 9.6)
    c.setFillColor(colors.HexColor("#334155"))
    note = ("Las notas resumen patrones detectados (pausas largas, faltas de salida/entrada, cruces de medianoche, etc.). "
            "Se recomienda revisar días con incidencias antes de autorizar horas extra.")
    yy = y2 - 0.28 * inch
    for ln in textwrap.wrap(note, width=110):
        c.drawString(x, yy, ln)
        yy -= 0.20 * inch
    c.setFillColor(colors.black)


# -----------------------------
# Main render
# -----------------------------
def render_employee_pdf(out_path: str, db, local_tz: str, cutoff_hhmm: str, profile: Dict[str, Any], jornadas_rows: List[Dict[str, Any]]):
    if profile is None:
        profile = {}
    # Fallback de nombre si no viene en perfil
    if (not profile.get('employee_name')) and jornadas_rows:
        profile['employee_name'] = str(jornadas_rows[0].get('employee_name') or '')
    if (not profile.get('employee_id')) and jornadas_rows:
        profile['employee_id'] = str(jornadas_rows[0].get('employee_id') or '')

    out_dir = os.path.dirname(os.path.abspath(out_path)) or "."
    os.makedirs(out_dir, exist_ok=True)

    emp_id = _safe(profile.get("employee_id"))
    emp_name = _safe(profile.get("employee_name"))
    title = "URSOMEX | Supervisor RRHH"
    subtitle = "Reporte Empleado"

    # Eventos por día operativo (para registro diario)
    events_by_op = _build_events_by_opdate(db, emp_id, jornadas_rows, local_tz)


    # --- Cálculo RRHH (neto) consistente en TODO el reporte ---
    # Construye eventos por jornada_id en el rango de jornadas
    start_utc = ""
    end_utc = ""
    for r in jornadas_rows:
        su = str(r.get("start_time_utc") or "")
        eu = str(r.get("end_time_utc") or "")
        if su and (not start_utc or su < start_utc):
            start_utc = su
        if eu and (not end_utc or eu > end_utc):
            end_utc = eu
    # margen extra para cruces D+1
    if end_utc:
        try:
            dt_end = _parse_utc(end_utc)
            if dt_end is not None:
                end_utc = (dt_end + timedelta(hours=36)).isoformat().replace("+00:00","Z")
        except Exception:
            pass

    events_by_jid = {}
    if start_utc and end_utc:
        try:
            events_by_jid = build_events_by_jornada_id(db, emp_id, start_utc, end_utc)
        except Exception:
            events_by_jid = {}

    jornada_net_by_jid: Dict[str, int] = {}
    for r in jornadas_rows:
        jid = str(r.get("jornada_id") or "").strip()
        if not jid:
            continue
        evs = events_by_jid.get(jid, [])
        calc = compute_net_minutes_from_events(evs)
        nm = int(calc.net_minutes or 0)
        # fallback si no hay eventos
        if nm <= 0:
            nm = int(r.get("duration_minutes") or 0)
        jornada_net_by_jid[jid] = nm

    # Series por día operativo
    by_day_min: Dict[str, int] = {}
    by_day_first_in: Dict[str, int] = {}
    by_day_last_out: Dict[str, int] = {}

    for r in jornadas_rows:
        op = str(r.get("op_date") or "").strip()
        if not op:
            continue

        jid = str(r.get("jornada_id") or "").strip()
        by_day_min[op] = by_day_min.get(op, 0) + int(jornada_net_by_jid.get(jid, int(r.get("duration_minutes") or 0)))

        stl = _to_local_dt(str(r.get("start_time_utc") or ""), local_tz)
        enl = _to_local_dt(str(r.get("end_time_utc") or ""), local_tz)
        if stl:
            sm = stl.hour * 60 + stl.minute
            by_day_first_in[op] = sm if op not in by_day_first_in else min(by_day_first_in[op], sm)
        if enl:
            em = enl.hour * 60 + enl.minute
            if enl.date().isoformat() > op:
                em += 24 * 60
            by_day_last_out[op] = em if op not in by_day_last_out else max(by_day_last_out[op], em)

    op_dates_sorted = sorted(by_day_min.keys())
    op_labels = []
    gantt_start = []
    gantt_end = []
    dur_hours = []
    first_in_hours = []

    for op in op_dates_sorted:
        d_op = parse_iso_date(op)
        op_labels.append(date_es(d_op, "compact") if d_op else op)
        dur_hours.append(by_day_min.get(op, 0) / 60.0)
        first_in_hours.append(by_day_first_in.get(op, 0) / 60.0 if op in by_day_first_in else 0.0)
        gantt_start.append(by_day_first_in.get(op, 0) / 60.0 if op in by_day_first_in else 0.0)
        gantt_end.append(by_day_last_out.get(op, 0) / 60.0 if op in by_day_last_out else 0.0)

    # Comparativo global (aprox) usando duration_minutes agregadas por employee+day
    # (suficientemente rápido para producción)
    emp_day_net = [m / 60.0 for m in by_day_min.values() if m > 0]
    emp_entry = [m / 60.0 for m in by_day_first_in.values()]
    emp_exit = [m / 60.0 for m in by_day_last_out.values()]
    emp_extra = [max(0.0, h - 8.0) for h in emp_day_net]
    emp_days_over_10 = sum(1 for h in emp_day_net if h >= 10.0)

    def _median(vals: List[float]) -> float:
        if not vals:
            return 0.0
        s = sorted(vals)
        n = len(s)
        return s[n // 2] if n % 2 == 1 else (s[n // 2 - 1] + s[n // 2]) / 2.0

    def _mean(vals: List[float]) -> float:
        return sum(vals) / len(vals) if vals else 0.0

    emp_metrics = {
        "dur_med_h": _median(emp_day_net),
        "entry_h": _median(emp_entry),
        "exit_h": _median(emp_exit),
        "extra_avg_h": _mean(emp_extra),
        "days_over_10": float(emp_days_over_10),
    }

    global_metrics: Dict[str, float] = {}
    try:
        # rango por op_date
        if op_dates_sorted and hasattr(db, "list_jornadas_closed_opdate_range"):
            grows = db.list_jornadas_closed_opdate_range(op_dates_sorted[0], op_dates_sorted[-1])
            g_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
            for jr in grows:
                eid = str(jr.get("employee_id") or "").strip()
                od = str(jr.get("op_date") or "").strip()
                if not eid or not od:
                    continue
                key = (eid, od)
                rec = g_by_key.get(key)
                if rec is None:
                    rec = {"net_min": 0, "first": None, "last": None}
                    g_by_key[key] = rec

                rec["net_min"] += int(jr.get("duration_minutes") or 0)

                stl = _to_local_dt(str(jr.get("start_time_utc") or ""), local_tz)
                enl = _to_local_dt(str(jr.get("end_time_utc") or ""), local_tz)
                if stl:
                    sm = stl.hour * 60 + stl.minute
                    rec["first"] = sm if rec["first"] is None else min(rec["first"], sm)
                if enl:
                    em = enl.hour * 60 + enl.minute
                    if enl.date().isoformat() > od:
                        em += 24 * 60
                    rec["last"] = em if rec["last"] is None else max(rec["last"], em)

            g_net = []
            g_entry = []
            g_exit = []
            g_extra = []
            days10_by_emp: Dict[str, int] = {}
            for (eid, _od), rec in g_by_key.items():
                h = (rec["net_min"] or 0) / 60.0
                if h <= 0:
                    continue
                g_net.append(h)
                if rec["first"] is not None:
                    g_entry.append(rec["first"] / 60.0)
                if rec["last"] is not None:
                    g_exit.append(rec["last"] / 60.0)
                g_extra.append(max(0.0, h - 8.0))
                if h >= 10.0:
                    days10_by_emp[eid] = days10_by_emp.get(eid, 0) + 1
            avg_days10 = (sum(days10_by_emp.values()) / len(days10_by_emp)) if days10_by_emp else 0.0
            global_metrics = {
                "dur_med_h": _median(g_net),
                "entry_h": _median(g_entry),
                "exit_h": _median(g_exit),
                "extra_avg_h": _mean(g_extra),
                "days_over_10": float(avg_days10),
            }
    except Exception:
        global_metrics = {}

    # Temp images
    tmp_hours = os.path.join(out_dir, "_tmp_hours.png")
    tmp_hist = os.path.join(out_dir, "_tmp_hist.png")
    tmp_gantt = os.path.join(out_dir, "_tmp_gantt.png")
    tmp_dur = os.path.join(out_dir, "_tmp_dur.png")
    tmp_in = os.path.join(out_dir, "_tmp_in.png")
    tmp_cmp = os.path.join(out_dir, "_tmp_cmp.png")

    # Charts for page 1
    if op_labels and dur_hours:
        _plot_hours_series(op_labels, dur_hours, tmp_hours)

    start_minutes = [m for m in by_day_first_in.values() if m is not None]
    if start_minutes:
        _plot_start_hist(start_minutes, tmp_hist)

    # Gantt & trends charts
    if op_labels:
        _plot_gantt_timeline(op_labels, gantt_start, gantt_end, tmp_gantt)
        _plot_line(op_labels, dur_hours, "Horas", tmp_dur, "Duración neta por día")
        _plot_line(op_labels, first_in_hours, "Hora", tmp_in, "Hora de primera checada (IN) por día")
        if global_metrics:
            _plot_compare_employee_global(tmp_cmp, f"Empleado {emp_id}", emp_metrics, global_metrics)

    # ----------------- PDF -----------------
    c = canvas.Canvas(out_path, pagesize=A4)
    w, h = A4
    page_no = 1

    # Page 1: Dashboard
    draw_header(c, w, h, title, subtitle, page_no)
    draw_footer(c, w, h, page_no, quote_of_day(local_tz))

    c.setFont("Helvetica-Bold", 16)
    c.drawString(0.75 * inch, h - 1.20 * inch, "Reporte de empleado")

    c.setFont("Helvetica", 11)
    c.drawString(0.75 * inch, h - 1.45 * inch, f"Empleado: {emp_id}  {emp_name}")
    c.drawString(0.75 * inch, h - 1.63 * inch, f"Ventana analítica: {_safe(profile.get('window_days'))} días    Muestras: {_safe(profile.get('sample_jornadas'))}")

    # Indicadores
    y = h - 2.05 * inch
    draw_section_title(c, 0.70 * inch, y, "Indicadores", width=w - 1.40 * inch)
    y -= 0.36 * inch

    hours_stats = profile.get("hours", {}) if isinstance(profile.get("hours", {}), dict) else {}
    tt = profile.get("typical_times", {}) if isinstance(profile.get("typical_times", {}), dict) else {}

    days_att = len([d for d in by_day_min.keys() if by_day_min.get(d, 0) > 0])

    gap = 0.14 * inch
    card_h = 0.92 * inch
    x0 = 0.70 * inch
    avail = (w - 1.40 * inch)
    card_w = (avail - 3 * gap) / 4.0

    # Row KPI (4): días con asistencia + mediana + p75 + p90 (máx se muestra como badge)
    draw_kpi_card(c, x0 + 0 * (card_w + gap), y, card_w, card_h, "Días con asistencia", f"{days_att}")
    draw_kpi_card(c, x0 + 1 * (card_w + gap), y, card_w, card_h, "Horas (mediana)", f"{hours_to_hhmm(hours_stats.get('median'))}")
    draw_kpi_card(c, x0 + 2 * (card_w + gap), y, card_w, card_h, "p75", f"{hours_to_hhmm(hours_stats.get('p75'))}")
    draw_kpi_card(c, x0 + 3 * (card_w + gap), y, card_w, card_h, "p90", f"{hours_to_hhmm(hours_stats.get('p90'))}")

    y -= card_h + 0.22 * inch

    card_h2 = 0.80 * inch
    card_w2 = (avail - gap) / 2.0
    entry = _safe(tt.get("entry_median") or "--:--")
    exit_ = _safe(tt.get("exit_median") or "--:--")
    draw_kpi_card(c, x0, y, card_w2, card_h2, "Entrada típica (mediana)", entry, sub="Hora local")
    draw_kpi_card(c, x0 + card_w2 + gap, y, card_w2, card_h2, "Salida típica (mediana)", exit_, sub="Hora local")

    y -= card_h2 + 0.18 * inch

    c.setFont("Helvetica", 9)
    bx = x0
    by = y + 0.10 * inch
    for lbl, val in [
        (">8h", hours_stats.get("pct_over_8")),
        (">10h", hours_stats.get("pct_over_10")),
        (">12h", hours_stats.get("pct_over_12")),
        (">16h", hours_stats.get("pct_over_16")),
    ]:
        tw = draw_badge(c, bx, by, f"{lbl} {_fmt_pct(val)}")
        bx += tw + 0.10 * inch

    y -= 0.10 * inch
    c.setFont("Helvetica", 10)

    y -= 0.10 * inch
    if os.path.exists(tmp_hours):
        c.setFont("Helvetica-Bold", 11)
        c.drawString(0.75 * inch, y, "Horas por día operativo")
        y -= 0.10 * inch
        c.drawImage(tmp_hours, 0.75 * inch, y - 3.0 * inch, width=6.5 * inch, height=3.0 * inch, preserveAspectRatio=True, mask="auto")
        y -= 3.15 * inch

    if os.path.exists(tmp_hist) and y > 2.2 * inch:
        c.setFont("Helvetica-Bold", 11)
        c.drawString(0.75 * inch, y, "Distribución de hora de entrada")
        y -= 0.10 * inch
        c.drawImage(tmp_hist, 0.75 * inch, y - 2.6 * inch, width=6.5 * inch, height=2.6 * inch, preserveAspectRatio=True, mask="auto")
        y -= 2.75 * inch

    # Page 2: Gantt
    c.showPage()
    page_no += 1
    draw_header(c, w, h, title, subtitle, page_no)
    draw_footer(c, w, h, page_no, quote_of_day(local_tz))

    c.setFont("Helvetica-Bold", 13)
    c.drawString(0.75 * inch, h - 1.10 * inch, "1) Línea de tiempo de jornadas")
    c.setFont("Helvetica", 9)
    c.drawString(0.75 * inch, h - 1.30 * inch, "Vista tipo Gantt. El eje X usa 0-24; si el cierre cae en D+1 se representa como +24.")
    if os.path.exists(tmp_gantt):
        c.drawImage(tmp_gantt, 0.75 * inch, h - 1.35 * inch - 5.7 * inch, width=6.5 * inch, height=5.7 * inch, preserveAspectRatio=True, mask="auto")

    # Complemento: resumen tabular (evita que la hoja se vea "vacía" y mejora lectura)
    y_after = h - 1.35 * inch - 5.7 * inch - 0.25 * inch

    # KPIs rápidos para contexto
    total_days = len(op_dates_sorted)
    cross_days = sum(1 for od in op_dates_sorted if by_day_last_out.get(od, 0) >= 24 * 60)
    c.setFont("Helvetica", 9.2)
    c.drawString(0.75 * inch, y_after, f"Días analizados: {total_days}   |   Cruces D+1: {cross_days}")
    y_after -= 0.20 * inch

    # Tabla resumen (máx 14 filas, el resto se ve en Registro diario)
    from reportlab.platypus import Table, TableStyle
    from reportlab.lib import colors

    def _fmt_hhmm_from_min(mm: int) -> str:
        mm = int(mm or 0)
        plus = ""
        if mm >= 24 * 60:
            mm -= 24 * 60
            plus = "(+1)"
        hh = (mm // 60) % 24
        mi = mm % 60
        return f"{hh:02d}:{mi:02d}{plus}"

    # helper para incidencias legibles por día
    def _day_notes(od: str) -> str:
        incid = []
        for r in jornadas_rows:
            if str(r.get("op_date") or "") == od:
                for it in (r.get("incidencias") or []):
                    if it:
                        incid.append(_humanize_code(str(it)))
        incid = sorted(set([x for x in incid if x]))
        if not incid:
            return "-"
        s = ", ".join(incid[:2]) + ("…" if len(incid) > 2 else "")
        return s

    rows = [["Día", "Entrada", "Salida", "Neto", "Extra", "Cruza", "Notas"]]
    for od in op_dates_sorted[:14]:
        net_min = int(by_day_min.get(od, 0))
        extra_min = max(0, net_min - 8 * 60)
        row = [
            date_es(parse_iso_date(od), "compact") if parse_iso_date(od) else od,
            _fmt_hhmm_from_min(by_day_first_in.get(od, 0)),
            _fmt_hhmm_from_min(by_day_last_out.get(od, 0)),
            format_minutes_hhmm(net_min),
            format_minutes_hhmm(extra_min),
            "Sí" if by_day_last_out.get(od, 0) >= 24 * 60 else "No",
            _day_notes(od),
        ]
        rows.append(row)

    tbl = Table(rows, colWidths=[1.20*inch, 0.80*inch, 0.80*inch, 0.75*inch, 0.75*inch, 0.55*inch, 1.65*inch])
    teal = colors.HexColor("#1AA3A3")
    grid = colors.HexColor("#D7DEE2")
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), teal),
        ("TEXTCOLOR", (0,0), (-1,0), colors.white),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("FONTSIZE", (0,0), (-1,0), 9.0),
        ("FONTNAME", (0,1), (-1,-1), "Helvetica"),
        ("FONTSIZE", (0,1), (-1,-1), 8.8),
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("ALIGN", (1,1), (5,-1), "CENTER"),
        ("GRID", (0,0), (-1,-1), 0.6, grid),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.whitesmoke, colors.lightgrey]),
        ("LEFTPADDING", (0,0), (-1,-1), 4),
        ("RIGHTPADDING", (0,0), (-1,-1), 4),
        ("TOPPADDING", (0,0), (-1,-1), 3),
        ("BOTTOMPADDING", (0,0), (-1,-1), 3),
    ]))
    tw, th = tbl.wrapOn(c, 6.5*inch, y_after)
    tbl.drawOn(c, 0.75*inch, y_after - th)

    # Page 3: Tendencias + comparativo
    c.showPage()
    page_no += 1
    draw_header(c, w, h, title, subtitle, page_no)
    draw_footer(c, w, h, page_no, quote_of_day(local_tz))

    c.setFont("Helvetica-Bold", 13)
    c.drawString(0.75 * inch, h - 1.10 * inch, "2) Tendencias por día")
    c.setFont("Helvetica", 9)
    c.drawString(0.75 * inch, h - 1.30 * inch, "Duración neta y hora de primera checada (IN). Incluye comparativo contra global.")
    y = h - 1.55 * inch

    chart_w = 6.5 * inch
    chart_h = 2.25 * inch
    chart_gap = 0.25 * inch

    if os.path.exists(tmp_dur):
        c.drawImage(tmp_dur, 0.75 * inch, y - chart_h, width=chart_w, height=chart_h, preserveAspectRatio=True, mask="auto")
        y -= (chart_h + chart_gap)
    if os.path.exists(tmp_in):
        c.drawImage(tmp_in, 0.75 * inch, y - chart_h, width=chart_w, height=chart_h, preserveAspectRatio=True, mask="auto")
        y -= (chart_h + chart_gap)
    if os.path.exists(tmp_cmp):
        c.drawImage(tmp_cmp, 0.75 * inch, y - chart_h, width=chart_w, height=chart_h, preserveAspectRatio=True, mask="auto")
        y -= (chart_h + 0.05 * inch)

        # Mini tabla de valores
        if global_metrics:
            c.setFont("Helvetica-Bold", 9.5)
            c.drawString(0.75 * inch, y, "Valores del comparativo (empleado vs global)")
            y -= 0.14 * inch

            def _fmt_days(v: float) -> str:
                try:
                    vv = float(v or 0.0)
                except Exception:
                    vv = 0.0
                return f"{int(round(vv))} d" if abs(vv - round(vv)) < 1e-6 else f"{vv:.1f} d"

            cmp_data = [
                ["Métrica", "Empleado", "Global"],
                ["Duración neta (mediana)", hours_to_hhmm(emp_metrics.get("dur_med_h")), hours_to_hhmm(global_metrics.get("dur_med_h"))],
                ["Entrada típica", _fmt_clock(emp_metrics.get("entry_h", 0.0)), _fmt_clock(global_metrics.get("entry_h", 0.0))],
                ["Salida típica", _fmt_clock(emp_metrics.get("exit_h", 0.0)), _fmt_clock(global_metrics.get("exit_h", 0.0))],
                ["Horas extra (prom/día)", hours_to_hhmm(emp_metrics.get("extra_avg_h")), hours_to_hhmm(global_metrics.get("extra_avg_h"))],
                ["Overtime (días ≥10h)", _fmt_days(emp_metrics.get("days_over_10", 0.0)), _fmt_days(global_metrics.get("days_over_10", 0.0))],
            ]
            tbl = Table(cmp_data, colWidths=[2.30 * inch, 2.10 * inch, 2.10 * inch])
            tbl.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1AA3A3")),
                        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                        ("FONTSIZE", (0, 0), (-1, 0), 9),
                        ("FONTNAME", (0, 1), (0, -1), "Helvetica-Bold"),
                        ("FONTSIZE", (0, 1), (-1, -1), 8.8),
                        ("ALIGN", (1, 1), (-1, -1), "CENTER"),
                        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#D7DEE2")),
                        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                        ("LEFTPADDING", (0, 0), (-1, -1), 6),
                        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                        ("TOPPADDING", (0, 0), (-1, -1), 4),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                    ]
                )
            )
            tw, th = tbl.wrapOn(c, chart_w, y)
            tbl.drawOn(c, 0.75 * inch, y - th)
            y -= (th + chart_gap)

    # Page 4+: Registro diario (tabla)
    c.showPage()
    page_no += 1

    # Tabla estilo teal (Platypus) con split
    styles = getSampleStyleSheet()
    hdr = ParagraphStyle("hdr", parent=styles["Normal"], fontName="Helvetica-Bold", fontSize=9.6, leading=11.5, textColor=colors.white, alignment=1)
    body = ParagraphStyle("body", parent=styles["Normal"], fontName="Helvetica", fontSize=9.2, leading=11, textColor=colors.black, alignment=0)
    body_c = ParagraphStyle("body_c", parent=body, alignment=1)

    def _p(s: str, st: ParagraphStyle) -> Paragraph:
        ss = (s or "").strip()
        ss = ss.replace("_", "_\u200b")  # wrap en códigos sin mostrarlos
        ss = ss.replace("|", " | ")
        ss = " ".join(ss.split())
        ss = ss.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        ss = ss.replace("\n", "<br/>")
        return Paragraph(ss if ss else "-", st)

    # Prepara datos (incluye varias jornadas por op_date)
    data = [[_p(h, hdr) for h in ["Fecha\n(workday)", "Primera\nchecada", "Registros intermedios (IN/OUT)", "Última\nchecada", "Neto\n(HH:MM)", "Cruza\n?", "Notas"]]]

    # Para cada día operativo tomamos primera/última de las jornadas y neto total
    for op in op_dates_sorted:
        # primera/última (min/max)
        st_m = by_day_first_in.get(op, 0)
        en_m = by_day_last_out.get(op, 0)
        st = f"{st_m//60:02d}:{st_m%60:02d}" if op in by_day_first_in else "--:--"
        # salida puede ser +24 -> mostrar hora local y (+1) en texto
        en = _fmt_clock(en_m / 60.0) if op in by_day_last_out else "--:--"

        net_str = format_minutes_hhmm(by_day_min.get(op, 0))
        evs = events_by_op.get(op, [])
        inter = _intermediate_events_str(evs, local_tz)

        # notas: incidencias de jornadas de ese op_date
        incid = []
        for r in jornadas_rows:
            if str(r.get("op_date") or "") == op:
                for it in (r.get("incidencias") or []):
                    if it:
                        incid.append(humanize_code(str(it)))
        incid = sorted(set([x for x in incid if x]))
        note = ", ".join(incid[:3]) + ("…" if len(incid) > 3 else "")
        cruza = "Sí" if (op in by_day_last_out and by_day_last_out.get(op, 0) >= 24 * 60) else "No"

        d_op = parse_iso_date(op)
        op_label = date_es(d_op, "compact") if d_op else op

        data.append([
            _p(op_label, body),
            _p(st, body_c),
            _p(inter, body),
            _p(en, body_c),
            _p(net_str, body_c),
            _p(cruza, body_c),
            _p(note, body),
        ])

    # column widths similar to template teal
    x0 = 0.50 * inch
    avail_w = w - 2 * x0
    date_w = 1.10 * inch
    first_w = 0.78 * inch
    last_w = 0.78 * inch
    net_w = 0.78 * inch
    cross_w = 0.60 * inch
    fixed = date_w + first_w + last_w + net_w + cross_w
    rem = max(1.0, avail_w - fixed)
    inter_w = rem * 0.63
    notes_w = rem - inter_w
    colw = [date_w, first_w, inter_w, last_w, net_w, cross_w, notes_w]

    tbl = Table(data, colWidths=colw, repeatRows=1)
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1AA3A3")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("GRID", (0, 0), (-1, -1), 0.6, colors.HexColor("#D7DEE2")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))

    # draw title for registro diario
    def _draw_registro_header():
        draw_header(c, w, h, title, subtitle, page_no)
        draw_footer(c, w, h, page_no, quote_of_day(local_tz))
        c.setFont("Helvetica-Bold", 13)
        c.drawString(0.75 * inch, h - 1.10 * inch, "3) Registro diario (detalle operativo)")
        c.setFont("Helvetica", 9)
        c.drawString(0.75 * inch, h - 1.30 * inch, "Tabla por día con primer/último evento. Neto y cruces de medianoche.")

    _draw_registro_header()
    y_top = h - 1.52 * inch
    bottom = 0.85 * inch
    max_h = y_top - bottom
    parts = tbl.split(avail_w, max_h) or [tbl]
    for i, part in enumerate(parts):
        tw, th = part.wrap(avail_w, max_h)
        part.drawOn(c, x0, y_top - th)
        if i < len(parts) - 1:
            c.showPage()
            page_no += 1
            _draw_registro_header()

    # Last page: KPI guide
    c.showPage()
    page_no += 1
    _draw_kpi_guide_page(c, w, h, page_no, title, subtitle, local_tz)

    c.save()

    # Cleanup tmp
    for pth in (tmp_hours, tmp_hist, tmp_gantt, tmp_dur, tmp_in, tmp_cmp):
        try:
            if os.path.exists(pth):
                os.remove(pth)
        except Exception:
            pass
