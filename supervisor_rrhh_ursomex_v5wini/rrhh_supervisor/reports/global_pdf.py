from __future__ import annotations

import os
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List

import matplotlib.pyplot as plt
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas

from rrhh_supervisor.reports.layout import draw_footer, draw_header, draw_section_title, draw_kpi_card, draw_badge
from rrhh_supervisor.reports.motivation import quote_of_day
from rrhh_supervisor.reports.i18n_es import parse_iso_date, date_es, day_label_es, range_es
from rrhh_supervisor.services.global_report import GlobalReportData


def _plot_present_series(days: List[str], counts: List[int], out_png: str):
    plt.figure()
    plt.plot(days, counts)
    plt.xticks(rotation=60, ha="right")
    plt.ylabel("Empleados presentes")
    plt.tight_layout()
    plt.savefig(out_png, dpi=160)
    plt.close()


def _plot_durations_hist(hours: List[float], out_png: str):
    plt.figure()
    plt.hist(hours, bins=24)
    plt.xlabel("Horas por jornada")
    plt.ylabel("Frecuencia")
    plt.tight_layout()
    plt.savefig(out_png, dpi=160)
    plt.close()


def _plot_overtime_bar(labels: List[str], hours: List[float], out_png: str):
    plt.figure()
    plt.bar(labels, hours)
    plt.xticks(rotation=60, ha="right")
    plt.ylabel("Horas extra acumuladas")
    plt.tight_layout()
    plt.savefig(out_png, dpi=160)
    plt.close()


def render_global_pdf(out_path: str, data: GlobalReportData, local_tz: str = "America/Tijuana"):
    out_dir = os.path.dirname(out_path) or "."
    os.makedirs(out_dir, exist_ok=True)

    tmp1 = os.path.join(out_dir, "_tmp_present.png")
    tmp2 = os.path.join(out_dir, "_tmp_durations.png")
    tmp3 = os.path.join(out_dir, "_tmp_overtime.png")

    days = list(data.present_by_day.keys())
    counts = [int(data.present_by_day.get(d) or 0) for d in days]

    # Etiquetas en español para el eje X
    day_labels = []
    for s in days:
        d = parse_iso_date(str(s))
        if d:
            day_labels.append(f"{day_label_es(d, full=True)}\n{date_es(d, 'compact')}")
        else:
            day_labels.append(str(s))

    if days:
        _plot_present_series(day_labels, counts, tmp1)

    all_hours = [float(x) for x in data.durations_hours if x is not None]
    if all_hours:
        _plot_durations_hist(all_hours, tmp2)

    top_labels = [f"{t[0]} {t[1]}".strip() for t in data.top_overtime[:10]]
    top_hours = [float(t[2] or 0.0) for t in data.top_overtime[:10]]
    if top_labels and top_hours:
        _plot_overtime_bar(top_labels, top_hours, tmp3)

    c = canvas.Canvas(out_path, pagesize=A4)
    w, h = A4
    page_no = 1

    title = "URSOMEX | Supervisor RRHH"
    subtitle = "Reporte Global"
    draw_header(c, w, h, title, subtitle, page_no)
    draw_footer(c, w, h, page_no, quote_of_day(local_tz))

        # ---- Encabezado + KPIs (look dashboard) ----
    draw_section_title(c, 0.70 * inch, h - 1.10 * inch, "Reporte global de asistencia", width=w - 1.40 * inch)

    c.setFont("Helvetica", 10)
    c.setFillColorRGB(0.25, 0.25, 0.25)
    sd = parse_iso_date(str(data.start_date))
    ed = parse_iso_date(str(data.end_date))
    rango_es = range_es(sd, ed) if sd and ed else f"{data.start_date} a {data.end_date}"
    c.drawString(0.75 * inch, h - 1.40 * inch, f"Rango: {rango_es}")

    present_emp = sum(1 for t in (data.employee_rates or []) if (t[3] or 0) > 0)
    # Marca de generación (hora local)
    try:
        now_local = datetime.now(tz=ZoneInfo(local_tz)).replace(microsecond=0)
    except Exception:
        now_local = datetime.now().replace(microsecond=0)
    c.setFont("Helvetica-Oblique", 9)
    c.setFillColorRGB(0.35, 0.35, 0.35)
    c.drawString(0.75 * inch, h - 1.58 * inch, f"Generado: {date_es(now_local.date())} {now_local.hour:02d}:{now_local.minute:02d}")

    # KPIs
    counts_total = [int(data.present_by_day.get(d) or 0) for d in (data.op_days or [])] if data.present_by_day else []
    avg_present = (sum(counts_total) / float(len(counts_total))) if counts_total else 0.0
    dur = sorted([float(x) for x in (data.durations_hours or []) if x is not None])
    med_dur = dur[len(dur)//2] if dur else 0.0

    y_kpi = h - 1.80 * inch
    gap = 0.14 * inch
    avail = (w - 1.40 * inch)
    card_w = (avail - 3 * gap) / 4.0
    card_h = 0.85 * inch
    x0 = 0.70 * inch

    draw_kpi_card(c, x0 + 0 * (card_w + gap), y_kpi, card_w, card_h, "Empleados (roster)", f"{int(data.roster_total)}")
    draw_kpi_card(c, x0 + 1 * (card_w + gap), y_kpi, card_w, card_h, "Con asistencia", f"{int(present_emp)}")
    draw_kpi_card(c, x0 + 2 * (card_w + gap), y_kpi, card_w, card_h, "Prom. presentes/día", f"{avg_present:.1f}")
    draw_kpi_card(c, x0 + 3 * (card_w + gap), y_kpi, card_w, card_h, "Duración mediana", f"{med_dur:.1f} h")

    c.setFillColorRGB(0, 0, 0)
    c.setFont("Helvetica", 10)


    y = h - 2.95 * inch
    if os.path.exists(tmp1):
        c.setFont("Helvetica-Bold", 11)
        c.drawString(0.75 * inch, y, "Empleados presentes por día")
        y -= 0.10 * inch
        c.drawImage(tmp1, 0.75 * inch, y - 2.9 * inch, width=6.5 * inch, height=2.9 * inch, preserveAspectRatio=True, mask="auto")
        y -= 3.05 * inch

    if y < 2.3 * inch:
        c.showPage()
        page_no += 1
        draw_header(c, w, h, title, subtitle, page_no)
        draw_footer(c, w, h, page_no, quote_of_day(local_tz))
        y = h - 1.05 * inch

    if os.path.exists(tmp2):
        c.setFont("Helvetica-Bold", 11)
        c.drawString(0.75 * inch, y, "Distribución de duración de jornadas (horas)")
        y -= 0.10 * inch
        c.drawImage(tmp2, 0.75 * inch, y - 2.9 * inch, width=6.5 * inch, height=2.9 * inch, preserveAspectRatio=True, mask="auto")
        y -= 3.05 * inch

    if y < 2.3 * inch:
        c.showPage()
        page_no += 1
        draw_header(c, w, h, title, subtitle, page_no)
        draw_footer(c, w, h, page_no, quote_of_day(local_tz))
        y = h - 1.05 * inch

    if os.path.exists(tmp3):
        c.setFont("Helvetica-Bold", 11)
        c.drawString(0.75 * inch, y, "Top empleados por horas extra acumuladas")
        y -= 0.10 * inch
        c.drawImage(tmp3, 0.75 * inch, y - 2.9 * inch, width=6.5 * inch, height=2.9 * inch, preserveAspectRatio=True, mask="auto")
        y -= 3.05 * inch

    c.showPage()
    page_no += 1
    draw_header(c, w, h, title, subtitle, page_no)
    draw_footer(c, w, h, page_no, quote_of_day(local_tz))

    c.setFont("Helvetica-Bold", 12)
    c.drawString(0.75 * inch, h - 1.10 * inch, "Resumen (top 20)")
    c.setFont("Helvetica", 9)

    y = h - 1.40 * inch
    c.setFillColorRGB(0.92, 0.96, 0.96)
    c.rect(0.70 * inch, y - 0.13 * inch, w - 1.40 * inch, 0.22 * inch, stroke=0, fill=1)
    c.setFillColorRGB(0, 0, 0)
    c.drawString(0.75 * inch, y, "Empleado ID")
    c.drawString(2.00 * inch, y, "Nombre")
    c.drawRightString(w - 1.00 * inch, y, "Horas extra (>= 10h)")
    y -= 0.20 * inch

    for i, (emp_id, name, hours_ot) in enumerate(data.top_overtime[:20]):
        if y < 0.90 * inch:
            c.showPage()
            page_no += 1
            draw_header(c, w, h, title, subtitle, page_no)
            draw_footer(c, w, h, page_no, quote_of_day(local_tz))
            c.setFont("Helvetica-Bold", 12)
            c.drawString(0.75 * inch, h - 1.10 * inch, "Resumen (top 20)")
            c.setFont("Helvetica", 9)
            y = h - 1.40 * inch
        # Fondo alternado para dar look más profesional
        if (i % 2) == 0:
            c.setFillColorRGB(0.97, 0.985, 0.985)
            c.rect(0.70 * inch, y - 0.13 * inch, w - 1.40 * inch, 0.20 * inch, stroke=0, fill=1)
            c.setFillColorRGB(0, 0, 0)
        c.drawString(0.75 * inch, y, str(emp_id))
        c.drawString(2.00 * inch, y, str(name or ""))
        c.drawRightString(w - 1.00 * inch, y, f"{float(hours_ot or 0.0):.1f}")
        y -= 0.18 * inch

    c.save()

    for p in (tmp1, tmp2, tmp3):
        try:
            if os.path.exists(p):
                os.remove(p)
        except Exception:
            pass