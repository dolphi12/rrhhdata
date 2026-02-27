from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Optional, Tuple

from reportlab.lib import colors
from reportlab.lib.units import inch


def asset_logo_path() -> str:
    here = os.path.dirname(__file__)
    p = os.path.join(here, "..", "assets", "ursomex_logo.png")
    return os.path.abspath(p)


def _fmt(v: Any) -> str:
    return "" if v is None else str(v)


def draw_header(c, page_w: float, page_h: float, title: str, subtitle: str, page_no: int, band_color: Tuple[float, float, float] = (0.00, 0.55, 0.55)):
    band_h = 0.55 * inch
    c.saveState()
    c.setFillColorRGB(*band_color)
    c.rect(0, page_h - band_h, page_w, band_h, stroke=0, fill=1)

    logo = asset_logo_path()
    if os.path.exists(logo):
        try:
            c.drawImage(logo, 0.35 * inch, page_h - band_h + 0.10 * inch, width=0.35 * inch, height=0.35 * inch, mask="auto", preserveAspectRatio=True)
        except Exception:
            pass

    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(0.80 * inch, page_h - 0.32 * inch, _fmt(title))

    c.setFont("Helvetica", 9)
    c.drawRightString(page_w - 0.40 * inch, page_h - 0.32 * inch, _fmt(subtitle))

    c.restoreState()


def draw_footer(c, page_w: float, page_h: float, page_no: int, note: Optional[str] = None):
    """Pie de página.
    - 'note' puede ser una línea corta o varias líneas separadas por '\n'.
    - Si la nota es larga, se envuelve en 2 líneas para evitar que se corte.
    """
    c.saveState()
    c.setFont("Helvetica", 8)

    left_x = 0.45 * inch
    right_x = page_w - 0.45 * inch
    y = 0.35 * inch

    if note:
        text = _fmt(note).strip()
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        # Envolver solo si es una sola línea muy larga
        if len(lines) == 1:
            max_w = (right_x - left_x) * 0.78  # deja espacio visual
            s = lines[0]
            if c.stringWidth(s, "Helvetica", 8) > max_w:
                # Wrap simple por palabras (máx 2 líneas)
                words = s.split()
                a, b = [], []
                cur = []
                for w in words:
                    test = (" ".join(cur + [w])).strip()
                    if c.stringWidth(test, "Helvetica", 8) <= max_w or not cur:
                        cur.append(w)
                    else:
                        a = cur
                        cur = [w]
                if not a:
                    a = cur
                    cur = []
                b = cur
                lines = [" ".join(a), " ".join(b)] if b else [" ".join(a)]
        # Dibujar (máx 2 líneas)
        if len(lines) > 2:
            lines = lines[:2]
        for i, ln in enumerate(lines):
            c.drawString(left_x, y + (0.12 * inch if i == 1 else 0), ln)

    c.drawRightString(right_x, y, f"Página {int(page_no)}")
    c.restoreState()


# ---- Helpers de estilo (look 'dashboard') ----

PRIMARY = (0.00, 0.55, 0.55)         # teal corporativo
PRIMARY_DARK = (0.00, 0.40, 0.40)
CARD_BG = (0.965, 0.982, 0.982)
CARD_BG_ALT = (0.945, 0.970, 0.970)
TEXT_MUTED = (0.25, 0.25, 0.25)
GRID = (0.70, 0.70, 0.70)


def draw_section_title(c, x: float, y: float, text: str, width: float = 6.9 * inch):
    """Título de sección con barra lateral."""
    c.saveState()
    c.setFillColorRGB(*PRIMARY)
    c.roundRect(x, y - 0.18 * inch, 0.10 * inch, 0.22 * inch, 3, stroke=0, fill=1)
    c.setFillColorRGB(0, 0, 0)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(x + 0.16 * inch, y, text)
    # línea tenue
    c.setStrokeColorRGB(0.85, 0.88, 0.88)
    c.setLineWidth(1)
    c.line(x, y - 0.06 * inch, x + width, y - 0.06 * inch)
    c.restoreState()


def draw_kpi_card(c, x: float, y: float, w: float, h: float, label: str, value: str, sub: str = ""):
    """Tarjeta KPI."""
    c.saveState()
    c.setFillColorRGB(*CARD_BG)
    c.roundRect(x, y - h, w, h, 10, stroke=0, fill=1)
    c.setStrokeColorRGB(0.88, 0.92, 0.92)
    c.setLineWidth(1)
    c.roundRect(x, y - h, w, h, 10, stroke=1, fill=0)

    c.setFillColorRGB(*TEXT_MUTED)
    c.setFont("Helvetica", 8.5)
    c.drawString(x + 0.16 * inch, y - 0.26 * inch, label)

    c.setFillColorRGB(0, 0, 0)
    c.setFont("Helvetica-Bold", 16)
    c.drawString(x + 0.16 * inch, y - 0.58 * inch, value)

    if sub:
        c.setFillColorRGB(*TEXT_MUTED)
        c.setFont("Helvetica", 8.5)
        c.drawString(x + 0.16 * inch, y - 0.80 * inch, sub)
    c.restoreState()


def draw_badge(c, x: float, y: float, text: str, bg=(0.90, 0.95, 0.95)):
    """Etiqueta pequeña tipo 'chip'."""
    c.saveState()
    c.setFillColorRGB(*bg)
    tw = c.stringWidth(text, "Helvetica-Bold", 8) + 0.18 * inch
    th = 0.20 * inch
    c.roundRect(x, y - th, tw, th, 6, stroke=0, fill=1)
    c.setFillColorRGB(*PRIMARY_DARK)
    c.setFont("Helvetica-Bold", 8)
    c.drawString(x + 0.09 * inch, y - 0.15 * inch, text)
    c.restoreState()
    return tw
