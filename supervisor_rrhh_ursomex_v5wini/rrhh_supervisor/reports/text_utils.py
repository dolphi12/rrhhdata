from __future__ import annotations

import re
from typing import Optional

from reportlab.pdfbase import pdfmetrics


def ellipsize_by_width(text: str, max_width: float, font_name: str = "Helvetica", font_size: float = 9.0) -> str:
    """Trunca con '…' para que nunca se salga del ancho (puntos)."""
    s = (text or "").strip()
    if not s:
        return ""
    if pdfmetrics.stringWidth(s, font_name, font_size) <= max_width:
        return s
    ell = "…"
    # binary search
    lo, hi = 0, len(s)
    while lo < hi:
        mid = (lo + hi) // 2
        cand = s[:mid].rstrip() + ell
        if pdfmetrics.stringWidth(cand, font_name, font_size) <= max_width:
            lo = mid + 1
        else:
            hi = mid
    mid = max(0, lo - 1)
    return s[:mid].rstrip() + ell


def humanize_code(code: str) -> str:
    """Convierte códigos internos (FALTA_SALIDA, PATRON_4_COMIDA, etc.) a texto legible para PDF."""
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
        "PAUSA_LARGA_PATRON": "Pausa larga (patrón)",
        "JORNADA_LARGA_O_ABIERTA": "Jornada larga o abierta",
        "JORNADA_LARGA": "Jornada larga",
        "CIERRE_SIN_FIRMA_TARDE_POR_JORNADA_LARGA": "Cierre tardío sin firma (jornada larga)",
        "PATRON_EXTRA_PERMISOS": "Extra / permisos",
        "PATRON_EXTRA": "Extra",
        "PATRON_CIERRE_D1": "Cierre en día siguiente",
        "PATRON_4_COMIDA": "Patrón 4: comida",
        "PATRON_5_CENA": "Patrón 5: cena",
        "PATRON_2": "Patrón 2",
        "PATRON_1": "Patrón 1",
        "PATRON_3": "Patrón 3",
        "PATRON_5": "Patrón 5",
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
