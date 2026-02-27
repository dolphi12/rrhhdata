from __future__ import annotations

from reportlab.lib import colors
from reportlab.lib.units import inch

# URSOMEX / RRHH palette
TEAL = colors.HexColor("#1AA3A3")
TEAL_DARK = colors.HexColor("#0B6E6E")
GRID = colors.HexColor("#D7DEE2")
INK = colors.HexColor("#111111")
SUBINK = colors.HexColor("#334155")
MUTED_ROW = colors.whitesmoke

# Page margins (landscape letter / A4 safe)
MARGIN_X = 0.65 * inch
MARGIN_TOP = 0.55 * inch
MARGIN_BOTTOM = 0.65 * inch
