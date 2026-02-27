from __future__ import annotations

from datetime import date, datetime
from typing import Optional

WEEKDAY_FULL_ES = ["Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo"]
WEEKDAY_ABBR_ES = ["Lun","Mar","Mié","Jue","Vie","Sáb","Dom"]
MONTH_ABBR_ES = ["ene","feb","mar","abr","may","jun","jul","ago","sep","oct","nov","dic"]
MONTH_FULL_ES = ["enero","febrero","marzo","abril","mayo","junio","julio","agosto","septiembre","octubre","noviembre","diciembre"]

def parse_iso_date(s: str) -> Optional[date]:
    t = (s or "").strip()
    if not t:
        return None
    try:
        return date.fromisoformat(t)
    except Exception:
        return None

def date_es(d: date, fmt: str = "short") -> str:
    """Formato de fecha en español.
    - short: 17-feb-2026
    - long: 17 de febrero de 2026
    - compact: 17-feb
    """
    if fmt == "long":
        return f"{d.day} de {MONTH_FULL_ES[d.month-1]} de {d.year}"
    if fmt == "compact":
        return f"{d.day}-{MONTH_ABBR_ES[d.month-1]}"
    return f"{d.day}-{MONTH_ABBR_ES[d.month-1]}-{d.year}"

def weekday_es(d: date, abbr: bool = False) -> str:
    try:
        return (WEEKDAY_ABBR_ES if abbr else WEEKDAY_FULL_ES)[d.weekday()]
    except Exception:
        return d.strftime("%a" if abbr else "%A")

def day_label_es(d: date, full: bool = True, with_date: bool = False) -> str:
    """Etiqueta tipo 'Miércoles 11' o 'Mié 11' y opcionalmente con fecha."""
    wd = weekday_es(d, abbr=not full)
    if with_date:
        return f"{wd} {date_es(d,'short')}"
    return f"{wd} {d.day}"

def range_es(start: date, end: date) -> str:
    return f"{date_es(start)} a {date_es(end)}"

def now_stamp_es(dt: datetime) -> str:
    # 19-feb-2026 09:15
    d = dt.date()
    return f"{date_es(d)} {dt.hour:02d}:{dt.minute:02d}"
