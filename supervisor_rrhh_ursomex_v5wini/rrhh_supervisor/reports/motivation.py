from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from zoneinfo import ZoneInfo
from typing import List

# Frases cortas (pensadas para caber en el pie de página)
QUOTES_ES: List[str] = [
    "Hazlo simple, hazlo bien.",
    "Un paso hoy vale más que mil planes.",
    "La constancia supera al talento sin disciplina.",
    "Lo que se mide, se mejora.",
    "Pequeñas mejoras diarias crean grandes resultados.",
    "Orden y claridad: el resto fluye.",
    "Cumplir es la mejor reputación.",
    "Enfócate en el proceso; el resultado llega.",
    "Hoy cuenta: termina lo que empezaste.",
    "Calma, enfoque y acción.",
    "La calidad se nota, incluso en lo pequeño.",
    "Si puedes registrarlo, puedes optimizarlo.",
    "Disciplina: hacer lo correcto aunque no apetezca.",
    "Cada minuto bien usado suma.",
    "La puntualidad es respeto.",
    "Sigue, aunque sea lento; pero sigue.",
    "Alinea el equipo y todo avanza.",
    "El trabajo bien hecho se vuelve hábito.",
    "Crea hábitos, no excusas.",
    "Lo importante: claridad, orden y seguimiento.",
    "Hoy mejora tu 1%.",
    "La energía del equipo se contagia.",
    "Un día a la vez, con intención.",
    "Los detalles construyen el resultado.",
    "Menos ruido, más ejecución.",
    "La mejor auditoría es la consistencia.",
    "Confianza = hábitos repetidos.",
    "Respira y resuelve.",
    "Aprender y ajustar: esa es la ruta.",
    "El progreso es acumulativo.",
    "Lo que hoy cuidas, mañana te cuida a ti.",
]

def quote_for_date(d: date) -> str:
    if not QUOTES_ES:
        return ""
    return QUOTES_ES[d.toordinal() % len(QUOTES_ES)]

def quote_of_day(local_tz: str = "America/Tijuana") -> str:
    try:
        d = datetime.now(ZoneInfo(local_tz)).date()
    except Exception:
        d = datetime.now().date()
    return quote_for_date(d)
