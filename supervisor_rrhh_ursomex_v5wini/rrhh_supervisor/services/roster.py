from __future__ import annotations

import csv
from typing import Any, Dict, List, Optional


def _truthy(v: Any) -> Optional[bool]:
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in ("1", "true", "t", "yes", "y", "si", "sí", "activo", "active"):
        return True
    if s in ("0", "false", "f", "no", "n", "inactivo", "inactive", "baja", "off"):
        return False
    return None


def normalize_employee_id(v: Any, min_width: int = 0) -> str:
    s = "" if v is None else str(v).strip()
    if not s:
        return ""
    mw = int(min_width or 0)
    if mw > 0 and s.isdigit() and len(s) < mw:
        s = s.zfill(mw)
    return s


def _pick(d: Dict[str, Any], keys: List[str]) -> Any:
    for k in keys:
        if k in d and d[k] is not None and str(d[k]).strip() != "":
            return d[k]
    return None


def load_roster_csv(path: str, id_min_width: int = 0, default_active: int = 1) -> List[Dict[str, Any]]:
    p = (path or "").strip()
    if not p:
        return []
    with open(p, "r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        rows: List[Dict[str, Any]] = []
        for row in r:
            if not isinstance(row, dict):
                continue
            emp_raw = _pick(
                row,
                [
                    "employee_id",
                    "id",
                    "emp_id",
                    "no_empleado",
                    "noempleado",
                    "numero",
                    "num",
                    "codigo",
                    "code",
                ],
            )
            name_raw = _pick(
                row,
                [
                    "employee_name",
                    "name",
                    "nombre",
                    "empleado",
                    "employee",
                    "full_name",
                    "fullname",
                ],
            )
            act_raw = _pick(row, ["active", "activo", "status", "estatus", "estado"])
            emp = normalize_employee_id(emp_raw, id_min_width)
            if not emp:
                continue
            name = "" if name_raw is None else str(name_raw).strip()
            act = _truthy(act_raw)
            active = int(default_active) if act is None else (1 if act else 0)
            rows.append({"employee_id": emp, "employee_name": name, "active": active})
        return rows
