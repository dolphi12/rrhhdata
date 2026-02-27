from __future__ import annotations

import csv
from typing import Any, Dict, List

from rrhh_supervisor.services.roster import normalize_employee_id


def load_permissions_csv(path: str, min_id_width: int = 0) -> List[Dict[str, Any]]:
    """
    CSV esperado (encabezados flexibles):
      employee_id|id, op_date|date|day (YYYY-MM-DD), reason|motivo (opcional)
    """
    out: List[Dict[str, Any]] = []
    p = str(path or "").strip()
    if not p:
        return out

    with open(p, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # flexible headers
            eid = (
                row.get("employee_id")
                or row.get("id")
                or row.get("employee")
                or row.get("empleado_id")
                or row.get("empleado")
                or ""
            )
            od = (
                row.get("op_date")
                or row.get("date")
                or row.get("day")
                or row.get("fecha")
                or ""
            )
            reason = row.get("reason") or row.get("motivo") or row.get("nota") or ""
            eid = normalize_employee_id(eid, min_width=min_id_width)
            od = str(od or "").strip()
            if not eid or not od:
                continue
            out.append({"employee_id": eid, "op_date": od, "reason": str(reason or "").strip()})
    return out
