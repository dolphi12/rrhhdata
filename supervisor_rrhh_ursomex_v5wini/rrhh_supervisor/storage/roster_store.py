from __future__ import annotations

import csv
import os
from typing import Any, Dict, List, Optional

from rrhh_supervisor.services.roster import load_roster_csv, normalize_employee_id


class RosterStore:
    def __init__(self, path: str, id_min_width: int = 0):
        self.path = os.path.abspath(path)
        self.id_min_width = int(id_min_width or 0)
        self._rows: List[Dict[str, Any]] = []
        self.reload()

    def reload(self) -> None:
        if os.path.exists(self.path):
            self._rows = load_roster_csv(self.path, id_min_width=self.id_min_width, default_active=1)
        else:
            self._rows = []

    def list_roster(self, active_only: bool = True) -> List[Dict[str, Any]]:
        if not active_only:
            return list(self._rows)
        out = []
        for r in self._rows:
            act = r.get("active")
            if act is None or bool(act):
                out.append(r)
        return out

    def _find_idx(self, employee_id: str) -> Optional[int]:
        eid = normalize_employee_id(employee_id, self.id_min_width)
        for i, r in enumerate(self._rows):
            if normalize_employee_id(r.get("employee_id"), self.id_min_width) == eid:
                return i
        return None

    def upsert_employee(self, employee_id: str, employee_name: str, active: bool = True) -> None:
        eid = normalize_employee_id(employee_id, self.id_min_width)
        name = (employee_name or "").strip()
        idx = self._find_idx(eid)
        row = {"employee_id": eid, "employee_name": name, "active": 1 if active else 0}
        if idx is None:
            self._rows.append(row)
        else:
            self._rows[idx] = row

    def remove_employee(self, employee_id: str) -> bool:
        idx = self._find_idx(employee_id)
        if idx is None:
            return False
        self._rows.pop(idx)
        return True

    def save(self) -> None:
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["employee_id", "employee_name", "active"])
            w.writeheader()
            for r in self._rows:
                w.writerow(
                    {
                        "employee_id": normalize_employee_id(r.get("employee_id"), self.id_min_width),
                        "employee_name": (r.get("employee_name") or "").strip(),
                        "active": 1 if bool(r.get("active", 1)) else 0,
                    }
                )
        os.replace(tmp, self.path)
