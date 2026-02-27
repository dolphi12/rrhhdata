from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def to_utc_iso(event_time_iso: str) -> str:
    if not event_time_iso:
        return "0000-00-00T00:00:00Z"
    try:
        dt = datetime.fromisoformat(event_time_iso.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        return dt.replace(tzinfo=None).isoformat(timespec="seconds") + "Z"
    except Exception:

        return (event_time_iso[:19] if len(event_time_iso) >= 19 else event_time_iso) + "Z"


def _get(d: Dict[str, Any], *keys: str) -> Optional[Any]:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None


def _event_time_from_payload(payload: Dict[str, Any]) -> str:

    v = _get(payload, "time", "dateTime", "happenTime", "eventTime", "captureTime")
    if v is None:
        return ""
    return str(v)


def _normalize_employee_id(emp_id: Any) -> Optional[str]:
    if emp_id is None:
        return None
    s = str(emp_id).strip()
    if not s:
        return None
    # Keep the identifier as the device provides it; de-dup/merging across paddings is handled downstream
    # (e.g., jornadas indexing). Preserving leading zeros avoids breaking legacy ID formats.
    return s



def _employee_from_payload(payload: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    """Extract (employee_id, employee_name) from the device payload.

    Notes:
    - In URSOMEX you sometimes register people without a numeric ID; you put the NAME in the device's ID field.
      Some Hikvision payloads still include a huge unsigned-looking numeric `employeeNo` (e.g. 1844674407...),
      which is typically a placeholder (-1 cast to uint64) or a non-business internal id.
    - We therefore prefer `employeeNoString` when it contains non-digit characters, and we ignore obviously invalid
      huge numeric IDs when a human name is available.
    """
    emp_no_str = _normalize_employee_id(_get(payload, "employeeNoString"))
    emp_no = _normalize_employee_id(_get(payload, "employeeNo"))
    person_id = _normalize_employee_id(_get(payload, "personId"))
    card_no = _normalize_employee_id(_get(payload, "cardNo"))

    def _is_invalid_numeric_id(s: Optional[str]) -> bool:
        if not s:
            return True
        s = s.strip()
        if not s.isdigit():
            return True
        # Common unsigned placeholder pattern (e.g., -1 cast to uint64 -> 18446744073709551615)
        if len(s) >= 18 and s.startswith("1844674407"):
            return True
        try:
            v = int(s)
            if v <= 0:
                return True
            # Treat extremely large values as placeholders/invalid for business IDs.
            if v >= 2**63:
                return True
        except Exception:
            return True
        return False

    # 1) If employeeNoString contains letters (name), prefer it (user entered name as ID).
    if emp_no_str and (not emp_no_str.isdigit()):
        chosen = emp_no_str
    else:
        chosen = None
        # 2) Prefer a valid numeric business ID (smallish), skipping unsigned-placeholder values.
        for c in (emp_no_str, emp_no, person_id, card_no):
            if not c:
                continue
            if c.isdigit() and not _is_invalid_numeric_id(c):
                chosen = c
                break
        # 3) Otherwise, take the first non-empty candidate that is not a placeholder numeric.
        if chosen is None:
            for c in (emp_no_str, emp_no, person_id, card_no):
                if not c:
                    continue
                if c.isdigit() and _is_invalid_numeric_id(c):
                    continue
                chosen = c
                break

    name = _get(payload, "name", "personName", "employeeName")
    name = str(name).strip() if name is not None else None
    if (not name) and chosen and (not chosen.isdigit()):
        # Some devices/configs put the person's name into employeeNoString.
        name = chosen

    return chosen, name
def _verify_mode(payload: Dict[str, Any]) -> Optional[str]:
    v = _get(payload, "verify_mode", "currentVerifyMode", "verifyMode")
    return str(v).strip().lower() if v is not None else None


def _event_type(payload: Dict[str, Any]) -> str:

    v = _get(payload, "eventType", "attendanceStatus", "minor", "type")
    if v is None:
        return "checkIn"
    return str(v)


def compute_event_uid(payload: Dict[str, Any], device_ip: str, event_time: str) -> str:

    uid = _get(payload, "eventId", "serialNo", "logID", "id")
    if uid is not None:
        return f"{device_ip}:{uid}"

    raw = json_canonical(payload)
    base = f"{device_ip}|{event_time}|{raw}".encode("utf-8")
    return hashlib.sha1(base).hexdigest()


def json_canonical(payload: Dict[str, Any]) -> str:

    import json
    try:
        return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    except Exception:
        return str(payload)


def is_hr_event(payload: Dict[str, Any]) -> bool:

    emp_id, _ = _employee_from_payload(payload)
    return bool(emp_id)


def normalize_event(payload: Dict[str, Any], device_ip: Optional[str] = None) -> Dict[str, Any]:
    device_ip = device_ip or str(_get(payload, "device_ip", "ip") or "")
    event_time = _event_time_from_payload(payload)
    event_time_utc = to_utc_iso(event_time) if event_time else ""


    event_date = event_time[:10] if len(event_time) >= 10 else ""

    employee_id, employee_name = _employee_from_payload(payload)
    vmode = _verify_mode(payload)


    result_bucket = "invalid" if vmode == "invalid" else "valid"

    event_type = _event_type(payload)

    uid = compute_event_uid(payload, device_ip or "", event_time)

    return {
        "event_uid": uid,
        "device_ip": device_ip,
        "event_time": event_time,
        "event_time_utc": event_time_utc,
        "event_date": event_date,
        "event_type": event_type,
        "employee_id": employee_id,
        "employee_name": employee_name,
        "verify_mode": vmode,
        "result_bucket": result_bucket,
        "attendance_status": _get(payload, "attendanceStatus"),
        "label": _get(payload, "label"),
        "picture_url": _get(payload, "pictureURL", "pictureUrl", "picture"),
        "payload": {
            **payload,
            "verify_mode": vmode,
            "result_bucket": result_bucket,
        },
    }
