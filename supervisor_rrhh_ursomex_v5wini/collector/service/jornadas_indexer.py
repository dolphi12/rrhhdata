from __future__ import annotations

import uuid
import json
import hashlib
from contextlib import nullcontext
from collections import Counter
from datetime import datetime, timedelta, time, date
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from collector.storage.db import DB


MODEL_VERSION = "adv_v2"

UTC = ZoneInfo("UTC")


def _jornada_uid(employee_id: str, start_time_utc: str) -> str:
    """Stable UID for a jornada across rebuilds.

    Hash (employee_id + start_time_utc at seconds precision) so rebuild produces the same UID.
    """
    e = (employee_id or "").strip()
    st = (start_time_utc or "").strip()
    st = st[:19]
    base = f"{e}|{st}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:20]



def _json_load(s: str) -> Any:
    try:
        return json.loads(s) if s else None
    except Exception:
        return None


def _json_dump(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)


def _season_key(op_date: str, seasonality: Dict[str, Any]) -> str:
    """Return season key for a given op_date.

    Modes:
      - month: YYYY-MM
      - peak_offpeak: 'peak' or 'offpeak' based on configured windows.
    """
    if not op_date:
        return ""
    mode = (seasonality.get("mode") or "month").strip().lower()
    if mode == "peak_offpeak":
        windows = seasonality.get("peak_windows") or []
        for w in windows:
            try:
                s = (w.get("start") or "").strip()
                e = (w.get("end") or "").strip()
                if not s or not e:
                    continue
                if s <= op_date <= e:
                    return "peak"
            except Exception:
                continue
        return "offpeak"
    # default: month
    try:
        d = datetime.strptime(op_date, "%Y-%m-%d").date()
        return f"{d.year:04d}-{d.month:02d}"
    except Exception:
        return op_date[:7]



# --- Seasonality v2 (auto drift: peak/offpeak) ---

def _median(vals: List[float]) -> float:
    v = sorted([float(x) for x in vals if x is not None])
    if not v:
        return 0.0
    n = len(v)
    mid = n // 2
    if n % 2 == 1:
        return v[mid]
    return (v[mid - 1] + v[mid]) / 2.0


def _mad(vals: List[float], med: float) -> float:
    dev = [abs(float(x) - med) for x in vals if x is not None]
    return _median(dev)


def _robust_z(x: float, med: float, mad: float) -> float:
    # 1.4826 makes MAD comparable to std for normal dist
    denom = (1.4826 * mad) if mad > 1e-9 else 1.0
    return (float(x) - float(med)) / denom


def _ewma(values: List[float], span_days: int) -> List[float]:
    span_days = int(span_days or 7)
    if span_days <= 1:
        return [float(v) for v in values]
    alpha = 2.0 / (span_days + 1.0)
    out: List[float] = []
    s: Optional[float] = None
    for v in values:
        fv = float(v or 0.0)
        if s is None:
            s = fv
        else:
            s = alpha * fv + (1.0 - alpha) * s
        out.append(float(s))
    return out


def _date_range(start_d: datetime.date, end_d: datetime.date) -> List[datetime.date]:
    out = []
    cur = start_d
    while cur <= end_d:
        out.append(cur)
        cur = cur + timedelta(days=1)
    return out


def _fetch_daily_jornadas_metrics(db: DB, start_op_date: str, end_op_date: str) -> List[Dict[str, Any]]:
    if not start_op_date or not end_op_date:
        return []
    rows: List[Any]
    if db.engine == "sqlite":
        with db._lock:
            rows = db._conn.execute(
                "SELECT op_date, "
                "COUNT(*) as jpd, "
                "SUM(CASE WHEN closed=1 AND end_time IS NOT NULL AND substr(end_time,1,10) > op_date THEN 1 ELSE 0 END) as d1n "
                "FROM jornadas WHERE op_date>=? AND op_date<=? GROUP BY op_date ORDER BY op_date",
                (start_op_date, end_op_date),
            ).fetchall()
    else:
        with db._conn.cursor() as cur:
            cur.execute(
                "SELECT op_date, "
                "COUNT(*) as jpd, "
                "SUM(CASE WHEN closed=1 AND end_time IS NOT NULL AND substring(end_time from 1 for 10) > op_date THEN 1 ELSE 0 END) as d1n "
                "FROM jornadas WHERE op_date>=%s AND op_date<=%s GROUP BY op_date ORDER BY op_date",
                (start_op_date, end_op_date),
            )
            rows = cur.fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        op_date, jpd, d1n = r
        jpd_i = int(jpd or 0)
        d1_i = int(d1n or 0)
        d1_rate = (d1_i / float(jpd_i)) if jpd_i > 0 else 0.0
        out.append({"op_date": str(op_date), "jpd": jpd_i, "d1_rate": float(d1_rate)})
    return out



def _compute_seasonality_v2_from_db(db: DB, cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Compute peak/offpeak regime (Seasonality v2) using stable signals: volume (jpd) + %D1.

    This version supports synchronized refresh aligned to the operational week (Mié→Mar),
    so peak_mode does not change mid-week unless you explicitly choose interval refresh.
    """
    if not isinstance(cfg, dict) or not bool(cfg.get("enabled", False)):
        return {"enabled": False}

    ewma_days = int(cfg.get("ewma_days") or 7)
    baseline_days = int(cfg.get("baseline_days") or 90)
    enter_thr = float(cfg.get("enter_threshold") or 2.0)
    enter_days = int(cfg.get("enter_days") or 3)
    exit_thr = float(cfg.get("exit_threshold") or 1.0)
    exit_days = int(cfg.get("exit_days") or 7)
    max_days_store = int(cfg.get("max_days_store") or 200)

    refresh_mode = str(cfg.get("refresh_mode") or "operational_week").strip().lower()
    refresh_days = int(cfg.get("refresh_days") or 7)
    use_last_complete_week = bool(cfg.get("use_last_complete_week", True))

    def _op_week_start(d: date) -> date:
        wd = d.weekday()  # Mon=0
        delta_to_wed = (wd - 2) % 7  # Wed=2
        return d - timedelta(days=delta_to_wed)

    # Fetch min/max op_date from jornadas (for safe bounds).
    max_op_seen: Optional[str] = None
    min_op_seen: Optional[str] = None
    if db.engine == "sqlite":
        with db._lock:
            row = db._conn.execute("SELECT MAX(op_date), MIN(op_date) FROM jornadas").fetchone()
            if row:
                max_op_seen = str(row[0]) if row[0] else None
                min_op_seen = str(row[1]) if row[1] else None
    else:
        with db._conn.cursor() as cur:
            cur.execute("SELECT MAX(op_date), MIN(op_date) FROM jornadas")
            row = cur.fetchone()
            if row:
                max_op_seen = str(row[0]) if row[0] else None
                min_op_seen = str(row[1]) if row[1] else None

    if not max_op_seen:
        return {"enabled": True, "peak_mode_current": False, "ioi_current": 0.0, "series": []}

    # Load previous state for refresh gating.
    try:
        prev = _json_load((db.get_state("model_seasonality_v2_state") or "").strip())
    except Exception:
        prev = {}
    if not isinstance(prev, dict):
        prev = {}
    prev_last_op = str(prev.get("last_op_date") or "") or str(prev.get("max_op_date") or "")
    prev_week = str(prev.get("last_refresh_week_start") or "")

    # Determine effective end op_date for computation.
    effective_end_op = max_op_seen
    last_refresh_week_start = ""
    try:
        d_now = datetime.strptime(max_op_seen, "%Y-%m-%d").date()
        if refresh_mode in ("operational_week", "weekly", "op_week"):
            wk_start = _op_week_start(d_now)
            last_refresh_week_start = wk_start.isoformat()
            # Refresh at most once per operational week.
            if prev_week and prev_week == last_refresh_week_start and bool(prev.get("enabled", True)):
                # Reuse only if state already has the new snapshot fields.
                if str(prev.get("data_week_end") or "") and isinstance(prev.get("peak_by_op_date"), dict) and isinstance(prev.get("ioi_by_op_date"), dict):
                    return prev

            # Optionally use last COMPLETE week (ends Tuesday) for stability.
            if use_last_complete_week:
                end_d = wk_start - timedelta(days=1)  # Tuesday of previous week
                if min_op_seen:
                    try:
                        d_min = datetime.strptime(min_op_seen, "%Y-%m-%d").date()
                        if end_d < d_min:
                            end_d = d_now
                    except Exception:
                        pass
                effective_end_op = end_d.isoformat()
            else:
                effective_end_op = max_op_seen
        else:
            # Interval refresh (days).
            if prev_last_op:
                d_prev = datetime.strptime(prev_last_op, "%Y-%m-%d").date()
                if (d_now - d_prev).days < max(1, refresh_days) and bool(prev.get("enabled", True)):
                    return prev
    except Exception:
        # best effort: proceed
        effective_end_op = max_op_seen

    # Parse effective end date
    try:
        max_d = datetime.strptime(effective_end_op, "%Y-%m-%d").date()
    except Exception:
        return {"enabled": True, "peak_mode_current": False, "ioi_current": 0.0, "series": []}

    # Window for baseline + smoothing
    start_d = max_d - timedelta(days=int(baseline_days + 30))
    start_op = start_d.isoformat()
    end_op = max_d.isoformat()

    raw = _fetch_daily_jornadas_metrics(db, start_op, end_op)

    # Fill missing dates with zeros so EWMA + hysteresis are stable.
    raw_map = {r["op_date"]: r for r in raw}
    dates = _date_range(start_d, max_d)
    series: List[Dict[str, Any]] = []
    for d in dates:
        od = d.isoformat()
        r = raw_map.get(od) or {}
        series.append({"op_date": od, "jpd": int(r.get("jpd") or 0), "d1_rate": float(r.get("d1_rate") or 0.0)})

    # Baselines from last baseline_days days (raw values).
    tail = series[-baseline_days:] if len(series) > baseline_days else series
    jpd_vals = [float(x["jpd"]) for x in tail]
    d1_vals = [float(x["d1_rate"]) for x in tail]
    jpd_med = _median(jpd_vals)
    d1_med = _median(d1_vals)
    jpd_mad = _mad(jpd_vals, jpd_med)
    d1_mad = _mad(d1_vals, d1_med)

    jpd_ew = _ewma([float(x["jpd"]) for x in series], ewma_days)
    d1_ew = _ewma([float(x["d1_rate"]) for x in series], ewma_days)

    w = cfg.get("ioi_weights") or {}
    w_jpd = float(w.get("jpd") or 0.55)
    w_d1 = float(w.get("d1") or 0.45)
    sw = w_jpd + w_d1
    if sw <= 0:
        w_jpd, w_d1 = 0.55, 0.45
        sw = 1.0
    w_jpd /= sw
    w_d1 /= sw

    ioi_list: List[float] = []
    for i in range(len(series)):
        z_jpd = _robust_z(jpd_ew[i], jpd_med, jpd_mad)
        z_d1 = _robust_z(d1_ew[i], d1_med, d1_mad)
        ioi = w_jpd * z_jpd + w_d1 * z_d1
        ioi_list.append(float(ioi))

    # Hysteresis to avoid oscillations.
    peak = False
    enter_run = 0
    exit_run = 0
    peak_by_date: Dict[str, bool] = {}
    ioi_by_date: Dict[str, float] = {}
    for i, row in enumerate(series):
        od = row["op_date"]
        ioi = float(ioi_list[i])
        ioi_by_date[od] = ioi

        if not peak:
            if ioi >= enter_thr:
                enter_run += 1
            else:
                enter_run = 0
            if enter_run >= enter_days:
                peak = True
                exit_run = 0
        else:
            if ioi <= exit_thr:
                exit_run += 1
            else:
                exit_run = 0
            if exit_run >= exit_days:
                peak = False
                enter_run = 0
        peak_by_date[od] = bool(peak)

    # Trim stored series to last max_days_store
    if max_days_store > 0 and len(series) > max_days_store:
        keep = set([d.isoformat() for d in dates[-max_days_store:]])
        peak_by_date = {k: v for k, v in peak_by_date.items() if k in keep}
        ioi_by_date = {k: v for k, v in ioi_by_date.items() if k in keep}
        series = [x for x in series if x["op_date"] in keep]

    # Data week fields for snapshot/audit
    data_week_start = ""
    data_week_end = effective_end_op
    try:
        data_week_start = _op_week_start(max_d).isoformat()
    except Exception:
        data_week_start = ""

    out = {
        "enabled": True,
        "mode": "auto_drift",
        "refresh_mode": refresh_mode,
        "use_last_complete_week": use_last_complete_week,
        "generated_at_utc": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "max_op_date_seen": max_op_seen,
        "max_op_date": effective_end_op,
        "last_op_date": effective_end_op,
        "last_refresh_week_start": last_refresh_week_start or prev_week or "",
        "data_week_start": data_week_start,
        "data_week_end": data_week_end,
        "baseline": {"jpd_median": jpd_med, "jpd_mad": jpd_mad, "d1_median": d1_med, "d1_mad": d1_mad},
        "peak_mode_current": bool(peak_by_date.get(effective_end_op, False)),
        "ioi_current": float(ioi_by_date.get(effective_end_op, 0.0)),
        "peak_by_op_date": peak_by_date,
        "ioi_by_op_date": ioi_by_date,
        "series": series,
    }
    return out


def _maybe_write_weekly_model_snapshot(db: DB, *, cluster_state: Dict[str, Any], season_state: Dict[str, Any]):
    """Write one stable snapshot per operational week for audit/diagnostics.

    We key snapshots by week_start_op (Mié). This is intentionally conservative:
    it writes only if both cluster and seasonality are enabled and the snapshot
    for that week does not exist yet.
    """
    try:
        if not isinstance(cluster_state, dict) or not bool(cluster_state.get("enabled", False)):
            return
        if not isinstance(season_state, dict) or not bool(season_state.get("enabled", False)):
            return
        week_start = str(season_state.get("data_week_start") or "") or str(cluster_state.get("data_week_start") or "")
        week_end = str(season_state.get("data_week_end") or "") or str(cluster_state.get("data_week_end") or "")
        if not week_start or not week_end:
            return
        # One record per week_start.
        if db.weekly_audit_exists(week_start):
            return

        # Fetch metrics for that operational week (7 days).
        daily = _fetch_daily_jornadas_metrics(db, week_start, week_end)
        jpd_sum = sum(int(x.get("jpd") or 0) for x in (daily or []))
        d1_num = 0.0
        for x in (daily or []):
            j = float(x.get("jpd") or 0.0)
            d1_num += float(x.get("d1_rate") or 0.0) * j
        d1_rate_mean = (d1_num / float(jpd_sum)) if jpd_sum > 0 else 0.0

        # IOI stats for the week from season_state
        ioi_by = (season_state.get("ioi_by_op_date") or {}) if isinstance(season_state.get("ioi_by_op_date"), dict) else {}
        dates = _date_range(datetime.strptime(week_start, "%Y-%m-%d").date(), datetime.strptime(week_end, "%Y-%m-%d").date())
        ioi_vals: List[float] = []
        for d in dates:
            od = d.isoformat()
            if od in ioi_by:
                try:
                    ioi_vals.append(float(ioi_by.get(od) or 0.0))
                except Exception:
                    pass
        ioi_mean = (sum(ioi_vals) / float(len(ioi_vals))) if ioi_vals else float(season_state.get("ioi_current") or 0.0)
        try:
            ioi_end = float(ioi_by.get(week_end) or 0.0)
        except Exception:
            ioi_end = float(season_state.get("ioi_current") or 0.0)

        peak_by = (season_state.get("peak_by_op_date") or {}) if isinstance(season_state.get("peak_by_op_date"), dict) else {}
        peak_mode = bool(peak_by.get(week_end, bool(season_state.get("peak_mode_current", False))))

        # Cluster counts (prefer state; fallback to counting employee states).
        cluster_counts = cluster_state.get("cluster_counts") if isinstance(cluster_state.get("cluster_counts"), dict) else None
        if not cluster_counts:
            cluster_counts = {}
            try:
                # Count current employee states by model_cluster_id
                if db.engine == "sqlite":
                    with db._lock:
                        rows = db._conn.execute("SELECT state_json FROM employee_jornada_state").fetchall()
                    for (s,) in rows or []:
                        try:
                            st = json.loads(s) if isinstance(s, str) else {}
                            cid = str((st or {}).get("model_cluster_id") or "").strip()
                            if cid:
                                cluster_counts[cid] = int(cluster_counts.get(cid, 0)) + 1
                        except Exception:
                            continue
                else:
                    with db._conn.cursor() as cur:
                        cur.execute("SELECT state_json FROM employee_jornada_state")
                        rows = cur.fetchall()
                    for (s,) in rows or []:
                        try:
                            st = s if isinstance(s, dict) else (json.loads(s) if isinstance(s, str) else {})
                            cid = str((st or {}).get("model_cluster_id") or "").strip()
                            if cid:
                                cluster_counts[cid] = int(cluster_counts.get(cid, 0)) + 1
                        except Exception:
                            continue
            except Exception:
                cluster_counts = {}

        rec = {
            "week_start_op": week_start,
            "week_end_op": week_end,
            "created_at_utc": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "max_op_date_used": str(season_state.get("last_op_date") or week_end),
            "peak_mode": bool(peak_mode),
            "ioi_mean": float(ioi_mean),
            "ioi_end": float(ioi_end),
            "jpd_sum": int(jpd_sum),
            "d1_rate_mean": float(d1_rate_mean),
            "cluster_k": int(cluster_state.get("k") or 0),
            "cluster_counts": cluster_counts if isinstance(cluster_counts, dict) else {},
            "notes": "synced_opweek_snapshot_v1",
        }
        db.upsert_weekly_audit(rec)
    except Exception:
        # Never break indexing because of audit
        return


def _profile_empty() -> Dict[str, Any]:
    return {
        "v": MODEL_VERSION,
        "n": 0.0,
        "cross_n": 0.0,
        "close_win_n": 0.0,
        "late_n": 0.0,
        "cnt_hist": {},
        "cross_cnt_hist": {},
        "updated_at": "",
    }


def _decay_num(x: Any, decay: float) -> float:
    try:
        return float(x or 0.0) * decay
    except Exception:
        return 0.0


def _decay_map(m: Any, decay: float) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if not isinstance(m, dict):
        return out
    for k, v in m.items():
        try:
            fv = float(v or 0.0) * decay
            if fv >= 1e-6:
                out[str(k)] = fv
        except Exception:
            continue
    return out


def _profile_update(
    profile: Dict[str, Any],
    *,
    seq_cnt: int,
    cross_midnight: bool,
    end_in_close_window: bool,
    has_late_signature: bool,
    alpha: float,
) -> Dict[str, Any]:
    """EMA-style update of a lightweight profile."""
    if not isinstance(profile, dict):
        profile = _profile_empty()
    alpha = float(alpha or 0.05)
    if alpha <= 0.0:
        alpha = 0.05
    if alpha >= 1.0:
        alpha = 0.25
    decay = 1.0 - alpha

    profile["n"] = _decay_num(profile.get("n"), decay) + alpha
    profile["cross_n"] = _decay_num(profile.get("cross_n"), decay) + (alpha if cross_midnight else 0.0)
    profile["close_win_n"] = _decay_num(profile.get("close_win_n"), decay) + (alpha if end_in_close_window else 0.0)
    profile["late_n"] = _decay_num(profile.get("late_n"), decay) + (alpha if has_late_signature else 0.0)

    cnt_hist = _decay_map(profile.get("cnt_hist"), decay)
    cnt_hist[str(int(seq_cnt))] = float(cnt_hist.get(str(int(seq_cnt)), 0.0)) + alpha
    profile["cnt_hist"] = cnt_hist

    cross_cnt_hist = _decay_map(profile.get("cross_cnt_hist"), decay)
    if cross_midnight:
        cross_cnt_hist[str(int(seq_cnt))] = float(cross_cnt_hist.get(str(int(seq_cnt)), 0.0)) + alpha
    profile["cross_cnt_hist"] = cross_cnt_hist

    profile["updated_at"] = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    profile["v"] = MODEL_VERSION
    return profile


def _profile_rate(profile: Dict[str, Any], num_key: str, den_key: str) -> float:
    try:
        num = float(profile.get(num_key) or 0.0)
        den = float(profile.get(den_key) or 0.0)
        if den <= 0:
            return 0.0
        return max(0.0, min(1.0, num / den))
    except Exception:
        return 0.0


def _profile_mode_cnt(hist: Dict[str, Any]) -> int:
    if not isinstance(hist, dict) or not hist:
        return 0
    best_k = None
    best_v = -1.0
    for k, v in hist.items():
        try:
            fv = float(v)
            if fv > best_v:
                best_v = fv
                best_k = k
        except Exception:
            continue
    try:
        return int(best_k) if best_k is not None else 0
    except Exception:
        return 0


def _cluster_id_for_employee(base_profile: Dict[str, Any]) -> str:
    """Very-light clustering without external deps.

    The goal is not perfect clustering, but a stable behavior bucket so that
    new/low-data employees can borrow priors from a similar group.
    """
    if not isinstance(base_profile, dict):
        return "UNKNOWN"
    cross_rate = _profile_rate(base_profile, "cross_n", "n")
    mode_cnt = _profile_mode_cnt(base_profile.get("cnt_hist") or {})

    cross_bucket = "CROSS" if cross_rate >= 0.20 else "SAME"
    if mode_cnt in (2, 4, 5, 6):
        return f"{cross_bucket}_{mode_cnt}"
    if mode_cnt >= 7:
        return f"{cross_bucket}_EXTRA"
    return f"{cross_bucket}_OTHER"


# ----------------------------
# Cluster v2 (rolling window k-means, stable labels + hysteresis)
# ----------------------------

def _fetch_employee_window_metrics(db: DB, start_op_date: str, end_op_date: str) -> List[Dict[str, Any]]:
    """Lightweight per-employee metrics for clustering, computed from jornadas.

    We intentionally use only stable signals (volume, D+1 rate, event-count mix) and
    avoid heavy dependencies.
    """
    if not start_op_date or not end_op_date:
        return []

    if db.engine == "sqlite":
        with db._lock:
            rows = db._conn.execute(
                "SELECT employee_id, "
                "COUNT(*) as n, "
                "SUM(CASE WHEN closed=1 AND end_time IS NOT NULL AND substr(end_time,1,10) > op_date THEN 1 ELSE 0 END) as d1n, "
                "SUM(CASE WHEN events_count=1 THEN 1 ELSE 0 END) as c1, "
                "SUM(CASE WHEN events_count=2 THEN 1 ELSE 0 END) as c2, "
                "SUM(CASE WHEN events_count=4 THEN 1 ELSE 0 END) as c4, "
                "SUM(CASE WHEN events_count=5 THEN 1 ELSE 0 END) as c5, "
                "SUM(CASE WHEN events_count>=6 THEN 1 ELSE 0 END) as c6p, "
                "AVG(events_count) as avg_ev, "
                "AVG(duration_minutes) as avg_dur "
                "FROM jornadas "
                "WHERE op_date>=? AND op_date<=? "
                "  AND employee_id IS NOT NULL AND employee_id<>'' "
                "  AND (incidencias_json NOT LIKE '%PRUEBA_REGISTRO%') "
                "GROUP BY employee_id",
                (start_op_date, end_op_date),
            ).fetchall()
    else:
        with db._conn.cursor() as cur:
            cur.execute(
                "SELECT employee_id, "
                "COUNT(*) as n, "
                "SUM(CASE WHEN closed=1 AND end_time IS NOT NULL AND substring(end_time from 1 for 10) > op_date THEN 1 ELSE 0 END) as d1n, "
                "SUM(CASE WHEN events_count=1 THEN 1 ELSE 0 END) as c1, "
                "SUM(CASE WHEN events_count=2 THEN 1 ELSE 0 END) as c2, "
                "SUM(CASE WHEN events_count=4 THEN 1 ELSE 0 END) as c4, "
                "SUM(CASE WHEN events_count=5 THEN 1 ELSE 0 END) as c5, "
                "SUM(CASE WHEN events_count>=6 THEN 1 ELSE 0 END) as c6p, "
                "AVG(events_count) as avg_ev, "
                "AVG(duration_minutes) as avg_dur "
                "FROM jornadas "
                "WHERE op_date>=%s AND op_date<=%s "
                "  AND employee_id IS NOT NULL AND employee_id<>'' "
                "  AND (incidencias_json NOT LIKE '%%PRUEBA_REGISTRO%%') "
                "GROUP BY employee_id",
                (start_op_date, end_op_date),
            )
            rows = cur.fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows or []:
        emp, n, d1n, c1, c2, c4, c5, c6p, avg_ev, avg_dur = r
        out.append(
            {
                "employee_id": str(emp or ""),
                "n": int(n or 0),
                "d1n": int(d1n or 0),
                "c1": int(c1 or 0),
                "c2": int(c2 or 0),
                "c4": int(c4 or 0),
                "c5": int(c5 or 0),
                "c6p": int(c6p or 0),
                "avg_ev": float(avg_ev or 0.0),
                "avg_dur": float(avg_dur or 0.0),
            }
        )
    return out


def _kmeans_deterministic(X: List[List[float]], k: int, max_iter: int = 15) -> Tuple[List[int], List[List[float]]]:
    """Deterministic k-means (no external deps).

    Initialization is deterministic by picking evenly spaced points in the input order.
    """
    import math

    n = len(X)
    if n == 0:
        return [], []
    k = int(k or 1)
    k = max(1, min(k, n))
    d = len(X[0])

    # Init centroids: evenly spaced points (deterministic).
    if k == 1:
        cents = [X[0][:]]
    else:
        idxs = [int(round(i * (n - 1) / float(k - 1))) for i in range(k)]
        cents = [X[i][:] for i in idxs]

    assign = [-1] * n

    def dist2(a: List[float], b: List[float]) -> float:
        s = 0.0
        for j in range(d):
            v = float(a[j]) - float(b[j])
            s += v * v
        return s

    for _ in range(int(max_iter or 10)):
        changed = False

        # Assign step.
        for i in range(n):
            best_c = 0
            best_d = dist2(X[i], cents[0])
            for c in range(1, k):
                dd = dist2(X[i], cents[c])
                if dd < best_d:
                    best_d = dd
                    best_c = c
            if assign[i] != best_c:
                assign[i] = best_c
                changed = True

        # Update step.
        sums = [[0.0] * d for _ in range(k)]
        cnts = [0] * k
        for i in range(n):
            c = assign[i]
            cnts[c] += 1
            xi = X[i]
            for j in range(d):
                sums[c][j] += float(xi[j])

        for c in range(k):
            if cnts[c] > 0:
                cents[c] = [sums[c][j] / float(cnts[c]) for j in range(d)]
            else:
                # Empty cluster: pick point farthest from its centroid (deterministic tie-breaker by index)
                far_i = 0
                far_d = -1.0
                for i in range(n):
                    dd = dist2(X[i], cents[assign[i]])
                    if dd > far_d:
                        far_d = dd
                        far_i = i
                cents[c] = X[far_i][:]

        if not changed:
            break

    return assign, cents


def _cluster_sort_key(centroid: List[float]) -> Tuple[float, float, float]:
    # stable ordering -> stable labels across runs
    if not centroid:
        return (0.0, 0.0, 0.0)
    d1 = float(centroid[0]) if len(centroid) > 0 else 0.0
    avg_ev = float(centroid[5]) if len(centroid) > 5 else 0.0
    noise = float(centroid[1]) if len(centroid) > 1 else 0.0  # p1
    return (d1, avg_ev, noise)


def _compute_cluster_v2_from_db(db: DB, cfg: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """Compute/refresh rolling-window clustering and return (state, assignments).

    - Features are computed from the rolling window of jornadas (window_days), optionally aligned to
      the operational week (Mié→Mar) for stable refresh.
    - Assignments can be stabilized by hysteresis (requires N consecutive refreshes to switch).
    """
    if not isinstance(cfg, dict) or not bool(cfg.get("enabled", False)):
        return {"enabled": False}, {}

    # Config
    window_days = int(cfg.get("window_days") or 28)
    refresh_days = int(cfg.get("refresh_days") or 7)
    k = int(cfg.get("k") or 6)
    min_jornadas = int(cfg.get("min_jornadas") or 8)
    hysteresis_runs = int(cfg.get("hysteresis_runs") or 2)

    refresh_mode = str(cfg.get("refresh_mode") or "operational_week").strip().lower()
    use_last_complete_week = bool(cfg.get("use_last_complete_week", True))

    feature_names = ["d1_rate", "p1", "p2", "p4", "p5", "p6p", "avg_ev"]

    def _op_week_start(d: date) -> date:
        wd = d.weekday()  # Mon=0
        delta_to_wed = (wd - 2) % 7  # Wed=2
        return d - timedelta(days=delta_to_wed)

    # Determine max/min op_date (bounds)
    max_op: Optional[str] = None
    min_op: Optional[str] = None
    if db.engine == "sqlite":
        with db._lock:
            row = db._conn.execute("SELECT MAX(op_date), MIN(op_date) FROM jornadas").fetchone()
            if row:
                max_op = str(row[0]) if row[0] else None
                min_op = str(row[1]) if row[1] else None
    else:
        with db._conn.cursor() as cur:
            cur.execute("SELECT MAX(op_date), MIN(op_date) FROM jornadas")
            row = cur.fetchone()
            if row:
                max_op = str(row[0]) if row[0] else None
                min_op = str(row[1]) if row[1] else None

    # If there are no jornadas yet, return a well-formed empty state (never crash).
    if not max_op:
        state = {
            "enabled": True,
            "k": 0,
            "window_days": window_days,
            "refresh_days": refresh_days,
            "refresh_mode": refresh_mode,
            "use_last_complete_week": use_last_complete_week,
            "last_refresh_week_start": "",
            "max_op_date_seen": "",
            "last_op_date": "",
            "labels": [],
            "feature_names": feature_names,
            "mu": [],
            "sigma": [],
            "centroids_z": [],
            "clustered_employees": 0,
            "cluster_counts": {},
            "data_week_start": "",
            "data_week_end": "",
            "note": "no_jornadas",
        }
        return state, {}

    max_op_seen = max_op

    # Load previous state (for refresh gating)
    try:
        prev = _json_load((db.get_state("model_cluster_v2_state") or "").strip())
    except Exception:
        prev = {}
    if not isinstance(prev, dict):
        prev = {}
    prev_last = str(prev.get("last_op_date") or "")
    prev_week = str(prev.get("last_refresh_week_start") or "")

    # Determine effective end op_date based on refresh policy.
    effective_end_op = max_op
    week_start_s = ""
    try:
        d_now = datetime.strptime(max_op, "%Y-%m-%d").date()

        if refresh_mode in ("operational_week", "weekly", "op_week"):
            wk_start = _op_week_start(d_now)
            week_start_s = wk_start.isoformat()

            # Refresh at most once per operational week (Mié→Mar).
            if prev_week and prev_week == week_start_s and bool(prev.get("enabled", True)):
                # Reuse only if previous state includes the snapshot fields we expect.
                if isinstance(prev.get("cluster_counts"), dict) and str(prev.get("data_week_end") or ""):
                    return prev, {}

            # Prefer the last COMPLETE op-week (ends Tuesday) for stability.
            if use_last_complete_week:
                end_d = wk_start - timedelta(days=1)  # Tuesday of previous op week
                if min_op:
                    try:
                        d_min = datetime.strptime(min_op, "%Y-%m-%d").date()
                        if end_d < d_min:
                            end_d = d_now
                    except Exception:
                        pass
                effective_end_op = end_d.isoformat()
            else:
                effective_end_op = max_op

        else:
            # Interval refresh
            if prev_last:
                d_prev = datetime.strptime(prev_last, "%Y-%m-%d").date()
                if (d_now - d_prev).days < max(1, refresh_days) and bool(prev.get("enabled", True)):
                    return prev, {}

    except Exception:
        # Best-effort fallback: proceed with max_op as effective end.
        effective_end_op = max_op

    max_op_eff = effective_end_op

    # Compute window start (optionally aligned to op-week boundaries)
    start_op = max_op_eff
    try:
        end_d = datetime.strptime(max_op_eff, "%Y-%m-%d").date()
        if refresh_mode in ("operational_week", "weekly", "op_week"):
            weeks = max(1, (max(1, window_days) + 6) // 7)
            window_days_eff = weeks * 7
            start_d = end_d - timedelta(days=window_days_eff - 1)
            start_d = _op_week_start(start_d)
        else:
            start_d = end_d - timedelta(days=max(1, window_days) - 1)
        start_op = start_d.isoformat()
    except Exception:
        start_op = max_op_eff

    # Feature extraction in the window
    rows = _fetch_employee_window_metrics(db, start_op, max_op_eff)

    emps: List[str] = []
    feats: List[List[float]] = []
    for r in rows:
        n = int(r.get("n") or 0)
        if n < min_jornadas:
            continue

        d1n = int(r.get("d1n") or 0)
        c1 = int(r.get("c1") or 0)
        c2 = int(r.get("c2") or 0)
        c4 = int(r.get("c4") or 0)
        c5 = int(r.get("c5") or 0)
        c6p = int(r.get("c6p") or 0)
        avg_ev = float(r.get("avg_ev") or 0.0)

        d1_rate = (d1n / float(n)) if n > 0 else 0.0
        p1 = (c1 / float(n)) if n > 0 else 0.0
        p2 = (c2 / float(n)) if n > 0 else 0.0
        p4 = (c4 / float(n)) if n > 0 else 0.0
        p5 = (c5 / float(n)) if n > 0 else 0.0
        p6p = (c6p / float(n)) if n > 0 else 0.0

        emp_id = str(r.get("employee_id") or "").strip()
        if not emp_id:
            continue
        emps.append(emp_id)
        feats.append([float(d1_rate), float(p1), float(p2), float(p4), float(p5), float(p6p), float(avg_ev)])

    # Snapshot week context
    data_week_start = ""
    data_week_end = str(max_op_eff or "")
    try:
        if refresh_mode in ("operational_week", "weekly", "op_week") and data_week_end:
            _end_d = datetime.strptime(data_week_end, "%Y-%m-%d").date()
            data_week_start = _op_week_start(_end_d).isoformat()
    except Exception:
        data_week_start = ""

    # Not enough employees => return empty clustering (do NOT crash; keep a visible state).
    if len(emps) < 5 or len(feats) < 5:
        state = {
            "enabled": True,
            "k": 0,
            "window_days": window_days,
            "refresh_days": refresh_days,
            "refresh_mode": refresh_mode,
            "use_last_complete_week": use_last_complete_week,
            "last_refresh_week_start": week_start_s or prev_week or "",
            "max_op_date_seen": max_op_seen,
            "last_op_date": max_op_eff,
            "labels": [],
            "feature_names": feature_names,
            "mu": [],
            "sigma": [],
            "centroids_z": [],
            "clustered_employees": len(emps),
            "cluster_counts": {},
            "data_week_start": data_week_start,
            "data_week_end": data_week_end,
            "window_start": start_op,
            "window_end": max_op_eff,
            "note": "too_few_employees_for_kmeans",
        }
        return state, {}

    # Deterministic ordering for stability
    order = sorted(range(len(emps)), key=lambda i: emps[i])
    emps = [emps[i] for i in order]
    feats = [feats[i] for i in order]

    # Standardize
    m = len(feats)
    d = len(feature_names)
    mu = [0.0] * d
    for j in range(d):
        mu[j] = sum(float(feats[i][j]) for i in range(m)) / float(m)

    sigma = [0.0] * d
    for j in range(d):
        var = sum((float(feats[i][j]) - mu[j]) ** 2 for i in range(m)) / float(m)
        sigma[j] = (var ** 0.5) if var > 1e-12 else 1.0

    X = [[(float(feats[i][j]) - mu[j]) / sigma[j] for j in range(d)] for i in range(m)]

    k_eff = max(2, min(int(k or 6), max(2, int(m ** 0.5) + 2), m))
    assign, cents = _kmeans_deterministic(X, k_eff, max_iter=int(cfg.get("max_iter") or 15))

    # Stable label order (sort centroids)
    cent_with_idx = list(enumerate(cents))
    cent_with_idx.sort(key=lambda kv: _cluster_sort_key(kv[1]))
    old_to_new = {old: new for new, (old, _) in enumerate(cent_with_idx)}
    labels = [f"K{i}" for i in range(len(cent_with_idx))]

    # Assign employees
    assignments: Dict[str, str] = {}
    for i, emp in enumerate(emps):
        ci_old = int(assign[i] if i < len(assign) else 0)
        ci_new = old_to_new.get(ci_old, 0)
        assignments[emp] = labels[ci_new]

    # Apply hysteresis using employee state (stabilizes cluster flips).
    if hysteresis_runs > 1:
        for emp, new_c in list(assignments.items()):
            st = db.get_employee_jornada_state(emp) or {}
            if not isinstance(st, dict):
                st = {}
            prev_c = (st.get("model_cluster_id") or "").strip()
            cand = (st.get("model_cluster_candidate") or "").strip()
            try:
                streak = int(st.get("model_cluster_candidate_streak") or 0)
            except Exception:
                streak = 0

            final_c = new_c
            if prev_c and prev_c != new_c:
                if cand == new_c:
                    streak += 1
                else:
                    cand = new_c
                    streak = 1
                if streak >= hysteresis_runs:
                    prev_c = new_c
                    cand = ""
                    streak = 0
                    final_c = new_c
                else:
                    final_c = prev_c
            else:
                cand = ""
                streak = 0
                final_c = new_c if new_c else (prev_c or "")

            if final_c != new_c:
                assignments[emp] = final_c

            st["model_cluster_id"] = final_c
            st["model_cluster_candidate"] = cand
            st["model_cluster_candidate_streak"] = int(streak)
            try:
                db.upsert_employee_jornada_state(emp, st)
            except Exception:
                pass

    # Cluster counts (after hysteresis) for audit / dashboard.
    cluster_counts: Dict[str, int] = {}
    for _c in (assignments or {}).values():
        if not _c:
            continue
        cluster_counts[str(_c)] = int(cluster_counts.get(str(_c), 0)) + 1

    cents_sorted = [cent for _, cent in cent_with_idx]

    state = {
        "enabled": True,
        "k": int(k_eff),
        "window_days": window_days,
        "refresh_days": refresh_days,
        "refresh_mode": refresh_mode,
        "use_last_complete_week": use_last_complete_week,
        "last_refresh_week_start": week_start_s or prev_week or "",
        "max_op_date_seen": max_op_seen,
        "last_op_date": max_op_eff,
        "labels": labels,
        "feature_names": feature_names,
        "mu": mu,
        "sigma": sigma,
        "centroids_z": cents_sorted,
        "clustered_employees": len(emps),
        "cluster_counts": cluster_counts,
        "data_week_start": data_week_start,
        "data_week_end": data_week_end,
        "generated_at_utc": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "window_start": start_op,
        "window_end": max_op_eff,
        "note": "kmeans_deterministic_zspace",
    }
    return state, assignments


def _compute_carryback_confidence(
    *,
    st: Dict[str, Any],
    model_ctx: Dict[str, Any],
    op_date: str,
    ev_local: datetime,
    seq_cnt_before: int,
    has_late_signature: bool,
    require_late_signature: bool,
    allow_no_late: bool,
    min_prev_events: int,
    lookahead_additional_events: int,
    entry_start_t: time,
    close_end_t: time,
    cut_t: time,
    seasonality: Dict[str, Any],
    confidence_cfg: Dict[str, Any],
) -> Tuple[float, float, List[str]]:
    """Return (confidence, p_prior, reason_codes)."""

    reasons: List[str] = []
    t = ev_local.time()
    in_close_window = (cut_t <= t <= close_end_t)
    after_entry_start = (t >= entry_start_t)
    if in_close_window:
        reasons.append("IN_CLOSE_WINDOW")
    if after_entry_start:
        reasons.append("AFTER_ENTRY_START")

    min_samples = int(confidence_cfg.get("min_samples") or 10)
    if min_samples <= 0:
        min_samples = 10

    base_prof = st.get("model_profile_base") or {}
    seasonal_map = st.get("model_profiles_seasonal") or {}
    season_k = _season_key(op_date, seasonality) if seasonality.get("enabled", True) else ""
    season_prof = seasonal_map.get(season_k) if isinstance(seasonal_map, dict) else None

    cluster_id = (st.get("model_cluster_id") or "").strip() or _cluster_id_for_employee(base_prof)
    clusters = model_ctx.get("cluster_profiles") or {}
    cluster_prof = clusters.get(cluster_id) if isinstance(clusters, dict) else None
    global_prof = model_ctx.get("global_profile") or {}

    def _close_win_rate(p: Any) -> float:
        if not isinstance(p, dict):
            return 0.0
        return _profile_rate(p, "close_win_n", "cross_n")

    p_emp = _close_win_rate(base_prof)
    p_month = _close_win_rate(season_prof) if season_prof else 0.0
    recent_prof = st.get("model_profile_recent") or {}
    p_recent = _close_win_rate(recent_prof) if isinstance(recent_prof, dict) else 0.0
    p_cluster = _close_win_rate(cluster_prof) if cluster_prof else 0.0
    p_global = _close_win_rate(global_prof)

    # Seasonality v2: auto-drift peak/offpeak -> dynamic weights base/month/recent.
    season_v2 = model_ctx.get("seasonality_v2") or {}
    peak_by = model_ctx.get("peak_by_op_date") or {}
    ioi_by = model_ctx.get("ioi_by_op_date") or {}
    v2_enabled = bool(season_v2.get("enabled", False))
    peak_mode = bool(peak_by.get(op_date, False)) if v2_enabled else False
    ioi = float(ioi_by.get(op_date, 0.0)) if v2_enabled else 0.0
    if v2_enabled:
        reasons.append("PEAK_MODE" if peak_mode else "OFFPEAK_MODE")

    if v2_enabled:
        w_cfg = (season_v2.get("weights_peak") if peak_mode else season_v2.get("weights_offpeak")) or {}
        w_base_c = float(w_cfg.get("base") or 0.55)
        w_month_c = float(w_cfg.get("month") or 0.25)
        w_recent_c = float(w_cfg.get("recent") or 0.20)
        w_cluster_c = float(season_v2.get("w_cluster") or 0.10)
        w_global_c = float(season_v2.get("w_global") or 0.05)

        n_emp = float(base_prof.get("n") or 0.0) if isinstance(base_prof, dict) else 0.0
        n_month = float(season_prof.get("n") or 0.0) if isinstance(season_prof, dict) else 0.0
        n_recent = float(recent_prof.get("n") or 0.0) if isinstance(recent_prof, dict) else 0.0

        w_base = w_base_c * min(1.0, n_emp / float(min_samples))
        w_month = w_month_c * min(1.0, n_month / float(min_samples))
        w_recent = w_recent_c * min(1.0, n_recent / float(min_samples))
        w_cluster = w_cluster_c
        w_global = w_global_c

        w_sum = w_base + w_month + w_recent + w_cluster + w_global
        if w_sum <= 0:
            p_prior = 0.5
            reasons.append("PRIOR_DEFAULT")
        else:
            p_prior = (w_base * p_emp + w_month * p_month + w_recent * p_recent + w_cluster * p_cluster + w_global * p_global) / w_sum
            reasons.append("PRIOR_V2")
    else:
        # Backward-compatible v1 hierarchy (employee + month + cluster + global).
        n_emp = float(base_prof.get("n") or 0.0) if isinstance(base_prof, dict) else 0.0
        n_month = float(season_prof.get("n") or 0.0) if isinstance(season_prof, dict) else 0.0
        w_emp = 0.55 * min(1.0, n_emp / float(min_samples))
        w_month = 0.30 * min(1.0, n_month / float(min_samples))
        w_cluster = 0.10
        w_global = 0.05
        w_sum = w_emp + w_month + w_cluster + w_global
        if w_sum <= 0:
            p_prior = 0.5
            reasons.append("PRIOR_DEFAULT")
        else:
            p_prior = (w_emp * p_emp + w_month * p_month + w_cluster * p_cluster + w_global * p_global) / w_sum
            reasons.append("PRIOR_HIER")
    # Start from prior, then adjust with deterministic evidence.
    conf = 0.5 + (p_prior - 0.5) * 0.8

    if require_late_signature:
        if has_late_signature:
            conf += 0.10
            reasons.append("HAS_LATE_SIG")
        elif allow_no_late:
            conf += 0.02
            reasons.append("ALLOW_NO_LATE_BY_PATTERN")
        else:
            conf -= 0.12
            reasons.append("NO_LATE_SIG")

    if seq_cnt_before >= int(min_prev_events or 2):
        conf += 0.08
        reasons.append("MIN_PREV_OK")
    else:
        conf -= 0.15
        reasons.append("MIN_PREV_FAIL")

    if lookahead_additional_events >= 2:
        conf -= 0.30
        reasons.append("LOOKAHEAD_ENTRY")

    if after_entry_start:
        # Small penalty only (many real closures can happen after entry_start in your operation).
        conf -= 0.05

    if not in_close_window:
        conf = 0.0
        reasons.append("OUTSIDE_WINDOW")

    conf = max(0.0, min(1.0, conf))
    return conf, p_prior, reasons


def _looks_like_placeholder_id(emp_id: str) -> bool:
    s = (emp_id or "").strip()
    if not s or (not s.isdigit()):
        return False
    if len(s) >= 18 and s.startswith("1844674407"):
        return True
    try:
        v = int(s)
        return v <= 0 or v >= 2**63
    except Exception:
        return False


def _parse_utc(iso_z: str) -> Optional[datetime]:

    if not iso_z:
        return None
    s = iso_z.strip()
    if s.endswith("Z"):
        s = s[:-1]
    try:
        dt = datetime.fromisoformat(s)
        return dt.replace(tzinfo=UTC)
    except Exception:
        return None


def _to_iso_z(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _cutoff_time(hhmm: str) -> time:
    hhmm = (hhmm or "03:00").strip()
    h, m = hhmm.split(":")
    return time(int(h), int(m))





def employee_key(emp: str) -> str:
    """Key used to group events that sometimes arrive with different zero-padding."""
    s = (emp or "").strip()
    if not s:
        return ""
    if s.isdigit() and len(s) <= 3:
        # Only de-pad short numeric IDs; keep long IDs (e.g., card/person numbers) as-is.
        try:
            return str(int(s))
        except Exception:
            return s
    return s


def choose_display_id(events: List[Dict[str, Any]]) -> str:
    """Pick the best human-facing employee_id for the grouped events."""
    raw = []
    for e in events:
        v = (e.get("employee_id") or "").strip()
        if _looks_like_placeholder_id(v):
            v = (e.get("employee_name") or "").strip() or v
        if v:
            raw.append(v)

    if not raw:
        return ""

    counts = Counter(raw)

    # Prefer common legacy formats with leading zeros if present (e.g., 003 / 081).
    candidates = [k for k in counts if k.isdigit() and len(k) == 3 and k.startswith("0")]
    if candidates:
        return max(candidates, key=lambda k: (counts[k], len(k)))

    candidates = [k for k in counts if k.isdigit() and len(k) == 2 and k.startswith("0")]
    if candidates:
        return max(candidates, key=lambda k: (counts[k], len(k)))

    return max(counts, key=lambda k: (counts[k], len(k)))



def _hhmm_to_time(hhmm: str, fallback: str) -> time:

    s = (hhmm or "").strip()
    if not s:
        s = fallback
    try:
        parts = s.split(":")
        if len(parts) != 2:
            raise ValueError
        h = int(parts[0])
        m = int(parts[1])
        if h < 0 or h > 23 or m < 0 or m > 59:
            raise ValueError
        return time(h, m)
    except Exception:
        fb = (fallback or "03:00").strip()
        h, m = fb.split(":")
        return time(int(h), int(m))
def operational_date(local_dt: datetime, cutoff_hhmm: str) -> str:

    cut = _cutoff_time(cutoff_hhmm)
    d = local_dt.date()
    if local_dt.time() < cut:
        d = d - timedelta(days=1)
    return d.isoformat()


def local_bounds_for_op_date(op_date: str, cutoff_hhmm: str) -> Tuple[datetime, datetime]:

    cut = _cutoff_time(cutoff_hhmm)
    base = datetime.strptime(op_date, "%Y-%m-%d").date()
    start = datetime.combine(base, cut)
    end = datetime.combine(base + timedelta(days=1), cut)
    return start, end


def ensure_jornadas_indexed_until(
    db: DB,
    end_utc_iso: str,
    device_tz: ZoneInfo | str,
    cutoff_hhmm: str = "03:00",
    break_max_minutes: int = 75,
    rest_min_minutes: int = 240,
    debounce_minutes: int = 3,
    max_shift_hours: int = 24,
    rebuild: bool = False,
    hybrid_close: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:

    # Be tolerant: callers may pass IANA timezone string.
    if isinstance(device_tz, str):
        device_tz = ZoneInfo(device_tz)

    if rebuild:
        # Full rebuild: clear computed jornadas, but keep learning. Also reset transient per-employee
        # indexing state so the replay starts from a clean cursor while preserving learned patterns.
        db.clear_jornadas(preserve_patterns=True)
        try:
            db.reset_employee_state_preserve_patterns()
        except Exception:
            pass
        db.upsert_state("jornada_index_last_utc", "")

    end_dt = _parse_utc(end_utc_iso)
    if end_dt is None:
        raise ValueError("end_utc_iso inválido")

    last = (db.get_state("jornada_index_last_utc") or "").strip()
    if not last:

        start_dt = _min_processed_time_utc(db)
        if start_dt is None:
            db.upsert_state("jornada_index_last_utc", end_utc_iso)
            return {"indexed": 0, "start_utc": None, "end_utc": end_utc_iso}
        last_dt = start_dt
    else:
        last_dt = _parse_utc(last)
        if last_dt is None:
            last_dt = _min_processed_time_utc(db) or end_dt

    if last_dt >= end_dt:
        return {"indexed": 0, "start_utc": _to_iso_z(last_dt), "end_utc": end_utc_iso}


    hc = hybrid_close or {}
    hc_enabled = bool(hc.get("enabled", False))
    entry_start_hhmm = (hc.get("entry_start_hhmm") or "04:00").strip() or "04:00"
    close_window_end_hhmm = (hc.get("close_window_end_hhmm") or "07:30").strip() or "07:30"
    late_threshold_hhmm = (hc.get("late_threshold_hhmm") or "18:00").strip() or "18:00"
    anti_fp_window_minutes = int(hc.get("anti_fp_window_minutes", 90))
    anti_fp_min_additional_events = int(hc.get("anti_fp_min_additional_events", 2))
    require_late_signature = bool(hc.get("require_late_signature", True))
    max_join_shift_hours = int(hc.get("max_join_shift_hours", max_shift_hours))
    patterns_enabled = bool(hc.get("patterns_enabled", True))
    test_max_minutes = int(hc.get("test_max_minutes", 15))
    min_real_events = int(hc.get("min_real_events", 2))
    pattern_hist_max = int(hc.get("pattern_hist_max", 60))

    # --- Advanced learning (hierarchy + seasonality + confidence + audit) ---
    adv = hc.get("advanced_learning") or {}
    adv_enabled = bool(adv.get("enabled", False))
    seasonality = adv.get("seasonality") or {"enabled": True, "mode": "month"}
    if not isinstance(seasonality, dict):
        seasonality = {"enabled": True, "mode": "month"}
    confidence_cfg = adv.get("confidence") or {
        "enabled": True,
        "threshold": 0.60,
        "min_samples": 10,
    }
    if not isinstance(confidence_cfg, dict):
        confidence_cfg = {"enabled": True, "threshold": 0.60, "min_samples": 10}
    audit_cfg = adv.get("audit") or {"enabled": True, "export_excel": False, "keep_days": 30}
    if not isinstance(audit_cfg, dict):
        audit_cfg = {"enabled": True, "export_excel": False, "keep_days": 30}

    cluster_v2_cfg = adv.get("cluster_v2") or {
        "enabled": True,
        "window_days": 28,
        "refresh_days": 7,
        "k": 6,
        "min_jornadas": 8,
        "hysteresis_runs": 2,
    }
    if not isinstance(cluster_v2_cfg, dict):
        cluster_v2_cfg = {"enabled": False}



    model_ctx: Dict[str, Any] = {
        "enabled": adv_enabled,
        "seasonality": seasonality,
        "seasonality_v2": {},
        "peak_by_op_date": {},
        "ioi_by_op_date": {},
        "confidence": confidence_cfg,
        "audit": audit_cfg,
        "cluster_v2": cluster_v2_cfg,
        "cluster_v2_state": {},
        "global_profile": _profile_empty(),
        "cluster_profiles": {},
    }
    if adv_enabled:
        gp = _json_load((db.get_state("model_global_profile") or "").strip())
        if isinstance(gp, dict) and gp:
            model_ctx["global_profile"] = gp
        cp = _json_load((db.get_state("model_cluster_profiles") or "").strip())
        if isinstance(cp, dict):
            model_ctx["cluster_profiles"] = cp
        # Track model version in system_state for visibility.

        # Cluster v2 (weekly refresh): rolling-window k-means with stable labels.
        try:
            c2 = cluster_v2_cfg
            if isinstance(c2, dict) and bool(c2.get("enabled", False)):
                c2_state, _assign = _compute_cluster_v2_from_db(db, c2)
                if isinstance(c2_state, dict) and c2_state:
                    model_ctx["cluster_v2_state"] = c2_state
                    db.upsert_state("model_cluster_v2_state", _json_dump(c2_state))
        except Exception:
            pass

        db.upsert_state("model_version", MODEL_VERSION)

        # Seasonality v2 (auto drift peak/offpeak) - stable signals (volume + %D1)
        try:
            s2 = (adv.get("seasonality_v2") or {})
            if isinstance(s2, dict) and bool(s2.get("enabled", False)):
                s2_state = _compute_seasonality_v2_from_db(db, s2)
                model_ctx["seasonality_v2"] = s2
                model_ctx["peak_by_op_date"] = s2_state.get("peak_by_op_date") or {}
                model_ctx["ioi_by_op_date"] = s2_state.get("ioi_by_op_date") or {}
                # Persist for visibility / dashboard
                db.upsert_state("model_seasonality_v2_state", _json_dump(s2_state))
        except Exception:
            pass


    events = db.get_processed_by_utc_range(_to_iso_z(last_dt), _to_iso_z(end_dt))



    tx = db.transaction() if getattr(db, 'transaction', None) and db.engine == 'sqlite' else nullcontext()
    with tx:
        by_emp: Dict[str, List[Dict[str, Any]]] = {}
        for ev in events:
            emp_raw = (ev.get("employee_id") or "").strip()
            if _looks_like_placeholder_id(emp_raw):
                emp_raw = (ev.get("employee_name") or "").strip()
            key = employee_key(emp_raw)
            if not key:
                continue
            by_emp.setdefault(key, []).append(ev)

        total_indexed = 0
        for key, evs in by_emp.items():
            emp_display = choose_display_id(evs) or key
            evs.sort(key=lambda x: x.get("event_time_utc") or "")
            total_indexed += _index_employee_events(
                db=db,
                employee_id=emp_display,
                events=evs,
                device_tz=device_tz,
                cutoff_hhmm=cutoff_hhmm,
                break_max_minutes=break_max_minutes,
                rest_min_minutes=rest_min_minutes,
                debounce_minutes=debounce_minutes,
                max_shift_hours=max_shift_hours,
                hybrid_enabled=hc_enabled,
                entry_start_hhmm=entry_start_hhmm,
                close_window_end_hhmm=close_window_end_hhmm,
                late_threshold_hhmm=late_threshold_hhmm,
                anti_fp_window_minutes=anti_fp_window_minutes,
                anti_fp_min_additional_events=anti_fp_min_additional_events,
                require_late_signature=require_late_signature,
                max_join_shift_hours=max_join_shift_hours,
                patterns_enabled=patterns_enabled,
                test_max_minutes=test_max_minutes,
                min_real_events=min_real_events,
                pattern_hist_max=pattern_hist_max,
                model_ctx=model_ctx,
            )

        if adv_enabled:
            # Persist global + cluster profiles once per indexing run.
            try:
                db.upsert_state("model_global_profile", _json_dump(model_ctx.get("global_profile") or {}))
                db.upsert_state("model_cluster_profiles", _json_dump(model_ctx.get("cluster_profiles") or {}))
            except Exception:
                pass
            # Optional pruning of audit to avoid unlimited growth.
            try:
                if bool((audit_cfg or {}).get("enabled", False)):
                    keep_days = int((audit_cfg or {}).get("keep_days") or 0)
                    if keep_days > 0:
                        db.prune_model_audit(keep_days=keep_days)
            except Exception:
                pass

            # Refresh Cluster v2 + Seasonality v2 after indexing (both are op-week gated).
            c2_state_latest: Optional[Dict[str, Any]] = None
            s2_state_latest: Optional[Dict[str, Any]] = None

            try:
                c2 = cluster_v2_cfg
                if isinstance(c2, dict) and bool(c2.get("enabled", False)):
                    c2_state_latest, _assign = _compute_cluster_v2_from_db(db, c2)
                    if isinstance(c2_state_latest, dict) and c2_state_latest:
                        db.upsert_state("model_cluster_v2_state", _json_dump(c2_state_latest))
            except Exception:
                c2_state_latest = None

            try:
                s2 = (adv.get("seasonality_v2") or {})
                if isinstance(s2, dict) and bool(s2.get("enabled", False)):
                    s2_state_latest = _compute_seasonality_v2_from_db(db, s2)
                    if isinstance(s2_state_latest, dict) and s2_state_latest:
                        db.upsert_state("model_seasonality_v2_state", _json_dump(s2_state_latest))
            except Exception:
                s2_state_latest = None

            # Weekly snapshot (writes only once per operational week and never breaks indexing).
            try:
                _maybe_write_weekly_model_snapshot(
                    db,
                    cluster_state=c2_state_latest or (_json_load((db.get_state("model_cluster_v2_state") or "").strip()) or {}),
                    season_state=s2_state_latest or (_json_load((db.get_state("model_seasonality_v2_state") or "").strip()) or {}),
                )
            except Exception:
                pass

        db.upsert_state("jornada_index_last_utc", _to_iso_z(end_dt))
        return {"indexed": total_indexed, "start_utc": _to_iso_z(last_dt), "end_utc": end_utc_iso}


def _min_processed_time_utc(db: DB) -> Optional[datetime]:
    if db.engine == "sqlite":
        with db._lock:
            row = db._conn.execute("SELECT MIN(event_time_utc) FROM processed_events").fetchone()
            v = row[0] if row else None
    else:
        with db._conn.cursor() as cur:
            cur.execute("SELECT MIN(event_time_utc) FROM processed_events")
            row = cur.fetchone()
            v = row[0] if row else None
    if not v:
        return None
    if isinstance(v, datetime):
        return v.replace(tzinfo=UTC)
    return _parse_utc(str(v))


def _get_state(db: DB, employee_id: str) -> Dict[str, Any]:
    # IMPORTANT: preserve ALL learned keys across runs (patterns, profiles, etc.).
    st = db.get_employee_jornada_state(employee_id) or {}
    if not isinstance(st, dict):
        st = {}

    st.setdefault("current_jornada_id", st.get("current_jornada_id"))
    st.setdefault("current_start_utc", st.get("current_start_utc"))
    st.setdefault("current_start_local", st.get("current_start_local"))
    st.setdefault("current_op_date", st.get("current_op_date"))
    st["expected_role"] = (st.get("expected_role") or "IN").strip() or "IN"
    try:
        st["seq"] = int(st.get("seq") or 0)
    except Exception:
        st["seq"] = 0
    st.setdefault("last_event_utc", st.get("last_event_utc") or "")
    st.setdefault("last_event_local", st.get("last_event_local") or "")
    st.setdefault("employee_name", st.get("employee_name"))
    st["incidencias"] = list(st.get("incidencias") or [])
    st["has_late_signature"] = bool(st.get("has_late_signature") or False)

    # Advanced model state (safe defaults)
    if "model_profile_base" not in st:
        st["model_profile_base"] = _profile_empty()
    if "model_profiles_seasonal" not in st:
        st["model_profiles_seasonal"] = {}
    if "model_cluster_id" not in st:
        st["model_cluster_id"] = ""
    if "model_cluster_candidate" not in st:
        st["model_cluster_candidate"] = ""
    if "model_cluster_candidate_streak" not in st:
        st["model_cluster_candidate_streak"] = 0
    st["model_version"] = MODEL_VERSION

    return st


def _save_state(db: DB, employee_id: str, state: Dict[str, Any]):
    db.upsert_employee_jornada_state(employee_id, state)


def _index_employee_events(
    db: DB,
    employee_id: str,
    events: List[Dict[str, Any]],
    device_tz: ZoneInfo,
    cutoff_hhmm: str,
    break_max_minutes: int,
    rest_min_minutes: int,
    debounce_minutes: int,
    max_shift_hours: int,
    hybrid_enabled: bool,
    entry_start_hhmm: str,
    close_window_end_hhmm: str,
    late_threshold_hhmm: str,
    anti_fp_window_minutes: int,
    anti_fp_min_additional_events: int,
    require_late_signature: bool,
    max_join_shift_hours: int,
    patterns_enabled: bool = True,
    test_max_minutes: int = 15,
    min_real_events: int = 2,
    pattern_hist_max: int = 60,
    model_ctx: Optional[Dict[str, Any]] = None,
) -> int:
    st = _get_state(db, employee_id)
    indexed = 0

    model_ctx = model_ctx or {"enabled": False, "seasonality": {"enabled": False}, "confidence": {}, "audit": {}, "global_profile": {}, "cluster_profiles": {}}
    adv_enabled = bool(model_ctx.get("enabled", False))
    seasonality = model_ctx.get("seasonality") or {"enabled": False}
    confidence_cfg = model_ctx.get("confidence") or {}
    audit_cfg = model_ctx.get("audit") or {}

    entry_start_t = _hhmm_to_time(entry_start_hhmm, "04:00")
    close_end_t = _hhmm_to_time(close_window_end_hhmm, "07:30")
    late_t = _hhmm_to_time(late_threshold_hhmm, "18:00")
    cut_t = _cutoff_time(cutoff_hhmm)


    def ensure_parent_jornada(ev_local: datetime, ev_utc: datetime, employee_name: str, events_count: int):
        jid = st.get("current_jornada_id")
        if not jid:
            return
        if db.has_jornada(jid):
            return
        start_local = st.get("current_start_local") or ev_local.replace(microsecond=0).isoformat()
        start_utc_iso = st.get("current_start_utc") or _to_iso_z(ev_utc)
        op = (st.get("current_op_date") or operational_date(ev_local, cutoff_hhmm))
        start_utc = _parse_utc(start_utc_iso)
        dur_min = None
        if start_utc:
            dur_min = int((ev_utc - start_utc).total_seconds() // 60)
        db.upsert_jornada(
            {
                "jornada_id": jid,
                "employee_id": employee_id,
                "employee_name": st.get("employee_name") or employee_name,
                "op_date": op,
                "start_time": start_local,
                "start_time_utc": start_utc_iso,
                "end_time": ev_local.replace(microsecond=0).isoformat(),
                "end_time_utc": _to_iso_z(ev_utc),
                "duration_minutes": dur_min,
                "events_count": int(events_count or 0),
                "incidencias": st.get("incidencias") or [],
                "closed": 0,
            }
        )

    def start_new_jornada(ev_local: datetime, ev_utc: datetime, employee_name: str):
        st["current_jornada_id"] = str(uuid.uuid4())
        st["current_start_utc"] = _to_iso_z(ev_utc)
        st["current_start_local"] = ev_local.replace(microsecond=0).isoformat()
        st["current_op_date"] = operational_date(ev_local, cutoff_hhmm)
        st["expected_role"] = "IN"
        st["seq"] = 0
        st["incidencias"] = []
        st["employee_name"] = employee_name
        st["has_late_signature"] = False
        st["last_event_utc"] = ""
        st["last_event_local"] = ""
        ensure_parent_jornada(ev_local, ev_utc, employee_name, 0)

    def close_current_jornada(final_local: datetime, final_utc: datetime):
        jid = st.get("current_jornada_id")
        if not jid:
            return
        seq_cnt = int(st.get("seq") or 0)
        incid = list(st.get("incidencias") or [])
        # Only flag missing OUT when we already have at least 2 events in the jornada.
        # With a single checada we cannot infer direction reliably (could be an OUT).
        if st.get("expected_role") == "OUT" and seq_cnt >= 2:
            if "FALTA_SALIDA" not in incid:
                incid.append("FALTA_SALIDA")
        start_utc = _parse_utc(st.get("current_start_utc") or "")
        dur_min = None
        if start_utc:
            dur_min = int((final_utc - start_utc).total_seconds() // 60)

        # --- Pattern learning / clasificación (opcional) ---
        # seq_cnt already computed above
        emp_name = (st.get("employee_name") or "").strip()
        start_local_s = (st.get("current_start_local") or "").replace(" ", "T")
        start_local_dt = None
        if start_local_s:
            try:
                start_local_dt = datetime.fromisoformat(start_local_s)
            except Exception:
                start_local_dt = None
        if start_local_dt is None:
            start_local_dt = final_local
        cross_midnight = final_local.date() > start_local_dt.date()

        is_single = seq_cnt == 1
        is_short = (dur_min is not None and dur_min <= test_max_minutes)
        id_long = employee_id.isdigit() and len(employee_id) > 6
        is_test = bool(patterns_enabled) and (
            (is_single and (is_short or (not emp_name) or id_long))
            or ((seq_cnt <= 2) and is_short and ((not emp_name) or id_long))
        )
        if is_single and "EVENTO_SUELTO" not in incid:
            incid.append("EVENTO_SUELTO")
        if is_test and "PRUEBA_REGISTRO" not in incid:
            incid.append("PRUEBA_REGISTRO")

        if patterns_enabled:
            if cross_midnight and seq_cnt >= 2 and "PATRON_CIERRE_D1" not in incid:
                incid.append("PATRON_CIERRE_D1")
            if seq_cnt == 2 and "PATRON_2" not in incid:
                incid.append("PATRON_2")
            elif seq_cnt == 4 and "PATRON_4_COMIDA" not in incid:
                incid.append("PATRON_4_COMIDA")
            elif seq_cnt == 5 and "PATRON_5_CENA" not in incid:
                incid.append("PATRON_5_CENA")
            elif seq_cnt == 6 and "PATRON_6" not in incid:
                incid.append("PATRON_6")
            elif seq_cnt >= 7 and "PATRON_EXTRA_PERMISOS" not in incid:
                incid.append("PATRON_EXTRA_PERMISOS")

        if patterns_enabled and (not is_test) and seq_cnt >= min_real_events and dur_min is not None and dur_min >= 0 and dur_min <= (max_shift_hours * 60):
            hist = st.get("pattern_hist") or []
            if isinstance(hist, str):
                try:
                    hist = json.loads(hist)
                except Exception:
                    hist = []
            if not isinstance(hist, list):
                hist = []
            hist.append({"cnt": seq_cnt, "dur": int(dur_min), "cross": bool(cross_midnight), "late": bool(st.get("has_late_signature"))})
            if len(hist) > pattern_hist_max:
                hist = hist[-pattern_hist_max:]
            st["pattern_hist"] = hist
            cross_hist = [h for h in hist if isinstance(h, dict) and h.get("cross")]
            if len(cross_hist) >= 3:
                c = Counter(int(h.get("cnt") or 0) for h in cross_hist if int(h.get("cnt") or 0) > 0)
                mode_cnt = c.most_common(1)[0][0] if c else 0
                st["pattern_close_prev_min_events"] = (2 if int(mode_cnt) <= 4 else 3) if mode_cnt else 2
                no_late = sum(1 for h in cross_hist if not bool(h.get("late")))
                st["pattern_allow_cross_without_late"] = (no_late / float(len(cross_hist))) >= 0.5
            else:
                st["pattern_close_prev_min_events"] = 2
                st["pattern_allow_cross_without_late"] = False

        # --- Advanced learning (profiles + hierarchy) ---
        if adv_enabled and (not is_test) and seq_cnt >= min_real_events and dur_min is not None and dur_min >= 0 and dur_min <= (max_shift_hours * 60):
            try:
                op_date_j = (st.get("current_op_date") or operational_date(start_local_dt, cutoff_hhmm))
                end_in_close_window = bool(cross_midnight) and (cut_t <= final_local.time() <= close_end_t)

                # Employee base profile
                alpha_base = float(((model_ctx.get("seasonality_v2") or {}).get("alpha") or {}).get("base") or (confidence_cfg or {}).get("alpha_base") or 0.05)
                base_prof = st.get("model_profile_base") or _profile_empty()
                base_prof = _profile_update(
                    base_prof,
                    seq_cnt=seq_cnt,
                    cross_midnight=bool(cross_midnight),
                    end_in_close_window=end_in_close_window,
                    has_late_signature=bool(st.get("has_late_signature")),
                    alpha=alpha_base,
                )
                st["model_profile_base"] = base_prof

                # Seasonal profile (optional)
                if bool((seasonality or {}).get("enabled", True)):
                    alpha_season = float(((model_ctx.get("seasonality_v2") or {}).get("alpha") or {}).get("month") or (seasonality or {}).get("alpha") or (confidence_cfg or {}).get("alpha_season") or 0.12)
                    sk = _season_key(op_date_j, seasonality)
                    smap = st.get("model_profiles_seasonal") or {}
                    if not isinstance(smap, dict):
                        smap = {}
                    sp = smap.get(sk) or _profile_empty()
                    sp = _profile_update(
                        sp,
                        seq_cnt=seq_cnt,
                        cross_midnight=bool(cross_midnight),
                        end_in_close_window=end_in_close_window,
                        has_late_signature=bool(st.get("has_late_signature")),
                        alpha=alpha_season,
                    )
                    smap[sk] = sp
                    st["model_profiles_seasonal"] = smap

                # Recent profile (fast adaptation, used more in peak_mode)
                try:
                    s2 = model_ctx.get("seasonality_v2") or {}
                    if bool(s2.get("enabled", False)):
                        a_cfg = s2.get("alpha") or {}
                        alpha_recent = float(a_cfg.get("recent") or 0.15)
                        rp = st.get("model_profile_recent") or _profile_empty()
                        rp = _profile_update(
                            rp,
                            seq_cnt=seq_cnt,
                            cross_midnight=bool(cross_midnight),
                            end_in_close_window=end_in_close_window,
                            has_late_signature=bool(st.get("has_late_signature")),
                            alpha=alpha_recent,
                        )
                        st["model_profile_recent"] = rp
                except Exception:
                    pass

                # Cluster assignment (v2 if present in state, else lightweight fallback)
                cid = (st.get("model_cluster_id") or "").strip() or _cluster_id_for_employee(base_prof)
                st["model_cluster_id"] = cid

                # Global + cluster profiles (shared, persisted once per run)
                gp = model_ctx.get("global_profile") or _profile_empty()
                gp = _profile_update(
                    gp,
                    seq_cnt=seq_cnt,
                    cross_midnight=bool(cross_midnight),
                    end_in_close_window=end_in_close_window,
                    has_late_signature=bool(st.get("has_late_signature")),
                    alpha=float((confidence_cfg or {}).get("alpha_global") or 0.02),
                )
                model_ctx["global_profile"] = gp
                clusters = model_ctx.get("cluster_profiles") or {}
                if not isinstance(clusters, dict):
                    clusters = {}
                cp = clusters.get(cid) or _profile_empty()
                cp = _profile_update(
                    cp,
                    seq_cnt=seq_cnt,
                    cross_midnight=bool(cross_midnight),
                    end_in_close_window=end_in_close_window,
                    has_late_signature=bool(st.get("has_late_signature")),
                    alpha=float((confidence_cfg or {}).get("alpha_cluster") or 0.04),
                )
                clusters[cid] = cp
                model_ctx["cluster_profiles"] = clusters
            except Exception:
                # Never block jornada closing due to model updates.
                pass

        jornada_row = {
            "jornada_id": jid,
            "employee_id": employee_id,
            "employee_name": st.get("employee_name"),
            "op_date": st.get("current_op_date") or operational_date(final_local, cutoff_hhmm),
            "start_time": st.get("current_start_local") or final_local.replace(microsecond=0).isoformat(),
            "start_time_utc": st.get("current_start_utc") or _to_iso_z(final_utc),
            "end_time": final_local.replace(microsecond=0).isoformat(),
            "end_time_utc": _to_iso_z(final_utc),
            "duration_minutes": dur_min,
            "events_count": int(st.get("seq") or 0),
            "incidencias": incid,
            "closed": 1,
        }
        db.upsert_jornada(jornada_row)

    def clear_current_jornada_state():
        st["current_jornada_id"] = None
        st["current_start_utc"] = None
        st["current_start_local"] = None
        st["current_op_date"] = None
        st["expected_role"] = "IN"
        st["seq"] = 0
        st["incidencias"] = []
        st["has_late_signature"] = False
        st["last_event_utc"] = ""
        st["last_event_local"] = ""

    def _lookahead_count_within(ev_base_utc: datetime, start_idx: int) -> int:
        cnt = 0
        for k in range(start_idx, len(events)):
            evk = events[k]
            evk_utc = _parse_utc(evk.get("event_time_utc") or "")
            if evk_utc is None:
                continue
            delta = (evk_utc - ev_base_utc).total_seconds() / 60.0
            if delta < 0:
                continue
            # Ignore very-close repeats (often tests / double-scan) when deciding ENTRADA vs CIERRE.
            if delta <= debounce_minutes:
                continue
            if delta <= anti_fp_window_minutes:
                cnt += 1
                if cnt >= anti_fp_min_additional_events:
                    return cnt
            else:
                return cnt
        return cnt

    for idx, ev in enumerate(events):
        ev_uid = ev.get("event_uid")
        if not ev_uid:
            continue
        ev_utc = _parse_utc(ev.get("event_time_utc") or "")
        if ev_utc is None:
            continue
        ev_local = ev_utc.astimezone(device_tz)
        employee_name = ev.get("employee_name") or st.get("employee_name") or ""

        if not st.get("current_jornada_id"):
            start_new_jornada(ev_local, ev_utc, employee_name)

        last_utc = _parse_utc(st.get("last_event_utc") or "")
        if last_utc is not None:
            gap_min = (ev_utc - last_utc).total_seconds() / 60.0
            if 0 <= gap_min <= debounce_minutes:
                continue

        last_utc = _parse_utc(st.get("last_event_utc") or "")
        if last_utc is not None:
            gap_min = (ev_utc - last_utc).total_seconds() / 60.0
            if st.get("expected_role") == "IN" and gap_min >= rest_min_minutes:
                last_local = last_utc.astimezone(device_tz)
                close_current_jornada(last_local, last_utc)
                clear_current_jornada_state()
                start_new_jornada(ev_local, ev_utc, employee_name)
            elif st.get("expected_role") == "OUT" and gap_min >= max_shift_hours * 60:
                incid = list(st.get("incidencias") or [])
                if "JORNADA_LARGA_O_ABIERTA" not in incid:
                    incid.append("JORNADA_LARGA_O_ABIERTA")
                st["incidencias"] = incid
                last_local = last_utc.astimezone(device_tz)
                close_current_jornada(last_local, last_utc)
                clear_current_jornada_state()
                start_new_jornada(ev_local, ev_utc, employee_name)


        # Enforce maximum TOTAL jornada duration from first event, even if events keep arriving.
        # This avoids multi-day jornadas when a person has sparse or unbalanced checadas across days.
        start_utc_total = _parse_utc(st.get("current_start_utc") or "")
        if start_utc_total is not None:
            total_min = (ev_utc - start_utc_total).total_seconds() / 60.0
            if total_min >= max_shift_hours * 60:
                incid = list(st.get("incidencias") or [])
                if "JORNADA_LARGA_O_ABIERTA" not in incid:
                    incid.append("JORNADA_LARGA_O_ABIERTA")
                st["incidencias"] = incid

                last_utc_total = _parse_utc(st.get("last_event_utc") or "") or ev_utc
                last_local_total = last_utc_total.astimezone(device_tz)
                close_current_jornada(last_local_total, last_utc_total)
                clear_current_jornada_state()
                start_new_jornada(ev_local, ev_utc, employee_name)

        cur_op = (st.get("current_op_date") or "").strip()
        ev_op = operational_date(ev_local, cutoff_hhmm)
        if cur_op and ev_op == cur_op:
            try:
                op_d = datetime.strptime(cur_op, "%Y-%m-%d").date()
            except Exception:
                op_d = None
            if ev_local.time() >= late_t or (op_d is not None and ev_local.date() > op_d):
                st["has_late_signature"] = True

        
        # --- Operational-date boundary handling ---
        # If the operational date changes, we normally close the previous jornada at the last known event
        # and start a new one. Exception: if hybrid close is enabled and we are expecting an OUT,
        # the first event on the next op_date within the close window can be treated as the closing OUT
        # for the previous jornada (carryback).
        if cur_op and ev_op != cur_op:
            if (
                hybrid_enabled
                and (st.get("expected_role") or "IN") == "OUT"
                and cut_t <= ev_local.time() <= close_end_t
            ):
                treat_close = True
                seq_before = int(st.get("seq") or 0)
                # Manual override (from Excel CORRECCIONES): can force carryback close or force new jornada.
                manual_force_close = False
                manual_force_entry = False
                try:
                    cur_uid = _jornada_uid(employee_id, st.get("current_start_utc") or "")
                    mdec = (db.get_manual_label(cur_uid) or "").strip().upper()
                    if mdec == "FORZAR_ENTRADA":
                        manual_force_entry = True
                        treat_close = False
                    elif mdec == "FORZAR_CIERRE_D1":
                        manual_force_close = True
                        treat_close = True
                except Exception:
                    manual_force_close = False
                    manual_force_entry = False
                allow_no_late = bool(st.get("pattern_allow_cross_without_late")) if patterns_enabled else False
                min_prev = int(st.get("pattern_close_prev_min_events") or 0) if patterns_enabled else 0
                lookahead_cnt = 0

                # Deterministic gates (existing behavior) - skipped if manual force-close/entry.
                if not manual_force_close and not manual_force_entry:
                    if ev_local.time() >= entry_start_t:
                        if require_late_signature and not st.get("has_late_signature") and not allow_no_late:
                            treat_close = False
                        if treat_close and min_prev and seq_before < min_prev:
                            treat_close = False
                        start_utc = _parse_utc(st.get("current_start_utc") or "")
                        if start_utc is not None and (ev_utc - start_utc).total_seconds() > max_join_shift_hours * 3600:
                            treat_close = False
                        if treat_close:
                            lookahead_cnt = _lookahead_count_within(ev_utc, idx + 1)
                            if lookahead_cnt >= anti_fp_min_additional_events:
                                treat_close = False

                # Hard guard even with manual force-close.
                if manual_force_close:
                    start_utc = _parse_utc(st.get("current_start_utc") or "")
                    if start_utc is not None and (ev_utc - start_utc).total_seconds() > max_join_shift_hours * 3600:
                        treat_close = False
                        manual_force_close = False

                # Model confidence layer (optional) to reduce false carrybacks.
                conf = 1.0
                p_prior = 0.5
                reasons: List[str] = []
                if manual_force_close:
                    reasons.append("MANUAL_FORCE_CLOSE")
                if manual_force_entry:
                    reasons.append("MANUAL_FORCE_ENTRY")
                if adv_enabled and bool((confidence_cfg or {}).get("enabled", True)):
                    conf, p_prior, reasons = _compute_carryback_confidence(
                        st=st,
                        model_ctx=model_ctx,
                        op_date=cur_op,
                        ev_local=ev_local,
                        seq_cnt_before=seq_before,
                        has_late_signature=bool(st.get("has_late_signature")),
                        require_late_signature=require_late_signature,
                        allow_no_late=allow_no_late,
                        min_prev_events=(min_prev or 2),
                        lookahead_additional_events=int(lookahead_cnt or 0),
                        entry_start_t=entry_start_t,
                        close_end_t=close_end_t,
                        cut_t=cut_t,
                        seasonality=seasonality,
                        confidence_cfg=confidence_cfg,
                    )
                    thr = float((confidence_cfg or {}).get("threshold") or 0.60)
                    if conf < thr:
                        treat_close = False
                        reasons.append("CONF_BELOW_THR")

                # Persist audit (optional)
                if adv_enabled and bool((audit_cfg or {}).get("enabled", False)):
                    try:
                        db.insert_model_audit(
                            {
                                "employee_id": employee_id,
                                "op_date": cur_op,
                                "event_time_utc": _to_iso_z(ev_utc),
                                "event_time_local": ev_local.replace(microsecond=0).isoformat(),
                                "boundary_from_op_date": cur_op,
                                "boundary_to_op_date": ev_op,
                                "decision": "CARRYBACK_CLOSE" if treat_close else "NEW_JORNADA",
                                "confidence": float(conf),
                                "p_prior": float(p_prior),
                                "reasons": ",".join(reasons[:20]),
                                "features": {
                                    "seq_before": seq_before,
                                    "min_prev": int(min_prev or 0),
                                    "allow_no_late": bool(allow_no_late),
                                    "has_late_signature": bool(st.get("has_late_signature")),
                                    "lookahead_cnt": int(lookahead_cnt or 0),
                                    "entry_start": entry_start_hhmm,
                                    "close_end": close_window_end_hhmm,
                                    "peak_mode": bool((model_ctx.get("peak_by_op_date") or {}).get(cur_op, False)),
                                    "ioi": float((model_ctx.get("ioi_by_op_date") or {}).get(cur_op, 0.0)),
                                },
                            }
                        )
                    except Exception:
                        pass

                if treat_close:
                    if manual_force_close:
                        incid = list(st.get("incidencias") or [])
                        if "MANUAL_FORZAR_CIERRE_D1" not in incid:
                            incid.append("MANUAL_FORZAR_CIERRE_D1")
                        st["incidencias"] = incid
                    role = "OUT"
                    st["seq"] = int(st.get("seq") or 0) + 1
                    seq = int(st["seq"])
                    ensure_parent_jornada(ev_local, ev_utc, employee_name, seq)
                    db.upsert_jornada_event(
                        {
                            "event_uid": ev_uid,
                            "jornada_id": st["current_jornada_id"],
                            "seq": seq,
                            "role": role,
                            "event_time": (ev.get("event_time") or ev_local.replace(microsecond=0).isoformat()),
                            "event_time_utc": _to_iso_z(ev_utc),
                            "employee_id": employee_id,
                        }
                    )
                    st["expected_role"] = "IN"
                    st["last_event_utc"] = _to_iso_z(ev_utc)
                    st["last_event_local"] = ev_local.replace(microsecond=0).isoformat()
                    st["employee_name"] = employee_name
                    close_current_jornada(ev_local, ev_utc)
                    clear_current_jornada_state()
                    indexed += 1
                    continue

            # Not treated as a carryback close: close previous jornada at last event and start new
            last_utc2 = _parse_utc(st.get("last_event_utc") or "")
            if last_utc2 is not None:
                last_local2 = last_utc2.astimezone(device_tz)
                close_current_jornada(last_local2, last_utc2)
            clear_current_jornada_state()
            start_new_jornada(ev_local, ev_utc, employee_name)

        role = st.get("expected_role") or "IN"
        st["seq"] = int(st.get("seq") or 0) + 1
        seq = int(st["seq"])

        last_utc3 = _parse_utc(st.get("last_event_utc") or "")
        if role == "IN" and last_utc3 is not None:
            gap_min = (ev_utc - last_utc3).total_seconds() / 60.0
            if break_max_minutes < gap_min < rest_min_minutes:
                incid = list(st.get("incidencias") or [])
                if "PAUSA_LARGA" not in incid:
                    incid.append("PAUSA_LARGA")
                st["incidencias"] = incid

        st["expected_role"] = "OUT" if role == "IN" else "IN"

        ensure_parent_jornada(ev_local, ev_utc, employee_name, seq)
        db.upsert_jornada_event(
            {
                "event_uid": ev_uid,
                "jornada_id": st["current_jornada_id"],
                "seq": seq,
                "role": role,
                "event_time": (ev.get("event_time") or ev_local.replace(microsecond=0).isoformat()),
                "event_time_utc": _to_iso_z(ev_utc),
                "employee_id": employee_id,
            }
        )

        start_utc = _parse_utc(st.get("current_start_utc") or "")
        dur_min = None
        if start_utc:
            dur_min = int((ev_utc - start_utc).total_seconds() // 60)
        db.upsert_jornada(
            {
                "jornada_id": st["current_jornada_id"],
                "employee_id": employee_id,
                "employee_name": employee_name,
                "op_date": st.get("current_op_date") or operational_date(ev_local, cutoff_hhmm),
                "start_time": st.get("current_start_local") or ev_local.replace(microsecond=0).isoformat(),
                "start_time_utc": st.get("current_start_utc") or _to_iso_z(ev_utc),
                "end_time": ev_local.replace(microsecond=0).isoformat(),
                "end_time_utc": _to_iso_z(ev_utc),
                "duration_minutes": dur_min,
                "events_count": seq,
                "incidencias": st.get("incidencias") or [],
                "closed": 0,
            }
        )

        st["last_event_utc"] = _to_iso_z(ev_utc)
        st["last_event_local"] = ev_local.replace(microsecond=0).isoformat()
        st["employee_name"] = employee_name

        indexed += 1

    _save_state(db, employee_id, st)
    return indexed


def jornadas_to_export_rows(
    jornadas: List[Dict[str, Any]],
    collapse_single_event_blocks: bool = True,
) -> List[Dict[str, Any]]:

    rows: List[Dict[str, Any]] = []
    jornadas_sorted = sorted(
        jornadas,
        key=lambda j: ((j.get("op_date") or ""), (j.get("employee_id") or ""), (j.get("start_time") or "")),
    )
    prev_key: Optional[Tuple[str, str]] = None

    for j in jornadas_sorted:
        emp = j.get("employee_id") or ""
        op_date = j.get("op_date") or ""
        key = (emp, op_date)
        evs = j.get("events") or []

        times: List[str] = []
        for e in evs:
            t = (e.get("event_time") or "").replace(" ", "T")
            try:
                dt = datetime.fromisoformat(t)
                times.append(dt.strftime("%H:%M"))
            except Exception:
                times.append(t[11:16] if len(t) >= 16 else t)

        if collapse_single_event_blocks and len(times) == 1 and rows and prev_key == key:
            prev = rows[-1]
            for i in range(1, 13):
                col = f"E{i:02d}"
                if not prev.get(col):
                    prev[col] = times[0]
                    break
            if j.get("end_time"):
                prev["end_local"] = _fmt_local(j.get("end_time"))
            codes = set((prev.get("incidencia_codes") or "").split(",") if prev.get("incidencia_codes") else [])
            codes.discard("")
            codes.add("EVENTO_SUELTO_REANEXADO")
            prev["incidencia_codes"] = ",".join(sorted(codes))
            detail = prev.get("incidencia_detail") or ""
            add = "Evento suelto reanexado al bloque anterior (mismo día operativo)."
            prev["incidencia_detail"] = (detail + " | " + add).strip(" |")
            continue

        row: Dict[str, Any] = {
            "fecha_registro": op_date,
            "employee_id": emp,
            "employee_name": j.get("employee_name") or "",
            "start_local": _fmt_local(j.get("start_time") or ""),
            "end_local": _fmt_local(j.get("end_time") or ""),
            "duration_minutes": ("" if j.get("duration_minutes") is None else j.get("duration_minutes")),
            "incidencia_codes": ",".join(j.get("incidencias") or []),
            "incidencia_detail": "",
        }
        # Hidden technical identifiers (for manual corrections import)
        try:
            row["jornada_uid"] = _jornada_uid(emp, j.get("start_time_utc") or "")
        except Exception:
            row["jornada_uid"] = ""
        row["jornada_id"] = j.get("jornada_id") or ""
        row["start_time_utc"] = j.get("start_time_utc") or ""
        for i in range(1, 13):
            row[f"E{i:02d}"] = times[i - 1] if i - 1 < len(times) else ""
        rows.append(row)
        prev_key = key

    return rows


def _fmt_local(s: str) -> str:
    if not s:
        return ""
    s2 = s.replace(" ", "T")
    try:
        dt = datetime.fromisoformat(s2)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return s
