from fastapi import APIRouter, Query
from pathlib import Path
import os
import sqlite3

router = APIRouter(prefix="/api/market2")

def _candidate_paths():
    here = Path(__file__).resolve().parent
    root = here.parent
    paths = []
    env_path = os.environ.get("MARKET_DB_PATH")
    if env_path:
        paths.append(Path(env_path))
    paths.extend([
        here / "market_automation.db",
        root / "market_automation.db",
        Path.cwd() / "market_automation.db",
        Path.cwd() / "app" / "market_automation.db",
    ])

    seen = set()
    out = []
    for p in paths:
        sp = str(p)
        if sp not in seen:
            seen.add(sp)
            out.append(p)
    return out

def _table_count(conn, table):
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    except Exception:
        return 0

def _pick_db_path():
    existing = [p for p in _candidate_paths() if p.exists() and p.stat().st_size > 0]
    best = None
    best_score = -1

    for p in existing:
        try:
            conn = sqlite3.connect(str(p))
            score = (
                _table_count(conn, "market_report_rows")
                + _table_count(conn, "current_policy_state")
                + _table_count(conn, "market_events")
                + _table_count(conn, "policy_event_rows")
            )
            conn.close()
            if score > best_score:
                best = p
                best_score = score
        except Exception:
            pass

    if best:
        return best
    if existing:
        return existing[0]
    return _candidate_paths()[0]

def _connect():
    path = _pick_db_path()
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn, path

def _cols(conn, table):
    try:
        return [r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    except Exception:
        return []

def _has_table(conn, table):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,)
    ).fetchone()
    return row is not None

def _num(v):
    try:
        if v is None or v == "":
            return None
        return int(float(v))
    except Exception:
        return None

def _pick(cols, names):
    for n in names:
        if n in cols:
            return n
    return None

@router.get("/health")
def market2_health():
    conn, path = _connect()
    try:
        tables = [
            "raw_telegram_messages",
            "market_event_bundles",
            "market_events",
            "policy_event_rows",
            "market_report_rows",
            "current_policy_state",
            "compliance_notices",
            "field_feedback",
        ]
        counts = {t: _table_count(conn, t) for t in tables}
        return {
            "db_path": str(path),
            "db_exists": path.exists(),
            "db_size": path.stat().st_size if path.exists() else 0,
            "counts": counts,
        }
    finally:
        conn.close()

@router.get("/reports")
def market2_reports(limit: int = Query(300, ge=1, le=1000)):
    conn, path = _connect()
    try:
        if not _has_table(conn, "market_report_rows"):
            return {"items": []}

        rows = conn.execute("""
            SELECT
                report_date,
                carrier,
                agency_name,
                model_name,
                price_010,
                price_mnp,
                price_change,
                notes
            FROM market_report_rows
            ORDER BY report_date DESC, id DESC
            LIMIT ?
        """, (limit,)).fetchall()

        return {"items": [dict(r) for r in rows]}
    except Exception as e:
        return {"items": [], "error": str(e), "db_path": str(path)}
    finally:
        conn.close()

@router.get("/rebate-status")
def market2_rebate_status():
    conn, path = _connect()
    try:
        if not _has_table(conn, "current_policy_state"):
            return {"items": []}

        cols = _cols(conn, "current_policy_state")
        last_col = _pick(cols, ["last_updated_at", "updated_at", "source_time", "created_at"])
        order_col = last_col or "id"

        rows = conn.execute(f"""
            SELECT
                carrier,
                model_group,
                sales_type,
                contract_type,
                plan_band,
                current_delta_krw,
                {last_col if last_col else "''"} AS last_updated_at
            FROM current_policy_state
            ORDER BY model_group, carrier, {order_col} DESC
        """).fetchall()

        return {"items": [dict(r) for r in rows]}
    except Exception as e:
        return {"items": [], "error": str(e), "db_path": str(path)}
    finally:
        conn.close()

@router.get("/competition")
def market2_competition():
    conn, path = _connect()
    try:
        if not _has_table(conn, "current_policy_state"):
            return {"items": []}

        rows = conn.execute("""
            SELECT carrier, model_group, current_delta_krw
            FROM current_policy_state
            WHERE model_group IS NOT NULL
              AND model_group != ''
              AND model_group != 'UNKNOWN'
        """).fetchall()

        by_model = {}

        for r in rows:
            model = r["model_group"]
            carrier = (r["carrier"] or "").upper()
            delta = _num(r["current_delta_krw"])

            if not model or delta is None:
                continue

            if model not in by_model:
                by_model[model] = {"model_group": model, "kt_delta": None, "skt_delta": None, "lgu_delta": None}

            if carrier == "KT":
                key = "kt_delta"
            elif carrier == "SKT":
                key = "skt_delta"
            elif carrier.startswith("LG"):
                key = "lgu_delta"
            else:
                continue

            old = by_model[model][key]
            if old is None or delta > old:
                by_model[model][key] = delta

        items = []
        for model in sorted(by_model):
            x = by_model[model]
            kt_base = x["kt_delta"] if x["kt_delta"] is not None else 0
            skt = x["skt_delta"]
            lgu = x["lgu_delta"]
            x["skt_vs_kt_gap"] = (skt - kt_base) if skt is not None else 0
            x["lgu_vs_kt_gap"] = (lgu - kt_base) if lgu is not None else 0
            items.append(x)

        return {"items": items}
    except Exception as e:
        return {"items": [], "error": str(e), "db_path": str(path)}
    finally:
        conn.close()

@router.get("/timeline")
def market2_timeline(limit: int = Query(100, ge=1, le=500)):
    conn, path = _connect()
    try:
        if not _has_table(conn, "market_events"):
            return {"items": []}

        cols = _cols(conn, "market_events")
        time_col = _pick(cols, ["source_time", "event_time", "created_at", "time", "message_time"])
        type_col = _pick(cols, ["event_type", "type"])
        carrier_col = _pick(cols, ["carrier"])
        summary_col = _pick(cols, ["summary", "title", "notes", "raw_text", "source_text", "message_text", "text"])

        select_parts = [
            f"{time_col} AS time" if time_col else "'' AS time",
            f"{type_col} AS event_type" if type_col else "'' AS event_type",
            f"{carrier_col} AS carrier" if carrier_col else "'' AS carrier",
            f"{summary_col} AS summary" if summary_col else "'' AS summary",
        ]

        order_expr = time_col if time_col else "id"
        sql = f"""
            SELECT {", ".join(select_parts)}
            FROM market_events
            ORDER BY {order_expr} DESC
            LIMIT ?
        """

        rows = conn.execute(sql, (limit,)).fetchall()
        return {"items": [dict(r) for r in rows]}
    except Exception as e:
        return {"items": [], "error": str(e), "db_path": str(path)}
    finally:
        conn.close()
