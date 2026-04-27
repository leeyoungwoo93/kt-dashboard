from fastapi import APIRouter, Query
from pathlib import Path
import os
import sqlite3
from typing import Any, Dict, List, Optional

router = APIRouter(prefix="/api/market2", tags=["market2"])

TABLES = [
    "raw_telegram_messages",
    "market_event_bundles",
    "market_events",
    "policy_event_rows",
    "market_report_rows",
    "current_policy_state",
    "compliance_notices",
    "field_feedback",
]


def _candidate_paths() -> List[Path]:
    here = Path(__file__).resolve().parent
    root = here.parent
    cwd = Path.cwd()
    paths: List[Path] = []

    env_path = os.environ.get("MARKET_AUTOMATION_DB")
    if env_path:
        paths.append(Path(env_path))

    paths.extend([
        here / "market_automation.db",
        root / "market_automation.db",
        cwd / "app" / "market_automation.db",
        cwd / "market_automation.db",
    ])

    # de-duplicate while preserving order
    seen = set()
    unique: List[Path] = []
    for p in paths:
        key = str(p)
        if key not in seen:
            seen.add(key)
            unique.append(p)
    return unique


def get_db_path() -> Path:
    for p in _candidate_paths():
        try:
            if p.exists() and p.stat().st_size > 0:
                return p
        except OSError:
            continue
    return _candidate_paths()[0]


def connect() -> sqlite3.Connection:
    db_path = get_db_path()
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def table_count(conn: sqlite3.Connection, table_name: str) -> int:
    if not table_exists(conn, table_name):
        return 0
    try:
        return int(conn.execute(f"SELECT COUNT(*) AS cnt FROM {table_name}").fetchone()["cnt"] or 0)
    except sqlite3.Error:
        return 0


def columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    if not table_exists(conn, table_name):
        return []
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table_name})").fetchall()]


def as_items(rows: List[sqlite3.Row]) -> Dict[str, Any]:
    return {"items": [dict(r) for r in rows]}


def safe_int(v: Optional[Any]) -> int:
    if v is None or v == "":
        return 0
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return 0


@router.get("/health")
def market2_health() -> Dict[str, Any]:
    db_path = get_db_path()
    exists = db_path.exists()
    size = db_path.stat().st_size if exists else 0

    counts: Dict[str, int] = {}
    schemas: Dict[str, List[str]] = {}
    if exists and size > 0:
        with connect() as conn:
            counts = {t: table_count(conn, t) for t in TABLES}
            schemas = {t: columns(conn, t) for t in TABLES if table_exists(conn, t)}

    return {
        "ok": exists and size > 0,
        "db_path": str(db_path),
        "db_exists": exists,
        "db_size": size,
        "candidate_paths": [str(p) for p in _candidate_paths()],
        "counts": counts,
        "schemas": schemas,
    }


@router.get("/reports")
def market2_reports(limit: int = Query(300, ge=1, le=1000)) -> Dict[str, Any]:
    with connect() as conn:
        if not table_exists(conn, "market_report_rows"):
            return {"items": []}
        rows = conn.execute(
            """
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
            ORDER BY datetime(report_date) DESC, rowid DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return as_items(rows)


@router.get("/rebate-status")
def market2_rebate_status(limit: int = Query(300, ge=1, le=1000)) -> Dict[str, Any]:
    with connect() as conn:
        if not table_exists(conn, "current_policy_state"):
            return {"items": []}
        rows = conn.execute(
            """
            SELECT
                carrier,
                model_group,
                sales_type,
                contract_type,
                plan_band,
                current_delta_krw,
                last_updated_at
            FROM current_policy_state
            ORDER BY datetime(last_updated_at) DESC, rowid DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return as_items(rows)


@router.get("/competition")
def market2_competition() -> Dict[str, Any]:
    with connect() as conn:
        if not table_exists(conn, "current_policy_state"):
            return {"items": []}
        rows = conn.execute(
            """
            SELECT
                model_group,
                MAX(CASE WHEN UPPER(carrier) = 'KT' THEN current_delta_krw END) AS kt_delta,
                MAX(CASE WHEN UPPER(carrier) = 'SKT' THEN current_delta_krw END) AS skt_delta,
                MAX(CASE WHEN UPPER(carrier) IN ('LGU', 'LGU+', 'LGT') THEN current_delta_krw END) AS lgu_delta,
                MAX(last_updated_at) AS last_updated_at
            FROM current_policy_state
            WHERE model_group IS NOT NULL
              AND TRIM(model_group) <> ''
              AND TRIM(model_group) <> 'UNKNOWN'
            GROUP BY model_group
            ORDER BY datetime(last_updated_at) DESC, model_group
            """
        ).fetchall()

    items: List[Dict[str, Any]] = []
    for r in rows:
        kt = r["kt_delta"]
        skt = r["skt_delta"]
        lgu = r["lgu_delta"]
        kt_base = safe_int(kt)
        skt_gap = safe_int(skt) - kt_base
        lgu_gap = safe_int(lgu) - kt_base
        items.append(
            {
                "model_group": r["model_group"],
                "kt_delta": kt,
                "skt_delta": skt,
                "lgu_delta": lgu,
                "skt_vs_kt_gap": skt_gap,
                "lgu_vs_kt_gap": lgu_gap,
                "last_updated_at": r["last_updated_at"],
            }
        )

    items.sort(key=lambda x: max(abs(safe_int(x.get("skt_vs_kt_gap"))), abs(safe_int(x.get("lgu_vs_kt_gap")))), reverse=True)
    return {"items": items}


@router.get("/timeline")
def market2_timeline(limit: int = Query(120, ge=1, le=1000)) -> Dict[str, Any]:
    with connect() as conn:
        if not table_exists(conn, "market_events"):
            return {"items": []}
        rows = conn.execute(
            """
            SELECT
                source_time AS time,
                source_time,
                event_type,
                carrier,
                summary
            FROM market_events
            ORDER BY datetime(source_time) DESC, rowid DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return as_items(rows)


@router.get("/summary")
def market2_summary() -> Dict[str, Any]:
    with connect() as conn:
        counts = {t: table_count(conn, t) for t in TABLES}
        latest_event = None
        if table_exists(conn, "market_events"):
            latest_event = conn.execute(
                "SELECT source_time, event_type, carrier, summary FROM market_events ORDER BY datetime(source_time) DESC, rowid DESC LIMIT 1"
            ).fetchone()
        latest_report = None
        if table_exists(conn, "market_report_rows"):
            latest_report = conn.execute(
                "SELECT report_date, carrier, agency_name, model_name FROM market_report_rows ORDER BY datetime(report_date) DESC, rowid DESC LIMIT 1"
            ).fetchone()
    return {
        "counts": counts,
        "latest_event": dict(latest_event) if latest_event else None,
        "latest_report": dict(latest_report) if latest_report else None,
    }
