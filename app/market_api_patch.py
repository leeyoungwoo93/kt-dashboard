from fastapi import APIRouter
from pathlib import Path
import os
import re
import sqlite3
import traceback

router = APIRouter(prefix="/api/market2", tags=["market2"])


def _db_candidates():
    here = Path(__file__).resolve().parent
    root = here.parent

    candidates = []
    env_db = os.getenv("MARKET_AUTOMATION_DB")
    if env_db:
        candidates.append(Path(env_db))

    candidates.extend([
        here / "market_automation.db",
        root / "market_automation.db",
        Path.cwd() / "market_automation.db",
        Path.cwd() / "app" / "market_automation.db",
    ])

    seen = set()
    out = []
    for p in candidates:
        key = str(p.resolve()) if p.exists() else str(p)
        if key not in seen:
            seen.add(key)
            out.append(p)
    return out


def db_path():
    for p in _db_candidates():
        try:
            if p.exists() and p.stat().st_size > 0:
                return p
        except Exception:
            pass
    return _db_candidates()[0]


def conn():
    p = db_path()
    c = sqlite3.connect(str(p))
    c.row_factory = sqlite3.Row
    return c


def table_exists(c, table):
    row = c.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def columns(c, table):
    if not table_exists(c, table):
        return []
    return [r["name"] for r in c.execute(f"PRAGMA table_info({table})").fetchall()]


def first_existing(cols, names, default=None):
    for n in names:
        if n in cols:
            return n
    return default


def safe_int(v):
    try:
        if v is None or v == "":
            return None
        return int(v)
    except Exception:
        return None


def pick(d, names, default=""):
    for n in names:
        v = d.get(n)
        if v is not None and v != "":
            return v
    return default


def normalize_carrier(v):
    s = str(v or "").strip().upper().replace(" ", "")
    if s in ("S", "SK", "SKT"):
        return "SKT"
    if s in ("K", "KT"):
        return "KT"
    if s in ("L", "LG", "LGU", "LGU+", "LGUPLUS", "LG유플러스"):
        return "LGU"
    return str(v or "UNKNOWN").strip() or "UNKNOWN"


def detect_carrier(text):
    t = str(text or "").upper()
    if "SKT" in t or " SK " in t or t.startswith("SK "):
        return "SKT"
    if "KT" in t:
        return "KT"
    if "LGU" in t or "LGU+" in t or "엘지" in t or "유플" in t:
        return "LGU"
    return ""


def classify_market_event(text, event_type=""):
    raw = str(text or "")
    t = raw.lower()
    et = str(event_type or "")
    joined = f"{et} {raw}".lower()

    rules = [
        ("공시지원금", ["공시", "공시지원", "지원금 상향", "지원금 하향"]),
        ("선택약정", ["선약", "선택약정", "요금할인"]),
        ("추가지원금", ["추지", "추가 지원", "추가지원", "추가보조", "추보"]),
        ("수수료변경", ["수수료", "리베이트", "rebate", "장려금", "정책금"]),
        ("가격인상", ["가격 인상", "출고가 인상", "상향", "인상"]),
        ("가격인하", ["가격 인하", "출고가 인하", "하향", "인하"]),
        ("신제품출시", ["출시", "사전예약", "런칭", "신모델", "신제품"]),
        ("판매중단", ["판매중단", "판매 중단", "단종", "종료", "마감"]),
        ("정책변경", ["정책", "변경", "개편", "조건", "적용"]),
    ]
    cls = "기타"
    for name, keys in rules:
        if any(k.lower() in joined for k in keys):
            cls = name
            break

    devices = [
        ("갤럭시S26류", [r"갤럭시\s*s?26", r"\bs26\b"]),
        ("갤럭시S25류", [r"갤럭시\s*s?25", r"\bs25\b"]),
        ("갤럭시S24류", [r"갤럭시\s*s?24", r"\bs24\b"]),
        ("Z Fold류", [r"z\s*fold", r"폴드"]),
        ("Z Flip류", [r"z\s*flip", r"플립"]),
        ("iPhone17류", [r"iphone\s*17", r"아이폰\s*17"]),
        ("iPhone16류", [r"iphone\s*16", r"아이폰\s*16"]),
        ("iPhone15류", [r"iphone\s*15", r"아이폰\s*15"]),
        ("A시리즈", [r"갤럭시\s*a\d+", r"\ba\d{2}\b"]),
    ]
    device_group = ""
    for name, patterns in devices:
        if any(re.search(p, joined, re.I) for p in patterns):
            device_group = name
            break

    amount = None
    amount_match = re.search(r"([+-]?\d+(?:,\d{3})*(?:\.\d+)?)\s*(만원|만|원)", raw)
    if amount_match:
        n = float(amount_match.group(1).replace(",", ""))
        amount = int(n * 10000) if amount_match.group(2) in ("만원", "만") else int(n)

    high_keys = ["긴급", "즉시", "금일", "오늘", "당일", "마감", "중단"]
    med_keys = ["내일", "익일", "이번주", "변경", "인상", "인하", "상향", "하향"]
    urgency = "HIGH" if any(k in raw for k in high_keys) else "MEDIUM" if any(k in raw for k in med_keys) else "NORMAL"

    return {
        "event_cls": cls,
        "device_group": device_group,
        "amount": amount,
        "urgency": urgency,
    }


def safe_error(e):
    return {
        "items": [],
        "error": str(e),
        "trace": traceback.format_exc(limit=3),
        "db_path": str(db_path()),
    }


@router.get("/health")
def health():
    p = db_path()
    result = {
        "ok": p.exists() and p.stat().st_size > 0 if p.exists() else False,
        "db_path": str(p),
        "db_size": p.stat().st_size if p.exists() else 0,
        "counts": {},
    }

    try:
        with conn() as c:
            for t in [
                "raw_telegram_messages",
                "market_event_bundles",
                "market_events",
                "policy_event_rows",
                "market_report_rows",
                "current_policy_state",
                "compliance_notices",
                "field_feedback",
            ]:
                if table_exists(c, t):
                    result["counts"][t] = c.execute(f"SELECT COUNT(*) AS n FROM {t}").fetchone()["n"]
                else:
                    result["counts"][t] = None
    except Exception as e:
        result["ok"] = False
        result["error"] = str(e)

    return result


@router.get("/reports")
def reports(limit: int = 300):
    try:
        with conn() as c:
            if not table_exists(c, "market_report_rows"):
                return {"items": [], "db_path": str(db_path()), "error": "market_report_rows table not found"}

            cols = columns(c, "market_report_rows")
            order_col = first_existing(cols, ["report_date", "source_time", "created_at", "id"], "rowid")

            rows = c.execute(
                f"SELECT * FROM market_report_rows ORDER BY {order_col} DESC LIMIT ?",
                (limit,),
            ).fetchall()

            items = []
            for r in rows:
                d = dict(r)
                items.append({
                    "report_date": pick(d, ["report_date", "source_time", "created_at"]),
                    "carrier": normalize_carrier(pick(d, ["carrier", "company", "telco"])),
                    "agency_name": pick(d, ["agency_name", "agency", "dealer_name"]),
                    "model_name": pick(d, ["model_name", "model_group", "model"]),
                    "price_010": safe_int(pick(d, ["price_010", "p010", "new_price"], None)),
                    "price_mnp": safe_int(pick(d, ["price_mnp", "mnp_price"], None)),
                    "price_change": safe_int(pick(d, ["price_change", "change_price"], None)),
                    "notes": pick(d, ["notes", "raw_text", "source_text", "text"]),
                })

            return {"items": items, "count": len(items), "db_path": str(db_path())}

    except Exception as e:
        return safe_error(e)


@router.get("/rebate-status")
def rebate_status(limit: int = 500):
    try:
        with conn() as c:
            if not table_exists(c, "current_policy_state"):
                return {"items": [], "db_path": str(db_path()), "error": "current_policy_state table not found"}

            cols = columns(c, "current_policy_state")
            order_col = first_existing(cols, ["last_updated_at", "updated_at", "created_at", "id"], "rowid")

            rows = c.execute(
                f"SELECT * FROM current_policy_state ORDER BY {order_col} DESC LIMIT ?",
                (limit,),
            ).fetchall()

            items = []
            for r in rows:
                d = dict(r)
                items.append({
                    "carrier": normalize_carrier(pick(d, ["carrier"])),
                    "model_group": pick(d, ["model_group", "model_name", "model"], "UNKNOWN"),
                    "sales_type": pick(d, ["sales_type"]),
                    "contract_type": pick(d, ["contract_type"]),
                    "plan_band": pick(d, ["plan_band"]),
                    "current_delta_krw": safe_int(pick(d, ["current_delta_krw", "delta_krw", "amount"], 0)) or 0,
                    "last_updated_at": pick(d, ["last_updated_at", "updated_at", "created_at"]),
                })

            return {"items": items, "count": len(items), "db_path": str(db_path())}

    except Exception as e:
        return safe_error(e)


@router.get("/competition")
def competition(limit: int = 300):
    try:
        status = rebate_status(limit=2000)
        rows = status.get("items", [])

        grouped = {}
        for r in rows:
            model = r.get("model_group") or "UNKNOWN"
            carrier = normalize_carrier(r.get("carrier"))
            if carrier not in ("KT", "SKT", "LGU"):
                continue

            if model not in grouped:
                grouped[model] = {"model_group": model, "kt_delta": None, "skt_delta": None, "lgu_delta": None}

            key = {
                "KT": "kt_delta",
                "SKT": "skt_delta",
                "LGU": "lgu_delta",
            }[carrier]

            if grouped[model][key] is None:
                grouped[model][key] = r.get("current_delta_krw")

        items = []
        for model, g in grouped.items():
            kt = g["kt_delta"] or 0
            skt = g["skt_delta"] or 0
            lgu = g["lgu_delta"] or 0

            g["skt_vs_kt_gap"] = skt - kt
            g["lgu_vs_kt_gap"] = lgu - kt
            items.append(g)

        items.sort(key=lambda x: abs(x.get("skt_vs_kt_gap", 0)) + abs(x.get("lgu_vs_kt_gap", 0)), reverse=True)

        return {"items": items[:limit], "count": len(items[:limit]), "db_path": str(db_path())}

    except Exception as e:
        return safe_error(e)


@router.get("/timeline")
def timeline(limit: int = 120):
    try:
        with conn() as c:
            if not table_exists(c, "market_events"):
                return {"items": [], "db_path": str(db_path()), "error": "market_events table not found"}

            cols = columns(c, "market_events")
            order_col = first_existing(cols, ["source_time", "event_time", "created_at", "id"], "rowid")

            rows = c.execute(
                f"SELECT * FROM market_events ORDER BY {order_col} DESC LIMIT ?",
                (limit,),
            ).fetchall()

            items = []
            for r in rows:
                d = dict(r)

                raw = pick(d, ["summary", "notes", "raw_text", "source_text", "text", "content"], "")
                event_type = pick(d, ["event_type", "type"], "")
                carrier = normalize_carrier(pick(d, ["carrier"], "")) if pick(d, ["carrier"], "") else detect_carrier(raw)

                summary = raw
                if not summary:
                    summary = " / ".join([
                        str(x) for x in [
                            event_type,
                            pick(d, ["model_group", "model_name", "model"], ""),
                            pick(d, ["sales_type"], ""),
                        ] if x
                    ])

                cls = classify_market_event(summary or raw, event_type)
                model_group = pick(d, ["model_group", "model_name", "model"], "") or cls["device_group"]
                delta_krw = safe_int(pick(d, ["delta_krw", "current_delta_krw", "amount"], None))
                if delta_krw is None:
                    delta_krw = cls["amount"]
                ts = pick(d, ["source_time", "event_time", "created_at"])

                items.append({
                    "id": pick(d, ["id", "event_id"]),
                    "event_id": pick(d, ["id", "event_id"]),
                    "time": ts,
                    "source_time": ts,
                    "event_time": ts,
                    "event_type": event_type,
                    "type": event_type,
                    "event_cls": cls["event_cls"],
                    "urgency": cls["urgency"],
                    "carrier": carrier,
                    "model_group": model_group,
                    "device_group": model_group,
                    "sales_type": pick(d, ["sales_type"], ""),
                    "delta_krw": delta_krw,
                    "amount": delta_krw,
                    "summary": summary,
                    "raw_text": raw,
                })

            return {"items": items, "count": len(items), "db_path": str(db_path())}

    except Exception as e:
        return safe_error(e)
