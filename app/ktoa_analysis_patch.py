from fastapi import APIRouter
from datetime import datetime, date
import calendar

router = APIRouter(prefix="/api/ktoa2", tags=["ktoa2"])


def get_cache():
    import sys
    m = sys.modules.get("app.main") or sys.modules.get("main")
    return getattr(m, "_ktoa_cache", [])


def working_days(year, month):
    total = calendar.monthrange(year, month)[1]
    wd = 0
    for d in range(1, total + 1):
        if date(year, month, d).weekday() < 5:
            wd += 1
    return wd


def working_days_until_today():
    today = date.today()
    wd = 0
    for d in range(1, today.day + 1):
        if date(today.year, today.month, d).weekday() < 5:
            wd += 1
    return wd


@router.get("/summary")
def summary():

    rows = get_cache()
    if not rows:
        return {"error": "no data"}

    last = rows[-1]

    today = datetime.today()
    total_wd = working_days(today.year, today.month)
    passed_wd = working_days_until_today()

    def proj(v):
        if passed_wd == 0:
            return v
        return int(v * total_wd / passed_wd)

    result = {
        "date": last["date"],

        "KT": {
            "MNO": last.get("KT_순증MNO", 0),
            "MVNO": last.get("KT_순증MVNO", 0),
            "projection": proj(last.get("KT_순증전체", 0))
        },

        "SKT": {
            "MNO": last.get("SKT_순증MNO", 0),
            "MVNO": last.get("SKT_순증MVNO", 0),
            "projection": proj(last.get("SKT_순증전체", 0))
        },

        "LGU": {
            "MNO": last.get("LGU+_순증MNO", 0),
            "MVNO": last.get("LGU+_순증MVNO", 0),
            "projection": proj(last.get("LGU+_순증전체", 0))
        }
    }

    return result


@router.get("/trend")
def trend():
    rows = get_cache()

    data = []
    for r in rows:
        data.append({
            "date": r["date"],
            "KT": r.get("KT_순증전체", 0),
            "SKT": r.get("SKT_순증전체", 0),
            "LGU": r.get("LGU+_순증전체", 0)
        })

    return {"items": data}