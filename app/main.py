import os, io
from fastapi import FastAPI, UploadFile, File, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from typing import List
import pandas as pd
from sqlalchemy import func, text
from app.database import engine, Base, SessionLocal
from app.models.sales import Sales, Commission, DeviceSales, Inventory, Subscriber

EXCLUDE_CH = ("3-1.", "3-2.", "8-2.")
MIN_BONBU = 100
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

def safe_int(v):
    try: return int(v) if pd.notna(v) else 0
    except: return 0

def safe_float(v):
    try: return float(v) if pd.notna(v) else 0.0
    except: return 0.0

def _load_sales(db, contents):
    df = pd.read_excel(io.BytesIO(contents), skiprows=2, header=None)
    db.query(Sales).delete()
    for _, row in df.iterrows():
        val_bonbu = str(row[3]) if pd.notna(row[3]) else ""
        if val_bonbu in ("", "nan") or val_bonbu.lstrip("-").isdigit(): continue
        db.add(Sales(
            boomun=str(row[1]) if pd.notna(row[1]) else "",
            bonbu=val_bonbu,
            team=str(row[5]) if pd.notna(row[5]) else "",
            dept=str(row[7]) if pd.notna(row[7]) else "",
            agency_code=str(row[8]) if pd.notna(row[8]) else "",
            agency_org=str(row[9]) if pd.notna(row[9]) else "",
            agency=str(row[11]) if pd.notna(row[11]) else "",
            channel1=str(row[12]) if pd.notna(row[12]) else "",
            channel2=str(row[13]) if pd.notna(row[13]) else "",
            channel3=str(row[14]) if pd.notna(row[14]) else "",
            channel_sub=str(row[19]) if pd.notna(row[19]) else "",
            sale_type=str(row[15]) if pd.notna(row[15]) else "",
            kids=str(row[16]) if pd.notna(row[16]) else "",
            foreigner=str(row[17]) if pd.notna(row[17]) else "",
            k110=str(row[18]) if pd.notna(row[18]) else "",
            sale_count=safe_int(row[21]), net_add=safe_int(row[22]),
            new_sub=safe_int(row[23]), mnp=safe_int(row[25]),
            smnp=safe_int(row[26]), lmnp=safe_int(row[27]),
            mmnp=safe_int(row[28]), vmnp=safe_int(row[29]),
            churn=safe_int(row[30]), mnp_churn=safe_int(row[32]),
            smnp_churn=safe_int(row[33]), lmnp_churn=safe_int(row[34]),
            mmnp_churn=safe_int(row[35]), vmnp_churn=safe_int(row[36]),
            forced_churn=safe_int(row[37]), premium_change=safe_int(row[38]),
            arpu=safe_float(row[39]), revenue=safe_float(row[40]),
            subscriber=safe_int(row[41]),
        ))
    db.commit()

def _load_commission(db, contents):
    df = pd.read_excel(io.BytesIO(contents), skiprows=1, header=1)
    db.query(Commission).delete()
    for _, row in df.iterrows():
        agency_code = str(row.get("수수료지급발생조직", "")) if pd.notna(row.get("수수료지급발생조직")) else ""
        if agency_code in ("", "nan"): continue
        db.add(Commission(
            jisa_code=str(row.iloc[0]) if pd.notna(row.iloc[0]) else "",
            jisa_name=str(row.iloc[1]) if pd.notna(row.iloc[1]) else "",
            team_code=str(row.iloc[2]) if pd.notna(row.iloc[2]) else "",
            team_name=str(row.iloc[3]) if pd.notna(row.iloc[3]) else "",
            agency_code=agency_code,
            agency_name=str(row.iloc[5]) if pd.notna(row.iloc[5]) else "",
            channel_type=str(row.get("판매접점이동전화채널유형", "")) if pd.notna(row.get("판매접점이동전화채널유형")) else "",
            channel_path=str(row.get("판매접점이동전화판매경로", "")) if pd.notna(row.get("판매접점이동전화판매경로")) else "",
            channel_sale=str(row.get("판매접점이동전화판매유형", "")) if pd.notna(row.get("판매접점이동전화판매유형")) else "",
            sale_policy=str(row.get("판매정책", "")) if pd.notna(row.get("판매정책")) else "",
            commission_policy=str(row.get("수수료정책", "")) if pd.notna(row.get("수수료정책")) else "",
            model_code=str(row.get("단말기모델", "")) if pd.notna(row.get("단말기모델")) else "",
            device_model=str(row.iloc[16]) if pd.notna(row.iloc[16]) else "",
            product=str(row.get("기본상품", "")) if pd.notna(row.get("기본상품")) else "",
            contract=str(row.get("개통서비스계약", "")) if pd.notna(row.get("개통서비스계약")) else "",
            dept_owner=str(row.get("수수료정책주관부서", "")) if pd.notna(row.get("수수료정책주관부서")) else "",
            item_code=str(row.get("수수료항목", "")) if pd.notna(row.get("수수료항목")) else "",
            refund_month=str(row.get("환수년월", "")) if pd.notna(row.get("환수년월")) else "",
            pay_type=str(row.get("수수료지급환수구분", "")) if pd.notna(row.get("수수료지급환수구분")) else "",
            amount=safe_float(row.get("수수료최종지급액", 0)),
        ))
    db.commit()

def _load_device(db, contents):
    df = pd.read_excel(io.BytesIO(contents), skiprows=2, header=None)
    db.query(DeviceSales).delete()
    for i, row in df.iterrows():
        if i == 0: continue
        val_bonbu = str(row[1]) if pd.notna(row[1]) else ""
        if val_bonbu in ("", "nan") or val_bonbu.lstrip("-").isdigit(): continue
        model_name = str(row[7]) if pd.notna(row[7]) else ""
        if model_name in ("", "nan", "ㆍ값없음"): model_name = str(row[6]) if pd.notna(row[6]) else ""
        for yyyymm, cs, cr in [("202604", 9, 10), ("202603", 11, 12)]:
            db.add(DeviceSales(
                bonbu=val_bonbu, team=str(row[3]) if pd.notna(row[3]) else "",
                agency_code=str(row[4]) if pd.notna(row[4]) else "",
                agency=str(row[5]) if pd.notna(row[5]) else "",
                model_code=str(row[6]) if pd.notna(row[6]) else "",
                model_name=model_name, yyyymm=yyyymm,
                sale_count=safe_int(row[cs]), revenue=safe_float(row[cr])
            ))
    db.commit()

def _load_inventory(db, contents):
    df = pd.read_excel(io.BytesIO(contents), skiprows=1, header=1)
    db.query(Inventory).delete()
    for _, row in df.iterrows():
        model = str(row.get("단말기모델", "")) if pd.notna(row.get("단말기모델")) else ""
        if model in ("", "nan", "합계"): continue
        ref_date = str(row.get("일자", ""))[:10]
        db.add(Inventory(
            ref_date=ref_date, model_name=model,
            total=safe_int(row.iloc[3]), jisa=safe_int(row.iloc[4]),
            youngi=safe_int(row.iloc[5]), strategy=safe_int(row.iloc[6]),
            mns=safe_int(row.iloc[7]), ktshop=safe_int(row.iloc[8]),
            etc=safe_int(row.iloc[9])
        ))
    db.commit()

def _load_subscriber(db, contents):
    df = pd.read_excel(io.BytesIO(contents), skiprows=2, header=None)
    db.query(Subscriber).delete()
    header_row = df.iloc[0]
    date_cols = {}
    for col_idx in range(17, len(df.columns)):
        val = header_row[col_idx]
        if pd.notna(val): date_cols[col_idx] = str(val)[:10]
    for i, row in df.iterrows():
        if i == 0: continue
        val_bonbu = str(row[3]) if pd.notna(row[3]) else ""
        if val_bonbu in ("", "nan") or val_bonbu.lstrip("-").isdigit(): continue
        for col_idx, date_str in date_cols.items():
            sub_val = safe_int(row[col_idx])
            if sub_val == 0: continue
            db.add(Subscriber(
                bonbu=val_bonbu, team=str(row[5]) if pd.notna(row[5]) else "",
                agency_code=str(row[8]) if pd.notna(row[8]) else "",
                agency=str(row[11]) if pd.notna(row[11]) else "",
                ref_date=date_str, sub_count=sub_val
            ))
    db.commit()

_ktoa_cache = None

def _load_ktoa(contents):
    global _ktoa_cache
    df_raw = pd.read_excel(io.BytesIO(contents), header=None)
    header0 = df_raw.iloc[0].ffill().tolist()   # ffill() — deprecated fillna(method) 수정
    header1 = df_raw.iloc[1].tolist()
    rows = df_raw.iloc[2:].copy()
    rows.columns = [f"{h0}_{h1}" if pd.notna(h1) else str(h0) for h0, h1 in zip(header0, header1)]
    rows = rows.rename(columns={rows.columns[0]: "date"})
    rows = rows[rows["date"].notna()].copy()
    rows["date"] = rows["date"].astype(str).str[:10]
    for c in rows.columns[1:]:
        rows[c] = pd.to_numeric(rows[c].astype(str).str.replace(",", ""), errors="coerce").fillna(0).astype(int)
    rows = rows[rows["date"] != "일합계"].copy()
    rows = rows[rows["date"].str.match(r"\d{4}-\d{2}-\d{2}")].copy()
    rows = rows.sort_values("date")
    _ktoa_cache = rows.to_dict(orient="records")

@asynccontextmanager
async def lifespan(app_):
    db = SessionLocal()
    try:
        for fname, loader in [
            ("sales.xlsx", _load_sales), ("commission.xlsx", _load_commission),
            ("device.xlsx", _load_device), ("inventory.xlsx", _load_inventory),
            ("subscriber.xlsx", _load_subscriber),
        ]:
            path = os.path.join(DATA_DIR, fname)
            if os.path.exists(path):
                with open(path, "rb") as f: loader(db, f.read())
                print(f"[자동로드] {fname} 완료")
        ktoa_path = os.path.join(DATA_DIR, "ktoa_day.xlsx")
        if os.path.exists(ktoa_path):
            with open(ktoa_path, "rb") as f: _load_ktoa(f.read())
            print("[자동로드] ktoa_day.xlsx 완료")
    except Exception as e: print(f"[자동로드 오류] {e}")
    finally: db.close()
    yield

def _migrate(engine):
    """신규 컬럼 자동 추가 (PostgreSQL: IF NOT EXISTS / SQLite: try-except)"""
    with engine.connect() as conn:
        for col_def in ["foreigner VARCHAR DEFAULT ''"]:
            try:
                conn.execute(text(f"ALTER TABLE sales ADD COLUMN IF NOT EXISTS {col_def}"))
                conn.commit()
            except Exception:
                try:
                    conn.execute(text(f"ALTER TABLE sales ADD COLUMN {col_def}"))
                    conn.commit()
                except Exception:
                    pass

Base.metadata.create_all(bind=engine)
_migrate(engine)

app = FastAPI(title="KT 무선판매 전략 대시보드", lifespan=lifespan)  # lifespan 오타 수정
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── Upload ──────────────────────────────────────────────────────

@app.post("/upload")
async def upload_sales(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        db = SessionLocal(); _load_sales(db, contents)
        total = db.query(func.sum(Sales.sale_count)).scalar() or 0
        return {"status": "성공", "total_sales": int(total)}
    except Exception as e: return {"status": "실패", "error": str(e)}
    finally: db.close()

@app.post("/upload/commission")
async def upload_commission(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        db = SessionLocal(); _load_commission(db, contents)
        total = db.query(func.sum(Commission.amount)).scalar() or 0
        return {"status": "성공", "total_amount": int(total)}
    except Exception as e: return {"status": "실패", "error": str(e)}
    finally: db.close()

@app.post("/upload/device")
async def upload_device(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        db = SessionLocal(); _load_device(db, contents)
        t4 = db.query(func.sum(DeviceSales.sale_count)).filter(DeviceSales.yyyymm == "202604").scalar() or 0
        return {"status": "성공", "april": int(t4)}
    except Exception as e: return {"status": "실패", "error": str(e)}
    finally: db.close()

@app.post("/upload/inventory")
async def upload_inventory(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        db = SessionLocal(); _load_inventory(db, contents)
        return {"status": "성공"}
    except Exception as e: return {"status": "실패", "error": str(e)}
    finally: db.close()

@app.post("/upload/subscriber")
async def upload_subscriber(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        db = SessionLocal(); _load_subscriber(db, contents)
        return {"status": "성공"}
    except Exception as e: return {"status": "실패", "error": str(e)}
    finally: db.close()

@app.post("/upload/ktoa")
async def upload_ktoa(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        _load_ktoa(contents)
        return {"status": "성공", "rows": len(_ktoa_cache) if _ktoa_cache else 0}
    except Exception as e: return {"status": "실패", "error": str(e)}

# ── Filters ─────────────────────────────────────────────────────

@app.get("/api/filters")
async def get_filters(
    bonbu_list: List[str] = Query(default=[]),
    team_list: List[str] = Query(default=[]),
):
    db = SessionLocal()
    try:
        bonbu_all = [r[0] for r in db.query(Sales.bonbu, func.sum(Sales.sale_count))
            .filter(Sales.bonbu != "", Sales.bonbu != "nan")
            .group_by(Sales.bonbu).having(func.sum(Sales.sale_count) >= MIN_BONBU)
            .order_by(Sales.bonbu).all()]

        tq = db.query(Sales.team).distinct().filter(Sales.team != "", Sales.team != "nan")
        if bonbu_list: tq = tq.filter(Sales.bonbu.in_(bonbu_list))
        team_all = [r[0] for r in tq.order_by(Sales.team).all()]

        aq = db.query(Sales.agency).distinct().filter(Sales.agency != "", Sales.agency != "nan")
        if bonbu_list: aq = aq.filter(Sales.bonbu.in_(bonbu_list))
        if team_list: aq = aq.filter(Sales.team.in_(team_list))
        agency_all = [r[0] for r in aq.order_by(Sales.agency).all()]

        channel_all = [r[0] for r in db.query(Sales.channel_sub).distinct()
            .filter(Sales.channel_sub != "", Sales.channel_sub != "nan")
            .order_by(Sales.channel_sub).all()]

        return {"bonbu_list": bonbu_all, "team_list": team_all,
                "agency_list": agency_all, "channel_list": channel_all}
    finally: db.close()

# ── Summary ─────────────────────────────────────────────────────

@app.get("/api/summary")
async def get_summary(
    agency: str = None,
    bonbu_list: List[str] = Query(default=[]),
    team_list: List[str] = Query(default=[]),
    channel_list: List[str] = Query(default=[]),
):
    db = SessionLocal()
    try:
        def af(q):
            if bonbu_list: q = q.filter(Sales.bonbu.in_(bonbu_list))
            if team_list: q = q.filter(Sales.team.in_(team_list))
            if agency: q = q.filter(Sales.agency == agency)
            if channel_list: q = q.filter(Sales.channel_sub.in_(channel_list))
            return q

        base = af(db.query(Sales))
        grand = db.query(Sales)
        def sc(q, col): return int(q.with_entities(func.sum(col)).scalar() or 0)

        total_rev = float(base.with_entities(func.sum(Sales.revenue)).scalar() or 0)
        total_sub = sc(base, Sales.subscriber)
        totals = {
            "sale": sc(base, Sales.sale_count), "subscriber": total_sub,
            "new_sub": sc(base, Sales.new_sub), "mnp": sc(base, Sales.mnp),
            "smnp": sc(base, Sales.smnp), "lmnp": sc(base, Sales.lmnp),
            "mmnp": sc(base, Sales.mmnp), "vmnp": sc(base, Sales.vmnp),
            "churn": sc(base, Sales.churn), "mnp_churn": sc(base, Sales.mnp_churn),
            "smnp_churn": sc(base, Sales.smnp_churn), "lmnp_churn": sc(base, Sales.lmnp_churn),
            "mmnp_churn": sc(base, Sales.mmnp_churn), "vmnp_churn": sc(base, Sales.vmnp_churn),
            "forced_churn": sc(base, Sales.forced_churn), "premium": sc(base, Sales.premium_change),
            "revenue": total_rev, "arpu": round(total_rev / total_sub) if total_sub > 0 else 0,
        }
        grand_totals = {"sale": sc(grand, Sales.sale_count),
                        "revenue": float(grand.with_entities(func.sum(Sales.revenue)).scalar() or 0)}

        def to_list(rows):
            return [{"name": r[0], "value": int(r[1] or 0)} for r in rows
                    if r[0] and r[0] not in ("nan", "ㆍ값없음", "")]

        bonbu_data = to_list(af(db.query(Sales.bonbu, func.sum(Sales.sale_count)))
            .group_by(Sales.bonbu).having(func.sum(Sales.sale_count) >= MIN_BONBU)
            .order_by(func.sum(Sales.sale_count).desc()).all())
        team_data = to_list(af(db.query(Sales.team, func.sum(Sales.sale_count)))
            .filter(Sales.team != "", Sales.team != "nan").group_by(Sales.team)
            .order_by(func.sum(Sales.sale_count).desc()).limit(20).all())
        channel_data = to_list(af(db.query(Sales.channel_sub, func.sum(Sales.sale_count)))
            .filter(Sales.channel_sub != "", Sales.channel_sub != "nan")
            .group_by(Sales.channel_sub).order_by(func.sum(Sales.sale_count).desc()).all())
        type_data = to_list(af(db.query(Sales.sale_type, func.sum(Sales.sale_count)))
            .filter(Sales.sale_type != "", Sales.sale_type != "nan").group_by(Sales.sale_type).all())
        kids_data = to_list(af(db.query(Sales.kids, func.sum(Sales.sale_count)))
            .filter(Sales.kids != "", Sales.kids != "nan").group_by(Sales.kids).all())
        k110_data = to_list(af(db.query(Sales.k110, func.sum(Sales.sale_count)))
            .filter(Sales.k110 != "", Sales.k110 != "nan").group_by(Sales.k110).all())
        foreigner_data = to_list(af(db.query(Sales.foreigner, func.sum(Sales.sale_count)))
            .filter(Sales.foreigner != "", Sales.foreigner != "nan").group_by(Sales.foreigner).all())

        # ── 본부별 상세 (중고/키즈/외국인/110K 비중 포함) ──
        bonbu_detail = []
        for r in af(db.query(
            Sales.bonbu,
            func.sum(Sales.sale_count), func.sum(Sales.subscriber),
            func.sum(Sales.new_sub), func.sum(Sales.mnp),
            func.sum(Sales.smnp), func.sum(Sales.lmnp),
            func.sum(Sales.mmnp), func.sum(Sales.vmnp),
            func.sum(Sales.churn), func.sum(Sales.mnp_churn),
            func.sum(Sales.mmnp_churn), func.sum(Sales.vmnp_churn),
            func.sum(Sales.premium_change), func.sum(Sales.revenue),
            func.count(func.distinct(Sales.agency)),
        )).filter(Sales.bonbu != "", Sales.bonbu != "nan").group_by(Sales.bonbu)\
          .having(func.sum(Sales.sale_count) >= MIN_BONBU)\
          .order_by(func.sum(Sales.sale_count).desc()).all():
            sale = int(r[1] or 0); sub = int(r[2] or 0); rev = float(r[14] or 0)
            nm = r[0]
            used_cnt   = int(af(db.query(func.sum(Sales.sale_count))).filter(Sales.bonbu==nm, Sales.sale_type.like("%중고%")).scalar() or 0)
            kids_cnt   = int(af(db.query(func.sum(Sales.sale_count))).filter(Sales.bonbu==nm, Sales.kids=="키즈").scalar() or 0)
            foreign_cnt= int(af(db.query(func.sum(Sales.sale_count))).filter(Sales.bonbu==nm, Sales.foreigner=="외국인").scalar() or 0)
            k110_cnt   = int(af(db.query(func.sum(Sales.sale_count))).filter(Sales.bonbu==nm, Sales.k110=="이상").scalar() or 0)
            bonbu_detail.append({
                "name": nm, "sale": sale, "sub": sub,
                "new_sub": int(r[3] or 0), "mnp": int(r[4] or 0),
                "smnp": int(r[5] or 0), "lmnp": int(r[6] or 0),
                "mmnp": int(r[7] or 0), "vmnp": int(r[8] or 0),
                "churn": int(r[9] or 0), "mnp_churn": int(r[10] or 0),
                "mmnp_churn": int(r[11] or 0), "vmnp_churn": int(r[12] or 0),
                "premium": int(r[13] or 0), "revenue": rev,
                "arpu": round(rev / sub) if sub > 0 else 0,
                "agency_count": int(r[15] or 0),
                "used_cnt": used_cnt, "kids_cnt": kids_cnt,
                "foreign_cnt": foreign_cnt, "k110_cnt": k110_cnt,
            })

        # ── 채널별 상세 ──
        channel_detail = []
        for r in af(db.query(
            Sales.channel_sub,
            func.sum(Sales.sale_count), func.sum(Sales.subscriber),
            func.sum(Sales.new_sub), func.sum(Sales.mnp),
            func.sum(Sales.mmnp), func.sum(Sales.vmnp),
            func.sum(Sales.churn), func.sum(Sales.premium_change),
            func.sum(Sales.revenue),
        )).filter(Sales.channel_sub != "", Sales.channel_sub != "nan")\
          .group_by(Sales.channel_sub).order_by(func.sum(Sales.sale_count).desc()).all():
            sale = int(r[1] or 0); sub = int(r[2] or 0); rev = float(r[9] or 0)
            nm = r[0]
            normal = int(af(db.query(func.sum(Sales.sale_count))).filter(Sales.channel_sub==nm, Sales.sale_type.like("%일반%")).scalar() or 0)
            used   = int(af(db.query(func.sum(Sales.sale_count))).filter(Sales.channel_sub==nm, Sales.sale_type.like("%중고%")).scalar() or 0)
            channel_detail.append({
                "name": nm, "sale": sale, "sub": sub,
                "new_sub": int(r[3] or 0), "mnp": int(r[4] or 0),
                "mmnp": int(r[5] or 0), "vmnp": int(r[6] or 0),
                "churn": int(r[7] or 0), "premium": int(r[8] or 0),
                "revenue": rev, "arpu": round(rev / sub) if sub > 0 else 0,
                "normal": normal, "used": used,
            })

        # ── 대리점 Top30 ──
        agency_detail = []
        for r in af(db.query(
            Sales.agency, Sales.bonbu,
            func.sum(Sales.sale_count), func.sum(Sales.subscriber),
            func.sum(Sales.new_sub), func.sum(Sales.mnp),
            func.sum(Sales.mmnp), func.sum(Sales.vmnp),
            func.sum(Sales.premium_change), func.sum(Sales.churn),
            func.sum(Sales.revenue),
        )).filter(Sales.agency != "", Sales.agency != "nan")\
          .group_by(Sales.agency, Sales.bonbu)\
          .order_by(func.sum(Sales.sale_count).desc()).limit(30).all():
            sub = int(r[3] or 0); rev = float(r[10] or 0)
            agency_detail.append({
                "name": r[0], "bonbu": r[1], "sale": int(r[2] or 0), "sub": sub,
                "new_sub": int(r[4] or 0), "mnp": int(r[5] or 0),
                "mmnp": int(r[6] or 0), "vmnp": int(r[7] or 0),
                "premium": int(r[8] or 0), "churn": int(r[9] or 0),
                "revenue": rev, "arpu": round(rev / sub) if sub > 0 else 0,
            })

        all_agency = [{"name": r[0], "value": int(r[1] or 0)}
            for r in af(db.query(Sales.agency, func.sum(Sales.sale_count)))
            .filter(Sales.agency != "", Sales.agency != "nan")
            .group_by(Sales.agency).order_by(func.sum(Sales.sale_count).desc()).all()]
        cumsum = pareto_count = 0
        for a in all_agency:
            cumsum += a["value"]; pareto_count += 1
            if totals["sale"] > 0 and cumsum >= totals["sale"] * 0.8: break

        mnp_detail = {
            "smnp": totals["smnp"], "lmnp": totals["lmnp"],
            "mmnp_in": totals["mmnp"], "vmnp": totals["vmnp"],
            "smnp_out": totals["smnp_churn"], "lmnp_out": totals["lmnp_churn"],
            "mmnp_out": totals["mmnp_churn"], "vmnp_out": totals["vmnp_churn"],
        }

        latest_date = db.query(func.max(Subscriber.ref_date)).scalar() or ""
        bonbu_sub_live = {}
        if latest_date:
            for r in db.query(Subscriber.bonbu, func.sum(Subscriber.sub_count))\
                    .filter(Subscriber.ref_date == latest_date, Subscriber.bonbu != "")\
                    .group_by(Subscriber.bonbu).all():
                bonbu_sub_live[r[0]] = int(r[1] or 0)
        for b in bonbu_detail:
            live = bonbu_sub_live.get(b["name"], 0)
            b["live_sub"] = live
            b["penetration"] = round(b["sale"] / live * 100, 2) if live > 0 else 0

        apr_model = {r[0]: int(r[1] or 0) for r in db.query(DeviceSales.model_name, func.sum(DeviceSales.sale_count))
            .filter(DeviceSales.yyyymm=="202604", DeviceSales.model_name!="", DeviceSales.model_name!="nan")
            .group_by(DeviceSales.model_name).all()}
        mar_model = {r[0]: int(r[1] or 0) for r in db.query(DeviceSales.model_name, func.sum(DeviceSales.sale_count))
            .filter(DeviceSales.yyyymm=="202603", DeviceSales.model_name!="", DeviceSales.model_name!="nan")
            .group_by(DeviceSales.model_name).all()}
        device_apr = sorted([{"name":k,"value":v} for k,v in apr_model.items()], key=lambda x:-x["value"])[:15]
        device_mar = sorted([{"name":k,"value":v} for k,v in mar_model.items()], key=lambda x:-x["value"])[:15]

        WORKING_DAYS = 21
        inv_data = []
        for r in db.query(Inventory.model_name, Inventory.total, Inventory.jisa,
                          Inventory.youngi, Inventory.strategy, Inventory.mns, Inventory.ktshop).all():
            if not r[0] or r[0] in ("", "nan"): continue
            apr_sale = apr_model.get(r[0], 0); mar_sale = mar_model.get(r[0], 0)
            daily_avg = round(apr_sale / WORKING_DAYS, 1) if apr_sale > 0 else 0
            days_left = round(r[1] / daily_avg) if daily_avg > 0 else None
            mom = round((apr_sale - mar_sale) / mar_sale * 100, 1) if mar_sale > 0 else None
            inv_data.append({"model": r[0], "inventory": int(r[1]),
                "jisa": int(r[2]), "youngi": int(r[3]), "strategy": int(r[4]),
                "mns": int(r[5]), "ktshop": int(r[6]),
                "apr_sale": apr_sale, "mar_sale": mar_sale,
                "daily_avg": daily_avg, "days_left": days_left, "mom": mom})
        inv_data.sort(key=lambda x: -x["inventory"])

        comm_by_ag = {r[0]: float(r[1] or 0) for r in
            db.query(Commission.agency_name, func.sum(Commission.amount))
            .filter(Commission.agency_name!="", Commission.agency_name!="nan")
            .group_by(Commission.agency_name).all()}
        comm_linked = []
        for s in db.query(Sales.agency, func.sum(Sales.sale_count),
                          func.sum(Sales.revenue), func.sum(Sales.subscriber))\
                .filter(Sales.agency!="", Sales.agency!="nan").group_by(Sales.agency).all():
            if s[0] in comm_by_ag:
                sale=int(s[1] or 0); comm=comm_by_ag[s[0]]; rev=float(s[2] or 0); sub=int(s[3] or 0)
                comm_linked.append({"name":s[0],"sale":sale,"commission":comm,"revenue":rev,
                    "arpu":round(rev/sub) if sub>0 else 0,
                    "comm_per_sale":round(comm/sale) if sale>0 else 0,
                    "roi":round(rev/comm*100,1) if comm>0 else 0})
        comm_linked.sort(key=lambda x: -x["commission"])
        total_comm = float(db.query(func.sum(Commission.amount)).scalar() or 0)
        comm_by_item = [{"name":r[0],"amount":float(r[1] or 0)}
            for r in db.query(Commission.item_code, func.sum(Commission.amount))
            .group_by(Commission.item_code).order_by(func.sum(Commission.amount).desc()).all()
            if r[0] and r[0] not in ("","nan")]
        comm_by_channel = [{"name":r[0],"amount":float(r[1] or 0)}
            for r in db.query(Commission.channel_type, func.sum(Commission.amount))
            .filter(Commission.channel_type!="", Commission.channel_type!="nan")
            .group_by(Commission.channel_type).order_by(func.sum(Commission.amount).desc()).all()]

        return {
            "totals": totals, "grand_totals": grand_totals,
            "bonbu": bonbu_data, "team": team_data, "channel": channel_data,
            "sale_type": type_data, "kids": kids_data, "k110": k110_data, "foreigner": foreigner_data,
            "bonbu_detail": bonbu_detail, "channel_detail": channel_detail,
            "agency_detail": agency_detail, "mnp_detail": mnp_detail,
            "pareto_80_count": pareto_count, "agency_total_count": len(all_agency),
            "latest_sub_date": latest_date,
            "device_apr": device_apr, "device_mar": device_mar, "inv_data": inv_data,
            "comm_linked": comm_linked[:20], "comm_total": total_comm,
            "comm_by_item": comm_by_item, "comm_by_channel": comm_by_channel,
        }
    finally: db.close()

@app.get("/api/ktoa")
async def get_ktoa():
    if not _ktoa_cache: return {"rows": [], "columns": []}
    return {"rows": _ktoa_cache, "columns": list(_ktoa_cache[0].keys())}

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    with open(os.path.join(os.path.dirname(__file__), "templates", "index.html"), encoding="utf-8") as f:
        return f.read()