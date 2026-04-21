import os
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import io
from sqlalchemy import func
from app.database import engine, Base, SessionLocal
from app.models.sales import Sales, Commission

Base.metadata.create_all(bind=engine)
app = FastAPI(title="KT 무선판매 전략 대시보드")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def safe_int(v):
    try:
        return int(v) if pd.notna(v) else 0
    except:
        return 0


def safe_float(v):
    try:
        return float(v) if pd.notna(v) else 0.0
    except:
        return 0.0


# ─────────────────────────────────────────────
# 판매 RAW 업로드
# ─────────────────────────────────────────────
@app.post("/upload")
async def upload_sales(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents), skiprows=2, header=None)
        db = SessionLocal()
        db.query(Sales).delete()
        count = 0
        for _, row in df.iterrows():
            val_bonbu = str(row[3]) if pd.notna(row[3]) else ""
            if val_bonbu == "" or val_bonbu == "nan" or val_bonbu.lstrip("-").isdigit():
                continue
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
                sale_type=str(row[15]) if pd.notna(row[15]) else "",
                kids=str(row[16]) if pd.notna(row[16]) else "",
                k110=str(row[18]) if pd.notna(row[18]) else "",
                sale_count=safe_int(row[21]),
                net_add=safe_int(row[22]),
                new_sub=safe_int(row[23]),
                mnp=safe_int(row[25]),
                smnp=safe_int(row[26]),
                lmnp=safe_int(row[27]),
                mmnp=safe_int(row[28]),
                vmnp=safe_int(row[29]),
                churn=safe_int(row[30]),
                mnp_churn=safe_int(row[32]),
                smnp_churn=safe_int(row[33]),
                lmnp_churn=safe_int(row[34]),
                mmnp_churn=safe_int(row[35]),
                vmnp_churn=safe_int(row[36]),
                forced_churn=safe_int(row[37]),
                premium_change=safe_int(row[38]),
                arpu=safe_float(row[39]),
                revenue=safe_float(row[40]),
                subscriber=safe_int(row[41]),
            ))
            count += 1
        db.commit()
        total = db.query(func.sum(Sales.sale_count)).scalar() or 0
        return {"status": "성공", "total_rows": count, "total_sales": int(total)}
    except Exception as e:
        db.rollback()
        return {"status": "실패", "error": str(e)}
    finally:
        db.close()


# ─────────────────────────────────────────────
# 정책수수료 업로드
# ─────────────────────────────────────────────
@app.post("/upload/commission")
async def upload_commission(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents), skiprows=1, header=1)
        db = SessionLocal()
        db.query(Commission).delete()
        count = 0
        for _, row in df.iterrows():
            agency_code = str(row.get("수수료지급발생조직", "")) if pd.notna(row.get("수수료지급발생조직")) else ""
            if agency_code == "" or agency_code == "nan":
                continue
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
            count += 1
        db.commit()
        total = db.query(func.sum(Commission.amount)).scalar() or 0
        return {"status": "성공", "total_rows": count, "total_amount": int(total)}
    except Exception as e:
        db.rollback()
        return {"status": "실패", "error": str(e)}
    finally:
        db.close()


# ─────────────────────────────────────────────
# 필터 목록
# ─────────────────────────────────────────────
@app.get("/api/filters")
async def get_filters(bonbu: str = None, team: str = None):
    db = SessionLocal()
    try:
        bonbu_list = [r[0] for r in
                      db.query(Sales.bonbu).distinct()
                      .filter(Sales.bonbu != "", Sales.bonbu != "nan")
                      .order_by(Sales.bonbu).all()]

        tq = db.query(Sales.team).distinct().filter(Sales.team != "", Sales.team != "nan")
        if bonbu:
            tq = tq.filter(Sales.bonbu == bonbu)
        team_list = [r[0] for r in tq.order_by(Sales.team).all()]

        aq = db.query(Sales.agency).distinct().filter(Sales.agency != "", Sales.agency != "nan")
        if bonbu:
            aq = aq.filter(Sales.bonbu == bonbu)
        if team:
            aq = aq.filter(Sales.team == team)
        agency_list = [r[0] for r in aq.order_by(Sales.agency).all()]

        return {"bonbu_list": bonbu_list, "team_list": team_list, "agency_list": agency_list}
    finally:
        db.close()


# ─────────────────────────────────────────────
# 메인 집계 API
# ─────────────────────────────────────────────
@app.get("/api/summary")
async def get_summary(bonbu: str = None, team: str = None, agency: str = None):
    db = SessionLocal()
    try:
        def af(q):
            if bonbu:
                q = q.filter(Sales.bonbu == bonbu)
            if team:
                q = q.filter(Sales.team == team)
            if agency:
                q = q.filter(Sales.agency == agency)
            return q

        base = af(db.query(Sales))
        grand = db.query(Sales)

        def scalar(q, col):
            return int(q.with_entities(func.sum(col)).scalar() or 0)

        def scalarf(q, col):
            return float(q.with_entities(func.avg(col)).scalar() or 0)

        totals = {
            "sale": scalar(base, Sales.sale_count),
            "new_sub": scalar(base, Sales.new_sub),
            "mnp": scalar(base, Sales.mnp),
            "smnp": scalar(base, Sales.smnp),
            "lmnp": scalar(base, Sales.lmnp),
            "mmnp": scalar(base, Sales.mmnp),
            "vmnp": scalar(base, Sales.vmnp),
            "churn": scalar(base, Sales.churn),
            "mnp_churn": scalar(base, Sales.mnp_churn),
            "smnp_churn": scalar(base, Sales.smnp_churn),
            "lmnp_churn": scalar(base, Sales.lmnp_churn),
            "mmnp_churn": scalar(base, Sales.mmnp_churn),
            "vmnp_churn": scalar(base, Sales.vmnp_churn),
            "forced_churn": scalar(base, Sales.forced_churn),
            "premium": scalar(base, Sales.premium_change),
            "subscriber": scalar(base, Sales.subscriber),
            "revenue": float(base.with_entities(func.sum(Sales.revenue)).scalar() or 0),
            "arpu": scalarf(base, Sales.arpu),
        }
        grand_totals = {
            "sale": scalar(grand, Sales.sale_count),
            "revenue": float(grand.with_entities(func.sum(Sales.revenue)).scalar() or 0),
        }

        def to_list(rows):
            return [{"name": r[0], "value": int(r[1] or 0)} for r in rows
                    if r[0] and r[0] not in ("nan", "ㆍ값없음", "")]

        bonbu_data = to_list(af(db.query(Sales.bonbu, func.sum(Sales.sale_count)))
                             .group_by(Sales.bonbu)
                             .order_by(func.sum(Sales.sale_count).desc()).all())

        team_data = to_list(af(db.query(Sales.team, func.sum(Sales.sale_count)))
                            .filter(Sales.team != "", Sales.team != "nan")
                            .group_by(Sales.team)
                            .order_by(func.sum(Sales.sale_count).desc()).limit(20).all())

        channel_data = to_list(af(db.query(Sales.channel2, func.sum(Sales.sale_count)))
                               .filter(Sales.channel2 != "", Sales.channel2 != "nan",
                                       Sales.channel2 != "ㆍ값없음")
                               .group_by(Sales.channel2)
                               .order_by(func.sum(Sales.sale_count).desc()).all())

        type_data = to_list(af(db.query(Sales.sale_type, func.sum(Sales.sale_count)))
                            .filter(Sales.sale_type != "", Sales.sale_type != "nan")
                            .group_by(Sales.sale_type).all())
        kids_data = to_list(af(db.query(Sales.kids, func.sum(Sales.sale_count)))
                            .filter(Sales.kids != "", Sales.kids != "nan")
                            .group_by(Sales.kids).all())
        k110_data = to_list(af(db.query(Sales.k110, func.sum(Sales.sale_count)))
                            .filter(Sales.k110 != "", Sales.k110 != "nan")
                            .group_by(Sales.k110).all())

        bonbu_detail = []
        for r in af(db.query(
            Sales.bonbu,
            func.sum(Sales.sale_count),
            func.sum(Sales.subscriber),
            func.sum(Sales.new_sub),
            func.sum(Sales.mnp),
            func.sum(Sales.smnp),
            func.sum(Sales.lmnp),
            func.sum(Sales.mmnp),
            func.sum(Sales.vmnp),
            func.sum(Sales.churn),
            func.sum(Sales.mnp_churn),
            func.sum(Sales.premium_change),
            func.avg(Sales.arpu),
            func.sum(Sales.revenue),
            func.count(func.distinct(Sales.agency)),
        )).filter(Sales.bonbu != "", Sales.bonbu != "nan").group_by(Sales.bonbu)\
          .order_by(func.sum(Sales.sale_count).desc()).all():
            sale = int(r[1] or 0)
            sub = int(r[2] or 0)
            bonbu_detail.append({
                "name": r[0], "sale": sale, "sub": sub,
                "new_sub": int(r[3] or 0),
                "mnp": int(r[4] or 0),
                "smnp": int(r[5] or 0), "lmnp": int(r[6] or 0),
                "mmnp": int(r[7] or 0), "vmnp": int(r[8] or 0),
                "churn": int(r[9] or 0),
                "mnp_churn": int(r[10] or 0),
                "premium": int(r[11] or 0),
                "arpu": round(float(r[12] or 0)),
                "revenue": float(r[13] or 0),
                "agency_count": int(r[14] or 0),
                "gap": sub - sale,
            })

        channel_detail = []
        for r in af(db.query(
            Sales.channel2,
            func.sum(Sales.sale_count),
            func.sum(Sales.subscriber),
            func.sum(Sales.new_sub),
            func.sum(Sales.mnp),
            func.sum(Sales.churn),
            func.avg(Sales.arpu),
            func.sum(Sales.revenue),
        )).filter(Sales.channel2 != "", Sales.channel2 != "nan", Sales.channel2 != "ㆍ값없음")\
          .group_by(Sales.channel2).order_by(func.sum(Sales.sale_count).desc()).all():
            channel_detail.append({
                "name": r[0], "sale": int(r[1] or 0),
                "sub": int(r[2] or 0),
                "new_sub": int(r[3] or 0),
                "mnp": int(r[4] or 0),
                "churn": int(r[5] or 0),
                "arpu": round(float(r[6] or 0)),
                "revenue": float(r[7] or 0),
            })

        agency_detail = []
        for r in af(db.query(
            Sales.agency,
            Sales.bonbu,
            func.sum(Sales.sale_count),
            func.sum(Sales.subscriber),
            func.sum(Sales.new_sub),
            func.sum(Sales.mnp),
            func.sum(Sales.premium_change),
            func.sum(Sales.churn),
            func.avg(Sales.arpu),
            func.sum(Sales.revenue),
        )).filter(Sales.agency != "", Sales.agency != "nan")\
          .group_by(Sales.agency, Sales.bonbu)\
          .order_by(func.sum(Sales.sale_count).desc()).limit(30).all():
            agency_detail.append({
                "name": r[0], "bonbu": r[1],
                "sale": int(r[2] or 0), "sub": int(r[3] or 0),
                "new_sub": int(r[4] or 0), "mnp": int(r[5] or 0),
                "premium": int(r[6] or 0), "churn": int(r[7] or 0),
                "arpu": round(float(r[8] or 0)),
                "revenue": float(r[9] or 0),
            })

        all_agency = to_list(af(db.query(Sales.agency, func.sum(Sales.sale_count)))
                             .filter(Sales.agency != "", Sales.agency != "nan")
                             .group_by(Sales.agency)
                             .order_by(func.sum(Sales.sale_count).desc()).all())
        total_sale = totals["sale"]
        cumsum, pareto_count = 0, 0
        for a in all_agency:
            cumsum += a["value"]
            pareto_count += 1
            if cumsum >= total_sale * 0.8:
                break

        mnp_detail = {
            "smnp": totals["smnp"], "lmnp": totals["lmnp"],
            "mmnp_in": totals["mmnp"], "vmnp": totals["vmnp"],
            "smnp_out": totals["smnp_churn"], "lmnp_out": totals["lmnp_churn"],
            "mmnp_out": totals["mmnp_churn"], "vmnp_out": totals["vmnp_churn"],
        }

        return {
            "totals": totals,
            "grand_totals": grand_totals,
            "bonbu": bonbu_data,
            "team": team_data,
            "channel": channel_data,
            "sale_type": type_data,
            "kids": kids_data,
            "k110": k110_data,
            "bonbu_detail": bonbu_detail,
            "channel_detail": channel_detail,
            "agency_detail": agency_detail,
            "mnp_detail": mnp_detail,
            "pareto_80_count": pareto_count,
            "agency_total_count": len(all_agency),
        }
    finally:
        db.close()


# ─────────────────────────────────────────────
# 수수료 집계 API
# ─────────────────────────────────────────────
@app.get("/api/commission")
async def get_commission(agency_name: str = None):
    db = SessionLocal()
    try:
        base = db.query(Commission)
        if agency_name:
            base = base.filter(Commission.agency_name == agency_name)

        total_amount = float(base.with_entities(func.sum(Commission.amount)).scalar() or 0)

        by_agency = [
            {"name": r[0], "amount": float(r[1] or 0), "count": int(r[2] or 0)}
            for r in base.with_entities(
                Commission.agency_name,
                func.sum(Commission.amount),
                func.count(Commission.id)
            ).filter(Commission.agency_name != "", Commission.agency_name != "nan")
             .group_by(Commission.agency_name)
             .order_by(func.sum(Commission.amount).desc()).limit(20).all()
        ]

        by_item = [
            {"name": r[0], "amount": float(r[1] or 0)}
            for r in base.with_entities(Commission.item_code, func.sum(Commission.amount))
             .group_by(Commission.item_code)
             .order_by(func.sum(Commission.amount).desc()).all()
        ]

        by_paytype = [
            {"name": r[0], "amount": float(r[1] or 0)}
            for r in base.with_entities(Commission.pay_type, func.sum(Commission.amount))
             .group_by(Commission.pay_type)
             .order_by(func.sum(Commission.amount).desc()).all()
        ]

        by_channel = [
            {"name": r[0], "amount": float(r[1] or 0)}
            for r in base.with_entities(Commission.channel_path, func.sum(Commission.amount))
             .filter(Commission.channel_path != "", Commission.channel_path != "nan")
             .group_by(Commission.channel_path)
             .order_by(func.sum(Commission.amount).desc()).all()
        ]

        # 판매량 × 수수료 연계
        comm_by_ag = {r["name"]: r["amount"] for r in by_agency}
        sales_by_ag = db.query(Sales.agency, func.sum(Sales.sale_count), func.avg(Sales.arpu))\
            .filter(Sales.agency != "", Sales.agency != "nan")\
            .group_by(Sales.agency).all()

        linked = []
        for s in sales_by_ag:
            ag_name = s[0]
            if ag_name in comm_by_ag:
                sale = int(s[1] or 0)
                comm = comm_by_ag[ag_name]
                arpu = round(float(s[2] or 0))
                linked.append({
                    "name": ag_name,
                    "sale": sale,
                    "commission": comm,
                    "arpu": arpu,
                    "commission_per_sale": round(comm / sale) if sale > 0 else 0,
                })
        linked.sort(key=lambda x: -x["commission"])

        return {
            "total_amount": total_amount,
            "by_agency": by_agency,
            "by_item": by_item,
            "by_paytype": by_paytype,
            "by_channel": by_channel,
            "linked": linked[:20],
        }
    finally:
        db.close()


# ─────────────────────────────────────────────
# 대시보드 HTML
# ─────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def dashboard():
    html_path = os.path.join(os.path.dirname(__file__), "templates", "index.html")
    with open(html_path, encoding="utf-8") as f:
        return f.read()