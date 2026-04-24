import os, io
from fastapi import FastAPI, UploadFile, File, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from typing import List
import pandas as pd
from sqlalchemy import func, text, Column, Integer, Float, String
from app.database import engine, Base, SessionLocal
from app.models.sales import Sales, Commission, DeviceSales, Inventory, Subscriber

MIN_BONBU = 100
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
BATCH = 200


# ── Dynamic columns / auxiliary tables ────────────────────────────
def _attach_column(model, attr, coltype, default=None):
    if not hasattr(model, attr):
        setattr(model, attr, Column(attr, coltype, default=default))

_attach_column(Sales, "new_sale", Integer, 0)   # 신규판매: 010신규 + MNP 성격의 전체 신규판매
_attach_column(Sales, "new010", Integer, 0)     # 010신규: 순수 신규
_attach_column(Sales, "new_arpu", Float, 0.0)   # 신규ARPU: 원천 없으면 arpu 가중평균으로 대체
_attach_column(DeviceSales, "new_sale", Integer, 0)  # 단말별 신규판매. 원천 없으면 0

# 가입자 모델은 과거 스키마(sub_today/sub_yesterday)와 현재 스키마(ref_date/sub_count)가
# 혼재할 수 있어 main.py에서 필요한 컬럼을 런타임에 보강한다.
_attach_column(Subscriber, "agency_code", String, "")
_attach_column(Subscriber, "ref_date", String, "")
_attach_column(Subscriber, "sub_count", Integer, 0)

class StoreSales(Base):
    __tablename__ = "store_sales"
    id = Column(Integer, primary_key=True, index=True)
    ref_month = Column(String, index=True, default="")
    sale_date = Column(String, index=True, default="")
    bonbu = Column(String, index=True, default="")
    team = Column(String, index=True, default="")
    agency = Column(String, index=True, default="")
    agency_code = Column(String, index=True, default="")
    store = Column(String, index=True, default="")
    contact = Column(String, index=True, default="")
    channel = Column(String, index=True, default="")
    sale = Column(Integer, default=0)
    new_sale = Column(Integer, default=0)
    new010 = Column(Integer, default=0)
    mnp = Column(Integer, default=0)
    premium = Column(Integer, default=0)
    churn = Column(Integer, default=0)
    revenue = Column(Float, default=0.0)
    arpu = Column(Float, default=0.0)

class CommonSubsidy(Base):
    __tablename__ = "common_subsidy"
    id = Column(Integer, primary_key=True, index=True)
    ref_date = Column(String, index=True, default="")
    model_name = Column(String, index=True, default="")
    model_code = Column(String, index=True, default="")
    carrier = Column(String, index=True, default="KT")
    join_type = Column(String, index=True, default="")
    channel = Column(String, index=True, default="")
    plan_group = Column(String, index=True, default="")
    amount = Column(Float, default=0.0)

class SalesTarget(Base):
    __tablename__ = "sales_target"
    id = Column(Integer, primary_key=True, index=True)
    yyyymm = Column(String, index=True, default="")
    level = Column(String, index=True, default="bonbu")
    name = Column(String, index=True, default="")
    target_sale = Column(Integer, default=0)
    target_new_sale = Column(Integer, default=0)
    target_mnp = Column(Integer, default=0)

class BusinessDay(Base):
    __tablename__ = "business_day"
    id = Column(Integer, primary_key=True, index=True)
    yyyymm = Column(String, index=True, default="")
    elapsed_days = Column(Integer, default=0)
    total_days = Column(Integer, default=0)
    annual_elapsed_days = Column(Integer, default=0)
    annual_total_days = Column(Integer, default=0)


def _row_val(row, idx, default=0):
    try:
        return row.iloc[idx]
    except Exception:
        return default


def _pick_col(df, candidates, fallback=None):
    cols = [str(c).strip() for c in df.columns]
    for cand in candidates:
        for c in cols:
            if cand in c:
                return c
    return fallback


def _read_headered_excel(contents, header_terms):
    raw = pd.read_excel(io.BytesIO(contents), header=None)
    best_i, best_score = 0, -1
    for i in range(min(12, len(raw))):
        vals = [str(v).strip() for v in raw.iloc[i].tolist() if pd.notna(v)]
        joined = " ".join(vals)
        score = sum(1 for t in header_terms if t in joined)
        if score > best_score:
            best_i, best_score = i, score
    cols = []
    used = {}
    for j, v in enumerate(raw.iloc[best_i].tolist()):
        name = str(v).strip() if pd.notna(v) and str(v).strip() not in ("", "nan") else f"col_{j}"
        if name in used:
            used[name] += 1
            name = f"{name}_{used[name]}"
        else:
            used[name] = 0
        cols.append(name)
    df = raw.iloc[best_i + 1:].copy()
    df.columns = cols
    return df

def safe_int(v):
    try: return int(v) if pd.notna(v) else 0
    except: return 0

def safe_float(v):
    try: return float(v) if pd.notna(v) else 0.0
    except: return 0.0

def _load_sales(db, contents):
    df = pd.read_excel(io.BytesIO(contents), skiprows=2, header=None)
    db.query(Sales).delete(); db.commit()
    buf = []
    for _, row in df.iterrows():
        val_bonbu = str(_row_val(row, 3, "")) if pd.notna(_row_val(row, 3, "")) else ""
        if val_bonbu in ("", "nan") or val_bonbu.lstrip("-").isdigit(): continue
        sale_cnt = safe_int(_row_val(row, 21))
        new_sale = safe_int(_row_val(row, 23))
        n010_raw = safe_int(_row_val(row, 24))
        mnp_val = safe_int(_row_val(row, 25))
        n010 = n010_raw if n010_raw > 0 else max(0, new_sale - mnp_val)
        arpu_val = safe_float(_row_val(row, 39))
        obj = Sales(
            boomun=str(_row_val(row, 1, "")) if pd.notna(_row_val(row, 1, "")) else "",
            bonbu=val_bonbu, team=str(_row_val(row, 5, "")) if pd.notna(_row_val(row, 5, "")) else "",
            dept=str(_row_val(row, 7, "")) if pd.notna(_row_val(row, 7, "")) else "",
            agency_code=str(_row_val(row, 8, "")) if pd.notna(_row_val(row, 8, "")) else "",
            agency_org=str(_row_val(row, 9, "")) if pd.notna(_row_val(row, 9, "")) else "",
            agency=str(_row_val(row, 11, "")) if pd.notna(_row_val(row, 11, "")) else "",
            channel1=str(_row_val(row, 12, "")) if pd.notna(_row_val(row, 12, "")) else "",
            channel2=str(_row_val(row, 13, "")) if pd.notna(_row_val(row, 13, "")) else "",
            channel3=str(_row_val(row, 14, "")) if pd.notna(_row_val(row, 14, "")) else "",
            channel_sub=str(_row_val(row, 19, "")) if pd.notna(_row_val(row, 19, "")) else "",
            sale_type=str(_row_val(row, 15, "")) if pd.notna(_row_val(row, 15, "")) else "",
            kids=str(_row_val(row, 16, "")) if pd.notna(_row_val(row, 16, "")) else "",
            foreigner=str(_row_val(row, 17, "")) if pd.notna(_row_val(row, 17, "")) else "",
            k110=str(_row_val(row, 18, "")) if pd.notna(_row_val(row, 18, "")) else "",
            sale_count=sale_cnt, net_add=safe_int(_row_val(row, 22)),
            new_sub=new_sale, mnp=mnp_val,
            smnp=safe_int(_row_val(row, 26)), lmnp=safe_int(_row_val(row, 27)),
            mmnp=safe_int(_row_val(row, 28)), vmnp=safe_int(_row_val(row, 29)),
            churn=safe_int(_row_val(row, 30)), mnp_churn=safe_int(_row_val(row, 32)),
            smnp_churn=safe_int(_row_val(row, 33)), lmnp_churn=safe_int(_row_val(row, 34)),
            mmnp_churn=safe_int(_row_val(row, 35)), vmnp_churn=safe_int(_row_val(row, 36)),
            forced_churn=safe_int(_row_val(row, 37)), premium_change=safe_int(_row_val(row, 38)),
            arpu=arpu_val, revenue=safe_float(_row_val(row, 40)),
            subscriber=safe_int(_row_val(row, 41)),
        )
        obj.new_sale = new_sale
        obj.new010 = n010
        obj.new_arpu = arpu_val
        buf.append(obj)
        if len(buf) >= BATCH: db.bulk_save_objects(buf); db.commit(); buf = []
    if buf: db.bulk_save_objects(buf); db.commit()

def _load_commission(db, contents):
    df = pd.read_excel(io.BytesIO(contents), skiprows=1, header=1)
    db.query(Commission).delete(); db.commit()
    buf = []
    for _, row in df.iterrows():
        agency_code = str(row.get("수수료지급발생조직", "")) if pd.notna(row.get("수수료지급발생조직")) else ""
        if agency_code in ("", "nan"): continue
        # 수수료정책명 = Unnamed: 14 (수수료정책 컬럼 바로 다음)
        policy_name = str(row.iloc[14]) if pd.notna(row.iloc[14]) else ""
        # 판매정책명 = Unnamed: 11
        sale_policy_name = str(row.iloc[11]) if pd.notna(row.iloc[11]) else ""
        buf.append(Commission(
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
            commission_policy_name=policy_name,
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
        if len(buf) >= BATCH: db.bulk_save_objects(buf); db.commit(); buf = []
    if buf: db.bulk_save_objects(buf); db.commit()

def _load_device(db, contents):
    import re as _re
    df = pd.read_excel(io.BytesIO(contents), header=None)
    db.query(DeviceSales).delete(); db.commit()
    if df.shape[0] < 5: return
    row2 = [str(v).strip() if pd.notna(v) else "" for v in df.iloc[2].tolist()]
    row3 = [str(v).strip() if pd.notna(v) else "" for v in df.iloc[3].tolist()]
    # 년월/판매량 컬럼 탐색
    pairs = []
    seen = {}
    for ci, v in enumerate(row2):
        vv = v.replace(",", "")
        if vv.isdigit() and len(vv) == 6:
            yyyymm = vv
            if yyyymm not in seen:
                seen[yyyymm] = ci
                sale_col, rev_col = None, None
                for offset in range(0, 5):
                    if ci + offset >= len(row3): break
                    m = row3[ci + offset]
                    if "판매량" in m and sale_col is None: sale_col = ci + offset
                    elif "매출" in m and rev_col is None: rev_col = ci + offset
                if sale_col is not None:
                    pairs.append((yyyymm, sale_col, rev_col))
    if not pairs:
        pairs = [("202603", 12, 13), ("202604", 14, 15)]

    def extract_group(alias):
        """별칭명에서 대표 그룹명 추출 (용량/색상/통신사 제거)"""
        if not alias or str(alias).strip() in ("ㆍ값없음", "_", "nan", ""):
            return None
        a = str(alias).strip()
        a = _re.sub(r"\s*(128GB|256GB|512GB|1TB|2TB|64GB|32GB|16GB)\s*", " ", a)
        a = _re.sub(r"\s*(SKT|LGU|KT|자급제|타사향|데모|리퍼|교체|중간시스템|교체단말|리퍼폰)\s*", " ", a)
        a = _re.sub(r"\s+", " ", a).strip()
        return a or None

    buf = []
    for ri, row in df.iterrows():
        if ri < 4: continue
        bonbu = str(row.iloc[1]) if pd.notna(row.iloc[1]) else ""
        if bonbu in ("", "nan") or bonbu.lstrip("-").isdigit(): continue
        team = str(row.iloc[3]) if pd.notna(row.iloc[3]) else ""
        agency_code = str(row.iloc[4]) if pd.notna(row.iloc[4]) else ""
        agency = str(row.iloc[5]) if pd.notna(row.iloc[5]) else ""
        alias = str(row.iloc[10]) if pd.notna(row.iloc[10]) else ""
        model_name = extract_group(alias)
        if not model_name:
            continue  # K000000 코드는 스킵
        rep_model_code = str(row.iloc[6]) if pd.notna(row.iloc[6]) else ""
        for yyyymm, sc_col, rv_col in pairs:
            sc = safe_int(row.iloc[sc_col]) if sc_col < len(row) else 0
            rv = safe_float(row.iloc[rv_col]) if (rv_col is not None and rv_col < len(row)) else 0.0
            if sc == 0 and rv == 0.0: continue
            obj = DeviceSales(
                bonbu=bonbu, team=team, agency_code=agency_code, agency=agency,
                model_code=rep_model_code, model_name=model_name,
                yyyymm=yyyymm, sale_count=sc, revenue=rv,
            )
            obj.new_sale = 0
            buf.append(obj)
            if len(buf) >= BATCH: db.bulk_save_objects(buf); db.commit(); buf = []
    if buf: db.bulk_save_objects(buf); db.commit()
def _load_inventory(db, contents):
    import re as _re
    df = pd.read_excel(io.BytesIO(contents), skiprows=1, header=1)
    db.query(Inventory).delete(); db.commit()

    def extract_group(alias):
        if not alias or str(alias).strip() in ("ㆍ값없음","_","nan","","None"): return None
        a = str(alias).strip()
        a = _re.sub(r"\s*\(Demo\)\s*", " ", a, flags=_re.IGNORECASE)
        a = _re.sub(r"\s*데모\s*", " ", a)
        a = _re.sub(r"\s*(128GB|256GB|512GB|1TB|2TB|64GB|32GB|16GB)\s*", " ", a)
        a = _re.sub(r"\s*(SKT|LGU|KT|자급제|타사향|리퍼|교체|중간시스템)\s*", " ", a)
        a = _re.sub(r"\s+", " ", a).strip()
        return a or None

    # 컬럼명 매핑 (실제 파일 구조 기준)
    # col: 일자, 재고조직레벨2, Unnamed:2(본부명), 재고조직레벨3, 재고조직, Unnamed:5(대리점),
    #      단말기모델대표단말기모델, Unnamed:7(대표모델코드), 단말기모델, Unnamed:9(세부코드),
    #      단말기별칭명, 메트릭, 재고량(KT+제조사)
    alias_col  = "단말기별칭명"
    bonbu_col  = "Unnamed: 2"
    qty_col    = "재고량 (KT+제조사)"
    date_col   = "일자"
    if alias_col not in df.columns or qty_col not in df.columns:
        # 폴백: 기존 방식
        for _, row in df.iterrows():
            model = str(row.get("단말기모델","")) if pd.notna(row.get("단말기모델")) else ""
            if model in ("","nan","합계"): continue
            db.add(Inventory(
                ref_date=str(row.get("일자",""))[:10], model_name=model,
                total=safe_int(row.iloc[3]), jisa=safe_int(row.iloc[4]),
                youngi=safe_int(row.iloc[5]), strategy=safe_int(row.iloc[6]),
                mns=safe_int(row.iloc[7]), ktshop=safe_int(row.iloc[8]),
                etc=safe_int(row.iloc[9])
            ))
        db.commit(); return

    # 별칭명 기반 단말 그룹 집계
    ref_date = str(df[date_col].dropna().iloc[0])[:10] if date_col in df.columns and len(df[date_col].dropna()) > 0 else ""
    df["_group"]  = df[alias_col].apply(extract_group)
    df["_bonbu"]  = df[bonbu_col] if bonbu_col in df.columns else ""
    df["_qty"]    = pd.to_numeric(df[qty_col], errors="coerce").fillna(0).astype(int)

    # 전체 집계 (본부 무관)
    total_agg = df[df["_group"].notna()].groupby("_group")["_qty"].sum().to_dict()

    # MNS(KT M&S) 집계
    mns_agg = df[(df["_group"].notna()) & (df["재고조직레벨2"]=="MNS0100")].groupby("_group")["_qty"].sum().to_dict()

    # KTShop 집계 - 영업채널본부 계열
    ktshop_agg = df[(df["_group"].notna()) & (df["재고조직레벨2"]=="540026")].groupby("_group")["_qty"].sum().to_dict()

    # 본부별 집계 (영기/지사 분리)
    # 지사 = 고객본부 계열 (545784, 545988, 546148, 546314, 546483, 546624, 546729, 546793)
    jisa_codes = {"545784","545988","546148","546314","546483","546624","546729","546793","413279"}
    jisa_agg = df[(df["_group"].notna()) & (df["재고조직레벨2"].isin(jisa_codes))].groupby("_group")["_qty"].sum().to_dict()

    # 전략 = 마케팅혁신본부
    strategy_agg = df[(df["_group"].notna()) & (df["재고조직레벨2"]=="540002")].groupby("_group")["_qty"].sum().to_dict()

    buf = []
    for model_name, total in total_agg.items():
        if total <= 0: continue
        buf.append(Inventory(
            ref_date=ref_date, model_name=model_name,
            total=int(total),
            jisa=int(jisa_agg.get(model_name, 0)),
            youngi=0,
            strategy=int(strategy_agg.get(model_name, 0)),
            mns=int(mns_agg.get(model_name, 0)),
            ktshop=int(ktshop_agg.get(model_name, 0)),
            etc=max(0, int(total) - int(jisa_agg.get(model_name,0)) - int(strategy_agg.get(model_name,0)) - int(mns_agg.get(model_name,0)) - int(ktshop_agg.get(model_name,0)))
        ))
        if len(buf) >= BATCH: db.bulk_save_objects(buf); db.commit(); buf = []
    if buf: db.bulk_save_objects(buf); db.commit()
def _load_subscriber(db, contents):
    df = pd.read_excel(io.BytesIO(contents), skiprows=2, header=None)
    db.query(Subscriber).delete(); db.commit()
    header_row = df.iloc[0]
    date_cols = {ci: str(v)[:10] for ci, v in enumerate(header_row) if ci >= 17 and pd.notna(v)}
    buf = []
    for i, row in df.iterrows():
        if i == 0: continue
        val_bonbu = str(row[3]) if pd.notna(row[3]) else ""
        if val_bonbu in ("", "nan") or val_bonbu.lstrip("-").isdigit(): continue
        for ci, date_str in date_cols.items():
            sv = safe_int(row[ci])
            if sv == 0: continue
            buf.append(Subscriber(
                bonbu=val_bonbu, team=str(row[5]) if pd.notna(row[5]) else "",
                agency_code=str(row[8]) if pd.notna(row[8]) else "",
                agency=str(row[11]) if pd.notna(row[11]) else "",
                ref_date=date_str, sub_count=sv
            ))
            if len(buf) >= BATCH: db.bulk_save_objects(buf); db.commit(); buf = []
    if buf: db.bulk_save_objects(buf); db.commit()

_ktoa_cache = None


def _load_storesales(db, contents):
    df = _read_headered_excel(contents, ["본부", "담당", "대리점", "판매", "신규", "010", "MNP", "해지", "ARPU"])
    db.query(StoreSales).delete(); db.commit()
    c_month = _pick_col(df, ["년월", "월", "기준월"])
    c_date = _pick_col(df, ["일자", "기준일", "개통일"])
    c_bonbu = _pick_col(df, ["본부"])
    c_team = _pick_col(df, ["담당", "지사"])
    c_agency = _pick_col(df, ["계약대리점", "대리점"])
    c_agency_code = _pick_col(df, ["대리점코드", "무선유통조직", "판매조직"])
    c_store = _pick_col(df, ["판매매장", "매장명", "매장"])
    c_contact = _pick_col(df, ["판매접점", "접점"])
    c_channel = _pick_col(df, ["채널", "판매유형", "판매경로"])
    c_sale = _pick_col(df, ["판매량", "총판매", "판매건수", "판매"])
    c_new = _pick_col(df, ["신규판매", "신규"])
    c_010 = _pick_col(df, ["010"])
    c_mnp = _pick_col(df, ["MNP", "번호이동"])
    c_premium = _pick_col(df, ["기변", "기기변경"])
    c_churn = _pick_col(df, ["해지"])
    c_rev = _pick_col(df, ["판매매출", "매출"])
    c_arpu = _pick_col(df, ["ARPU", "arpu"])
    buf = []
    for _, row in df.iterrows():
        bonbu = str(row.get(c_bonbu, "")).strip() if c_bonbu else ""
        store = str(row.get(c_store, "")).strip() if c_store else ""
        contact = str(row.get(c_contact, "")).strip() if c_contact else ""
        sale = safe_int(row.get(c_sale, 0)) if c_sale else 0
        if bonbu in ("", "nan", "None") and store in ("", "nan", "None") and sale == 0:
            continue
        new_sale = safe_int(row.get(c_new, 0)) if c_new else 0
        mnp = safe_int(row.get(c_mnp, 0)) if c_mnp else 0
        n010 = safe_int(row.get(c_010, 0)) if c_010 else max(0, new_sale - mnp)
        buf.append(StoreSales(
            ref_month=str(row.get(c_month, "")).strip()[:6] if c_month else "",
            sale_date=str(row.get(c_date, "")).strip()[:10] if c_date else "",
            bonbu=bonbu,
            team=str(row.get(c_team, "")).strip() if c_team else "",
            agency=str(row.get(c_agency, "")).strip() if c_agency else "",
            agency_code=str(row.get(c_agency_code, "")).strip() if c_agency_code else "",
            store=store,
            contact=contact,
            channel=str(row.get(c_channel, "")).strip() if c_channel else "",
            sale=sale,
            new_sale=new_sale,
            new010=n010,
            mnp=mnp,
            premium=safe_int(row.get(c_premium, 0)) if c_premium else 0,
            churn=safe_int(row.get(c_churn, 0)) if c_churn else 0,
            revenue=safe_float(row.get(c_rev, 0)) if c_rev else 0.0,
            arpu=safe_float(row.get(c_arpu, 0)) if c_arpu else 0.0,
        ))
        if len(buf) >= BATCH: db.bulk_save_objects(buf); db.commit(); buf = []
    if buf: db.bulk_save_objects(buf); db.commit()


def _load_subsidy(db, contents):
    df = _read_headered_excel(contents, ["단말", "모델", "지원금", "공통", "MNP", "기변", "010"])
    db.query(CommonSubsidy).delete(); db.commit()
    c_date = _pick_col(df, ["일자", "기준일", "시작일", "변경일"])
    c_model = _pick_col(df, ["단말기별칭명", "단말명", "모델명", "단말"])
    c_code = _pick_col(df, ["모델코드", "단말기모델", "대표단말"])
    c_carrier = _pick_col(df, ["사업자", "통신사"])
    c_type = _pick_col(df, ["가입유형", "유형"])
    c_channel = _pick_col(df, ["채널", "판매경로"])
    c_plan = _pick_col(df, ["요금", "요금제", "구간"])
    c_amount = _pick_col(df, ["지원금", "공시", "공통", "금액", "지급액"])
    buf = []
    for _, row in df.iterrows():
        model = str(row.get(c_model, "")).strip() if c_model else ""
        amount = safe_float(row.get(c_amount, 0)) if c_amount else 0.0
        if model in ("", "nan", "None", "합계") and amount == 0:
            continue
        buf.append(CommonSubsidy(
            ref_date=str(row.get(c_date, "")).strip()[:10] if c_date else "",
            model_name=model,
            model_code=str(row.get(c_code, "")).strip() if c_code else "",
            carrier=str(row.get(c_carrier, "KT")).strip() if c_carrier else "KT",
            join_type=str(row.get(c_type, "")).strip() if c_type else "",
            channel=str(row.get(c_channel, "")).strip() if c_channel else "",
            plan_group=str(row.get(c_plan, "")).strip() if c_plan else "",
            amount=amount,
        ))
        if len(buf) >= BATCH: db.bulk_save_objects(buf); db.commit(); buf = []
    if buf: db.bulk_save_objects(buf); db.commit()


def _load_targets(db, contents):
    df = _read_headered_excel(contents, ["년월", "본부", "담당", "목표", "판매"])
    db.query(SalesTarget).delete(); db.commit()
    c_mm = _pick_col(df, ["년월", "월"])
    c_level = _pick_col(df, ["구분", "레벨"])
    c_name = _pick_col(df, ["본부", "담당", "대리점", "조직"])
    c_sale = _pick_col(df, ["판매목표", "목표판매", "목표"])
    c_new = _pick_col(df, ["신규목표", "신규"])
    c_mnp = _pick_col(df, ["MNP목표", "MNP"])
    buf=[]
    for _, row in df.iterrows():
        name = str(row.get(c_name, "")).strip() if c_name else ""
        if name in ("", "nan", "None"): continue
        buf.append(SalesTarget(
            yyyymm=str(row.get(c_mm, "")).strip()[:6] if c_mm else "",
            level=str(row.get(c_level, "bonbu")).strip() if c_level else "bonbu",
            name=name,
            target_sale=safe_int(row.get(c_sale, 0)) if c_sale else 0,
            target_new_sale=safe_int(row.get(c_new, 0)) if c_new else 0,
            target_mnp=safe_int(row.get(c_mnp, 0)) if c_mnp else 0,
        ))
        if len(buf) >= BATCH: db.bulk_save_objects(buf); db.commit(); buf=[]
    if buf: db.bulk_save_objects(buf); db.commit()


def _load_business_days(db, contents):
    df = _read_headered_excel(contents, ["년월", "영업일", "경과", "전체"])
    db.query(BusinessDay).delete(); db.commit()
    c_mm = _pick_col(df, ["년월", "월"])
    c_elapsed = _pick_col(df, ["경과", "현재", "실적영업일"])
    c_total = _pick_col(df, ["총영업일", "전체", "마감", "영업일수"])
    c_ae = _pick_col(df, ["연간경과", "연경과"])
    c_at = _pick_col(df, ["연간총", "연총", "연간영업"])
    buf=[]
    for _, row in df.iterrows():
        mm=str(row.get(c_mm, "")).strip()[:6] if c_mm else ""
        if mm in ("", "nan", "None"): continue
        buf.append(BusinessDay(
            yyyymm=mm,
            elapsed_days=safe_int(row.get(c_elapsed, 0)) if c_elapsed else 0,
            total_days=safe_int(row.get(c_total, 0)) if c_total else 0,
            annual_elapsed_days=safe_int(row.get(c_ae, 0)) if c_ae else 0,
            annual_total_days=safe_int(row.get(c_at, 0)) if c_at else 0,
        ))
        if len(buf) >= BATCH: db.bulk_save_objects(buf); db.commit(); buf=[]
    if buf: db.bulk_save_objects(buf); db.commit()

def _load_ktoa(contents):
    """
    컬럼 구조 (row0 ffill + row1):
    date | SKT_KT | SKT_LGU+ | SKT_MVNO | SKT_SKT | SKT_계 |
          KT_SKT  | KT_LGU+  | KT_MVNO  | KT_KT   | KT_계  |
          LGU+_SKT| LGU+_KT  | LGU+_MVNO| LGU+_LGU+| LGU+_계|
          MVNO_SKT | MVNO_KT | MVNO_LGU+| MVNO_MVNO| MVNO_계| 합계

    해석: [사업자A]_[사업자B] = 원래 A사업자였던 사람이 B사업자로 이동한 수
    예) SKT_KT = 원래 SKT → KT로 이동 = KT유입(SKT로부터)
        KT_SKT  = 원래 KT → SKT로 이동 = KT이탈(SKT로)

    따라서:
    KT MNO유입 = SKT_KT + LGU+_KT (MVNO 제외)
    KT MNO이탈 = KT_SKT + KT_LGU+ (MVNO 제외)
    KT MNO순증 = (SKT_KT + LGU+_KT) - (KT_SKT + KT_LGU+)
    
    검증(21일): (1191+620) - (1248+714) = 1811-1962 = -151 ← 틀림
    실제 검증:
    KT_SKT=1191(row3 col6), SKT_KT=1248(row3 col1)
    KT_LGU+=714(row3 col7), LGU+_KT=620(row3 col12)
    KT유입(from SKT) = col1=SKT_KT? or col6=KT_SKT?
    
    row3=[날짜, 1248, 1618, 1368, 0, 4234, 1191, 714, 762, 0, 2667, 1657, 620, 800, 0, 3077, ...]
    col1=SKT_KT=1248, col6=KT_SKT=1191, col12=LGU+_KT=620, col7=KT_LGU+=714
    
    검증: KT순증=37이 되려면:
    (col6 + col12) - (col1 + col7) = (1191+620) - (1248+714) = 1811-1962 = -151 ✗
    (col1 + col12) - (col6 + col7) = (1248+620) - (1191+714) = 1868-1905 = -37 ✗
    -(col1 + col12) + (col6 + col7) = -1868+1905 = 37 ✓
    
    즉: KT순증(MNO) = (KT_SKT + KT_LGU+) - (SKT_KT + LGU+_KT)
                    = col6+col7 - col1-col12
                    = 1191+714 - 1248-620 = 1905-1868 = 37 ✓
    
    의미: KT_SKT = KT에서 나가서 SKT로 간 수? 아니면 SKT가 KT로 온 수?
    컬럼명 [행사업자]_[열사업자] = 행에서 열로 이동
    KT_SKT = KT→SKT 이동(KT이탈) = col6=1191
    SKT_KT = SKT→KT 이동(KT유입) = col1=1248
    결론: KT순증 = (SKT→KT + LGU→KT) - (KT→SKT + KT→LGU)
                = (1248+620) - (1191+714) = -37 ✗
    
    반대: KT순증 = (KT→SKT + KT→LGU) - (SKT→KT + LGU→KT)
                = (1191+714) - (1248+620) = 37 ✓
    
    최종 결론: KT_SKT(col6=1191)는 KT로 유입된 수(SKT에서 온),
               SKT_KT(col1=1248)는 KT에서 SKT로 이탈한 수
    즉 컬럼명이 [목적사업자]_[출발사업자] 형태임
    KT_SKT = KT로 이동(출발=SKT) → KT유입 from SKT
    SKT_KT = SKT로 이동(출발=KT) → KT이탈 to SKT
    
    KT MNO유입 = KT_SKT(col6) + KT_LGU+(col7) = 1191+714 = 1905
    KT MNO이탈 = SKT_KT(col1) + LGU+_KT(col12) = 1248+620 = 1868 
    KT MNO순증 = 1905-1868 = 37 ✓
    
    MVNO포함:
    KT전체유입 = KT_SKT + KT_LGU+ + KT_MVNO(col8=762)
    KT전체이탈 = SKT_KT + LGU+_KT + MVNO_KT(col17=583)
    KT전체순증 = (1191+714+762) - (1248+620+583) = 2667-2451 = 216
    """
    global _ktoa_cache
    df_raw = pd.read_excel(io.BytesIO(contents), header=None)
    header0 = df_raw.iloc[0].ffill().tolist()
    header1 = df_raw.iloc[1].tolist()
    rows = df_raw.iloc[2:].copy()
    cols = []
    for h0, h1 in zip(header0, header1):
        if pd.notna(h1) and str(h1) not in ('nan', ''):
            cols.append(f"{h0}_{h1}")
        else:
            cols.append(str(h0))
    rows.columns = cols
    rows = rows.rename(columns={cols[0]: "date"})
    rows = rows[rows["date"].notna()].copy()
    rows["date"] = rows["date"].astype(str).str[:10]
    for c in rows.columns[1:]:
        rows[c] = pd.to_numeric(
        rows[c].astype(str)
            .str.replace(",", "").str.replace(" ", "")
            .str.replace("천", "000").str.strip(),
        errors="coerce"
    ).fillna(0).astype(int)
    rows = rows[rows["date"] != "일합계"].copy()
    rows = rows[rows["date"].str.match(r"\d{4}-\d{2}-\d{2}")].copy()
    rows = rows.sort_values("date").reset_index(drop=True)

    all_cols = list(rows.columns)

    # 컬럼 탐색: [목적]_[출발] 구조
    def fc(dest, src):
        """[목적사업자]_[출발사업자] 컬럼 찾기
        예) KT_SKT = KT로 이동(출발=SKT) → KT유입
            SKT_KT = SKT로 이동(출발=KT) → KT이탈
        """
        return next((c for c in all_cols if c.startswith(f"{dest}_") and src in c), None)

    records = []
    for _, row in rows.iterrows():
        def g(col): return int(row[col]) if col and col in row.index else 0

        # KT 유입: KT_SKT(col6=1191), KT_LGU+(col7=714), KT_MVNO(col8=762)
        kt_from_skt = g(fc("KT", "SKT"))
        kt_from_lgu = g(fc("KT", "LGU"))
        kt_from_mv  = g(fc("KT", "MVNO"))
        # KT 이탈: SKT_KT(col1=1248), LGU+_KT(col12=620), MVNO_KT(col17=583)
        skt_from_kt = g(fc("SKT", "KT"))
        lgu_from_kt = g(fc("LGU+", "KT"))
        mv_from_kt  = g(fc("MVNO", "KT"))

        skt_from_lgu = g(fc("SKT", "LGU"))
        skt_from_mv = g(fc("SKT", "MVNO"))
        lgu_from_skt = g(fc("LGU+", "SKT"))
        lgu_from_mv = g(fc("LGU+", "MVNO"))
        mv_from_skt = g(fc("MVNO", "SKT"))
        mv_from_lgu = g(fc("MVNO", "LGU"))

        # KTOA 파일은 [목적사업자]_[출발사업자] 구조다.
        # 2026-04-21 검증: KT=(1191+714)-(1248+620)=+37.
        kt_mno_in  = kt_from_skt + kt_from_lgu
        kt_mno_out = skt_from_kt + lgu_from_kt
        kt_mno_net = kt_mno_in - kt_mno_out

        skt_mno_in  = skt_from_kt + skt_from_lgu
        skt_mno_out = kt_from_skt + lgu_from_skt
        skt_mno_net = skt_mno_in - skt_mno_out

        lgu_mno_in  = lgu_from_kt + lgu_from_skt
        lgu_mno_out = kt_from_lgu + skt_from_lgu
        lgu_mno_net = lgu_mno_in - lgu_mno_out

        kt_mvno_net = kt_from_mv - mv_from_kt
        skt_mvno_net = skt_from_mv - mv_from_skt
        lgu_mvno_net = lgu_from_mv - mv_from_lgu

        kt_all_in  = kt_mno_in + kt_from_mv
        kt_all_out = kt_mno_out + mv_from_kt
        kt_all_net = kt_mno_net + kt_mvno_net

        skt_all_net = skt_mno_net + skt_mvno_net
        lgu_all_net = lgu_mno_net + lgu_mvno_net

        rec = {"date": str(row["date"])}
        for c in all_cols[1:]:
            rec[c] = int(row[c])
        # 계산값
        rec.update({
            "KT_유입MNO": kt_mno_in, "KT_이탈MNO": kt_mno_out, "KT_순증MNO": kt_mno_net,
            "KT_유입전체": kt_all_in, "KT_이탈전체": kt_all_out, "KT_순증전체": kt_all_net,
            "SKT_순증MNO": skt_mno_net, "LGU+_순증MNO": lgu_mno_net,
            "KT_순증MVNO": kt_mvno_net, "SKT_순증MVNO": skt_mvno_net, "LGU+_순증MVNO": lgu_mvno_net,
            "SKT_순증전체": skt_all_net, "LGU+_순증전체": lgu_all_net,
            # 개별 유입/이탈
            "KT←SKT": kt_from_skt, "KT←LGU": kt_from_lgu, "KT←MVNO": kt_from_mv,
            "SKT←KT": skt_from_kt, "LGU←KT": lgu_from_kt, "MVNO←KT": mv_from_kt,
            "SKT←LGU": skt_from_lgu, "LGU←SKT": lgu_from_skt,
            "SKT←MVNO": skt_from_mv, "LGU←MVNO": lgu_from_mv,
            "MVNO←SKT": mv_from_skt, "MVNO←LGU": mv_from_lgu,
        })
        records.append(rec)
    _ktoa_cache = records

@asynccontextmanager
async def lifespan(app_):
    db = SessionLocal()
    try:
        for fname, loader in [
            ("sales.xlsx", _load_sales), ("commission.xlsx", _load_commission),
            ("device.xlsx", _load_device), ("inventory.xlsx", _load_inventory),
            ("subscriber.xlsx", _load_subscriber), ("storesales.xlsx", _load_storesales),
            ("subsidy.xlsx", _load_subsidy), ("targets.xlsx", _load_targets),
            ("business_days.xlsx", _load_business_days),
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
    def add_col(conn, table, col_def):
        col_name = col_def.split()[0]
        try:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col_def}"))
            conn.commit()
        except Exception:
            try:
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col_def}"))
                conn.commit()
            except Exception:
                pass
    with engine.connect() as conn:
        add_col(conn, Sales.__tablename__, "foreigner VARCHAR DEFAULT ''")
        add_col(conn, Sales.__tablename__, "new_sale INTEGER DEFAULT 0")
        add_col(conn, Sales.__tablename__, "new010 INTEGER DEFAULT 0")
        add_col(conn, Sales.__tablename__, "new_arpu FLOAT DEFAULT 0")
        add_col(conn, Commission.__tablename__, "commission_policy_name VARCHAR DEFAULT ''")
        add_col(conn, DeviceSales.__tablename__, "new_sale INTEGER DEFAULT 0")
        add_col(conn, Subscriber.__tablename__, "agency_code VARCHAR DEFAULT ''")
        add_col(conn, Subscriber.__tablename__, "ref_date VARCHAR DEFAULT ''")
        add_col(conn, Subscriber.__tablename__, "sub_count INTEGER DEFAULT 0")

Base.metadata.create_all(bind=engine)
_migrate(engine)
app = FastAPI(title="KT 무선판매 전략 대시보드", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── Upload ────────────────────────────────────────────────────────
@app.post("/upload")
async def upload_sales(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        db = SessionLocal(); _load_sales(db, contents)
        return {"status":"성공","total_sales":int(db.query(func.sum(Sales.sale_count)).scalar() or 0)}
    except Exception as e: return {"status":"실패","error":str(e)}
    finally: db.close()

@app.post("/upload/commission")
async def upload_commission(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        db = SessionLocal(); _load_commission(db, contents)
        return {"status":"성공","total":int(db.query(func.sum(Commission.amount)).scalar() or 0)}
    except Exception as e: return {"status":"실패","error":str(e)}
    finally: db.close()

@app.post("/upload/device")
async def upload_device(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        db = SessionLocal(); _load_device(db, contents)
        return {"status":"성공","total":int(db.query(func.sum(DeviceSales.sale_count)).scalar() or 0)}
    except Exception as e: return {"status":"실패","error":str(e)}
    finally: db.close()

@app.post("/upload/inventory")
async def upload_inventory(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        db = SessionLocal(); _load_inventory(db, contents)
        return {"status":"성공"}
    except Exception as e: return {"status":"실패","error":str(e)}
    finally: db.close()

@app.post("/upload/subscriber")
async def upload_subscriber(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        db = SessionLocal(); _load_subscriber(db, contents)
        return {"status":"성공"}
    except Exception as e: return {"status":"실패","error":str(e)}
    finally: db.close()


@app.post("/upload/storesales")
async def upload_storesales(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        db = SessionLocal(); _load_storesales(db, contents)
        return {"status":"성공","rows":int(db.query(func.count(StoreSales.id)).scalar() or 0)}
    except Exception as e: return {"status":"실패","error":str(e)}
    finally: db.close()

@app.post("/upload/subsidy")
async def upload_subsidy(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        db = SessionLocal(); _load_subsidy(db, contents)
        return {"status":"성공","rows":int(db.query(func.count(CommonSubsidy.id)).scalar() or 0)}
    except Exception as e: return {"status":"실패","error":str(e)}
    finally: db.close()

@app.post("/upload/targets")
async def upload_targets(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        db = SessionLocal(); _load_targets(db, contents)
        return {"status":"성공","rows":int(db.query(func.count(SalesTarget.id)).scalar() or 0)}
    except Exception as e: return {"status":"실패","error":str(e)}
    finally: db.close()

@app.post("/upload/business-days")
async def upload_business_days(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        db = SessionLocal(); _load_business_days(db, contents)
        return {"status":"성공","rows":int(db.query(func.count(BusinessDay.id)).scalar() or 0)}
    except Exception as e: return {"status":"실패","error":str(e)}
    finally: db.close()

@app.post("/upload/ktoa")
async def upload_ktoa(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        _load_ktoa(contents)
        return {"status":"성공","rows":len(_ktoa_cache) if _ktoa_cache else 0}
    except Exception as e: return {"status":"실패","error":str(e)}

# ── Filters ───────────────────────────────────────────────────────
@app.get("/api/filters")
async def get_filters(
    bonbu_list: List[str] = Query(default=[]),
    team_list: List[str] = Query(default=[]),
):
    db = SessionLocal()
    try:
        bonbu_all = [r[0] for r in db.query(Sales.bonbu, func.sum(Sales.sale_count))
            .filter(Sales.bonbu!="",Sales.bonbu!="nan")
            .group_by(Sales.bonbu).having(func.sum(Sales.sale_count)>=MIN_BONBU)
            .order_by(Sales.bonbu).all()]
        tq = db.query(Sales.team).distinct().filter(Sales.team!="",Sales.team!="nan")
        if bonbu_list: tq = tq.filter(Sales.bonbu.in_(bonbu_list))
        team_all = [r[0] for r in tq.order_by(Sales.team).all()]
        aq = db.query(Sales.agency).distinct().filter(Sales.agency!="",Sales.agency!="nan")
        if bonbu_list: aq = aq.filter(Sales.bonbu.in_(bonbu_list))
        if team_list: aq = aq.filter(Sales.team.in_(team_list))
        agency_all = [r[0] for r in aq.order_by(Sales.agency).all()]
        channel_all = [r[0] for r in db.query(Sales.channel_sub).distinct()
            .filter(Sales.channel_sub!="",Sales.channel_sub!="nan")
            .order_by(Sales.channel_sub).all()]
        # 수수료 정책명 목록
        policy_all = [r[0] for r in db.query(Commission.commission_policy_name).distinct()
            .filter(Commission.commission_policy_name!="",Commission.commission_policy_name!="nan")
            .order_by(Commission.commission_policy_name).all() if r[0]]
        return {"bonbu_list":bonbu_all,"team_list":team_all,
                "agency_list":agency_all,"channel_list":channel_all,
                "policy_list":policy_all}
    finally: db.close()

# ── Drilldown ─────────────────────────────────────────────────────
@app.get("/api/drilldown")
async def get_drilldown(
    level: str = "bonbu",
    bonbu_list: List[str] = Query(default=[]),
    team_list: List[str] = Query(default=[]),
    channel_list: List[str] = Query(default=[]),
    agency: str = None,
):
    db = SessionLocal()
    try:
        def af(q):
            if bonbu_list: q = q.filter(Sales.bonbu.in_(bonbu_list))
            if team_list: q = q.filter(Sales.team.in_(team_list))
            if agency: q = q.filter(Sales.agency==agency)
            if channel_list: q = q.filter(Sales.channel_sub.in_(channel_list))
            return q

        if level == "team":
            rows = af(db.query(Sales.agency, func.sum(Sales.sale_count), func.sum(Sales.subscriber),
                func.sum(Sales.new_sub), func.sum(Sales.mnp), func.sum(Sales.mmnp), func.sum(Sales.vmnp),
                func.sum(Sales.churn), func.sum(Sales.premium_change), func.sum(Sales.revenue),
            )).filter(Sales.agency!="",Sales.agency!="nan")\
              .group_by(Sales.agency).order_by(func.sum(Sales.sale_count).desc()).limit(20).all()
        else:
            rows = af(db.query(Sales.team, func.sum(Sales.sale_count), func.sum(Sales.subscriber),
                func.sum(Sales.new_sub), func.sum(Sales.mnp), func.sum(Sales.mmnp), func.sum(Sales.vmnp),
                func.sum(Sales.churn), func.sum(Sales.premium_change), func.sum(Sales.revenue),
            )).filter(Sales.team!="",Sales.team!="nan")\
              .group_by(Sales.team).order_by(func.sum(Sales.sale_count).desc()).limit(25).all()

        items = []
        for r in rows:
            sub=int(r[2] or 0); rev=float(r[9] or 0); sale=int(r[1] or 0)
            items.append({"name":r[0],"sale":sale,"sub":sub,
                "new_sub":int(r[3] or 0),"mnp":int(r[4] or 0),
                "mmnp":int(r[5] or 0),"vmnp":int(r[6] or 0),
                "churn":int(r[7] or 0),"premium":int(r[8] or 0),
                "revenue":rev,"arpu":round(rev/sub) if sub>0 else 0,
                "net":int(r[3] or 0)-int(r[7] or 0)})
        return {"level":"agency" if level=="team" else "team","items":items}
    finally: db.close()

# ── Summary ───────────────────────────────────────────────────────
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
            if agency: q = q.filter(Sales.agency==agency)
            if channel_list: q = q.filter(Sales.channel_sub.in_(channel_list))
            return q

        base = af(db.query(Sales)); grand = db.query(Sales)
        def sc(q,col): return int(q.with_entities(func.sum(col)).scalar() or 0)
        total_rev = float(base.with_entities(func.sum(Sales.revenue)).scalar() or 0)
        total_sub = sc(base, Sales.subscriber)
        total_new_sale = sc(base, Sales.new_sale) or sc(base, Sales.new_sub)
        total_new010 = sc(base, Sales.new010)
        if total_new010 == 0 and total_new_sale > 0:
            total_new010 = max(0, total_new_sale - sc(base, Sales.mnp))
        total_new_arpu_num = float(base.with_entities(func.sum(Sales.new_arpu * Sales.new_sale)).scalar() or 0)
        totals = {
            "sale":sc(base,Sales.sale_count),"subscriber":total_sub,
            "new_sub":total_new_sale,"new_sale":total_new_sale,"n010":total_new010,"new010":total_new010,
            "new_arpu":round(total_new_arpu_num/total_new_sale) if total_new_sale>0 and total_new_arpu_num>0 else (round(total_rev/total_sub) if total_sub>0 else 0),
            "mnp":sc(base,Sales.mnp),
            "smnp":sc(base,Sales.smnp),"lmnp":sc(base,Sales.lmnp),
            "mmnp":sc(base,Sales.mmnp),"vmnp":sc(base,Sales.vmnp),
            "churn":sc(base,Sales.churn),"mnp_churn":sc(base,Sales.mnp_churn),
            "smnp_churn":sc(base,Sales.smnp_churn),"lmnp_churn":sc(base,Sales.lmnp_churn),
            "mmnp_churn":sc(base,Sales.mmnp_churn),"vmnp_churn":sc(base,Sales.vmnp_churn),
            "forced_churn":sc(base,Sales.forced_churn),"premium":sc(base,Sales.premium_change),
            "revenue":total_rev,"arpu":round(total_rev/total_sub) if total_sub>0 else 0,
        }
        grand_totals={"sale":sc(grand,Sales.sale_count),
                      "revenue":float(grand.with_entities(func.sum(Sales.revenue)).scalar() or 0)}

        def to_list(rows):
            return [{"name":r[0],"value":int(r[1] or 0)} for r in rows
                    if r[0] and r[0] not in ("nan","ㆍ값없음","")]

        bonbu_data = to_list(af(db.query(Sales.bonbu,func.sum(Sales.sale_count)))
            .group_by(Sales.bonbu).having(func.sum(Sales.sale_count)>=MIN_BONBU)
            .order_by(func.sum(Sales.sale_count).desc()).all())
        team_data = to_list(af(db.query(Sales.team,func.sum(Sales.sale_count)))
            .filter(Sales.team!="",Sales.team!="nan").group_by(Sales.team)
            .order_by(func.sum(Sales.sale_count).desc()).limit(20).all())
        channel_data = to_list(af(db.query(Sales.channel_sub,func.sum(Sales.sale_count)))
            .filter(Sales.channel_sub!="",Sales.channel_sub!="nan")
            .group_by(Sales.channel_sub).order_by(func.sum(Sales.sale_count).desc()).all())
        type_data = to_list(af(db.query(Sales.sale_type,func.sum(Sales.sale_count)))
            .filter(Sales.sale_type!="",Sales.sale_type!="nan").group_by(Sales.sale_type).all())
        kids_data = to_list(af(db.query(Sales.kids,func.sum(Sales.sale_count)))
            .filter(Sales.kids!="",Sales.kids!="nan").group_by(Sales.kids).all())
        k110_data = to_list(af(db.query(Sales.k110,func.sum(Sales.sale_count)))
            .filter(Sales.k110.in_(["초이스","초이스外"])).group_by(Sales.k110).all())
        foreigner_data = to_list(af(db.query(Sales.foreigner,func.sum(Sales.sale_count)))
            .filter(Sales.foreigner!="",Sales.foreigner!="nan").group_by(Sales.foreigner).all())

        # 본부별 상세
        bonbu_detail = []
        for r in af(db.query(
            Sales.bonbu,func.sum(Sales.sale_count),func.sum(Sales.subscriber),
            func.sum(Sales.new_sub),func.sum(Sales.mnp),
            func.sum(Sales.smnp),func.sum(Sales.lmnp),
            func.sum(Sales.mmnp),func.sum(Sales.vmnp),
            func.sum(Sales.churn),func.sum(Sales.mnp_churn),
            func.sum(Sales.mmnp_churn),func.sum(Sales.vmnp_churn),
            func.sum(Sales.premium_change),func.sum(Sales.revenue),
            func.count(func.distinct(Sales.agency)), func.sum(Sales.new_sale), func.sum(Sales.new010),
            func.sum(Sales.new_arpu * Sales.new_sale),
        )).filter(Sales.bonbu!="",Sales.bonbu!="nan").group_by(Sales.bonbu)\
          .having(func.sum(Sales.sale_count)>=MIN_BONBU)\
          .order_by(func.sum(Sales.sale_count).desc()).all():
            sale=int(r[1] or 0); sub=int(r[2] or 0); rev=float(r[14] or 0); nm=r[0]
            new_s=int(r[16] or 0) or int(r[3] or 0); n010_s=int(r[17] or 0) or max(0, new_s-int(r[4] or 0)); churn_s=int(r[9] or 0)
            new_arpu_num=float(r[18] or 0)
            used_cnt    = int(af(db.query(func.sum(Sales.sale_count))).filter(Sales.bonbu==nm,Sales.sale_type.like("%중고%")).scalar() or 0)
            kids_cnt    = int(af(db.query(func.sum(Sales.sale_count))).filter(Sales.bonbu==nm,Sales.kids=="키즈").scalar() or 0)
            foreign_cnt = int(af(db.query(func.sum(Sales.sale_count))).filter(Sales.bonbu==nm,Sales.foreigner=="외국인").scalar() or 0)
            k110_cnt    = int(af(db.query(func.sum(Sales.sale_count))).filter(Sales.bonbu==nm,Sales.k110=="초이스").scalar() or 0)
            bonbu_detail.append({
                "name":nm,"sale":sale,"sub":sub,
                "new_sub":new_s,"new_sale":new_s,"n010":n010_s,"new010":n010_s,
                "new_arpu":round(new_arpu_num/new_s) if new_s>0 and new_arpu_num>0 else (round(rev/sub) if sub>0 else 0),
                "mnp":int(r[4] or 0),
                "smnp":int(r[5] or 0),"lmnp":int(r[6] or 0),
                "mmnp":int(r[7] or 0),"vmnp":int(r[8] or 0),
                "churn":churn_s,"mnp_churn":int(r[10] or 0),
                "mmnp_churn":int(r[11] or 0),"vmnp_churn":int(r[12] or 0),
                "premium":int(r[13] or 0),"revenue":rev,
                "arpu":round(rev/sub) if sub>0 else 0,
                "agency_count":int(r[15] or 0),
                "net":new_s-churn_s,
                "used_cnt":used_cnt,"kids_cnt":kids_cnt,
                "foreign_cnt":foreign_cnt,"k110_cnt":k110_cnt,
            })

        # 채널별 상세
        channel_detail = []
        for r in af(db.query(
            Sales.channel_sub,func.sum(Sales.sale_count),func.sum(Sales.subscriber),
            func.sum(Sales.new_sub),func.sum(Sales.mnp),
            func.sum(Sales.mmnp),func.sum(Sales.vmnp),
            func.sum(Sales.churn),func.sum(Sales.premium_change),func.sum(Sales.revenue),
            func.sum(Sales.new_sale),func.sum(Sales.new010),func.sum(Sales.new_arpu * Sales.new_sale),
        )).filter(Sales.channel_sub!="",Sales.channel_sub!="nan")\
          .group_by(Sales.channel_sub).order_by(func.sum(Sales.sale_count).desc()).all():
            sale=int(r[1] or 0); sub=int(r[2] or 0); rev=float(r[9] or 0); nm=r[0]
            new_s=int(r[10] or 0) or int(r[3] or 0); n010_s=int(r[11] or 0) or max(0, new_s-int(r[4] or 0)); new_arpu_num=float(r[12] or 0)
            normal=int(af(db.query(func.sum(Sales.sale_count))).filter(Sales.channel_sub==nm,Sales.sale_type.like("%일반%")).scalar() or 0)
            used  =int(af(db.query(func.sum(Sales.sale_count))).filter(Sales.channel_sub==nm,Sales.sale_type.like("%중고%")).scalar() or 0)
            channel_detail.append({
                "name":nm,"sale":sale,"sub":sub,
                "new_sub":new_s,"new_sale":new_s,"n010":n010_s,"new010":n010_s,
                "new_arpu":round(new_arpu_num/new_s) if new_s>0 and new_arpu_num>0 else (round(rev/sub) if sub>0 else 0),
                "mnp":int(r[4] or 0),
                "mmnp":int(r[5] or 0),"vmnp":int(r[6] or 0),
                "churn":int(r[7] or 0),"premium":int(r[8] or 0),
                "revenue":rev,"arpu":round(rev/sub) if sub>0 else 0,
                "normal":normal,"used":used,
                "net":new_s-int(r[7] or 0),
            })

        agency_detail = []
        for r in af(db.query(
            Sales.agency,Sales.bonbu,
            func.sum(Sales.sale_count),func.sum(Sales.subscriber),
            func.sum(Sales.new_sub),func.sum(Sales.mnp),
            func.sum(Sales.mmnp),func.sum(Sales.vmnp),
            func.sum(Sales.premium_change),func.sum(Sales.churn),func.sum(Sales.revenue),
            func.sum(Sales.new_sale),func.sum(Sales.new010),func.sum(Sales.new_arpu * Sales.new_sale),
        )).filter(Sales.agency!="",Sales.agency!="nan")\
          .group_by(Sales.agency,Sales.bonbu)\
          .order_by(func.sum(Sales.sale_count).desc()).limit(30).all():
            sub=int(r[3] or 0); rev=float(r[10] or 0)
            new_s=int(r[11] or 0) or int(r[4] or 0); n010_s=int(r[12] or 0) or max(0, new_s-int(r[5] or 0)); new_arpu_num=float(r[13] or 0)
            agency_detail.append({
                "name":r[0],"bonbu":r[1],"sale":int(r[2] or 0),"sub":sub,
                "new_sub":new_s,"new_sale":new_s,"n010":n010_s,"new010":n010_s,
                "new_arpu":round(new_arpu_num/new_s) if new_s>0 and new_arpu_num>0 else (round(rev/sub) if sub>0 else 0),
                "mnp":int(r[5] or 0),
                "mmnp":int(r[6] or 0),"vmnp":int(r[7] or 0),
                "premium":int(r[8] or 0),"churn":int(r[9] or 0),
                "revenue":rev,"arpu":round(rev/sub) if sub>0 else 0,
                "net":new_s-int(r[9] or 0),
            })

        all_agency = [{"name":r[0],"value":int(r[1] or 0)}
            for r in af(db.query(Sales.agency,func.sum(Sales.sale_count)))
            .filter(Sales.agency!="",Sales.agency!="nan")
            .group_by(Sales.agency).order_by(func.sum(Sales.sale_count).desc()).all()]
        cumsum=pareto_count=0
        for a in all_agency:
            cumsum+=a["value"]; pareto_count+=1
            if totals["sale"]>0 and cumsum>=totals["sale"]*0.8: break


        # 담당·지사별 상세 (초이스 비중 포함)
        team_detail = []
        for r in af(db.query(
            Sales.team, func.sum(Sales.sale_count), func.sum(Sales.subscriber),
            func.sum(Sales.new_sub), func.sum(Sales.mnp),
            func.sum(Sales.mmnp), func.sum(Sales.vmnp),
            func.sum(Sales.churn), func.sum(Sales.premium_change),
            func.sum(Sales.revenue), func.sum(Sales.new_sale), func.sum(Sales.new010),
            func.sum(Sales.new_arpu * Sales.new_sale),
        )).filter(Sales.team!="",Sales.team!="nan").group_by(Sales.team)          .order_by(func.sum(Sales.sale_count).desc()).all():
            nm=r[0]; sale=int(r[1] or 0); sub=int(r[2] or 0); rev=float(r[9] or 0)
            new_s=int(r[10] or 0) or int(r[3] or 0); n010_s=int(r[11] or 0) or max(0, new_s-int(r[4] or 0)); churn_s=int(r[7] or 0)
            new_arpu_num=float(r[12] or 0)
            choice_cnt = int(af(db.query(func.sum(Sales.sale_count))).filter(Sales.team==nm,Sales.k110=="초이스").scalar() or 0)
            team_detail.append({
                "name":nm,"sale":sale,"sub":sub,
                "new_sub":new_s,"new_sale":new_s,"n010":n010_s,"new010":n010_s,
                "new_arpu":round(new_arpu_num/new_s) if new_s>0 and new_arpu_num>0 else (round(rev/sub) if sub>0 else 0),
                "mnp":int(r[4] or 0),
                "mmnp":int(r[5] or 0),"vmnp":int(r[6] or 0),
                "churn":churn_s,"premium":int(r[8] or 0),
                "revenue":rev,"arpu":round(rev/sub) if sub>0 else 0,
                "net":new_s-churn_s,"choice_cnt":choice_cnt,
            })

        mnp_detail={"smnp":totals["smnp"],"lmnp":totals["lmnp"],
            "mmnp_in":totals["mmnp"],"vmnp":totals["vmnp"],
            "smnp_out":totals["smnp_churn"],"lmnp_out":totals["lmnp_churn"],
            "mmnp_out":totals["mmnp_churn"],"vmnp_out":totals["vmnp_churn"]}

        latest_date = db.query(func.max(Subscriber.ref_date)).scalar() or ""
        bonbu_sub_live = {}
        if latest_date:
            for r in db.query(Subscriber.bonbu,func.sum(Subscriber.sub_count))\
                    .filter(Subscriber.ref_date==latest_date,Subscriber.bonbu!="")\
                    .group_by(Subscriber.bonbu).all():
                bonbu_sub_live[r[0]]=int(r[1] or 0)
        for b in bonbu_detail:
            live=bonbu_sub_live.get(b["name"],0)
            b["live_sub"]=live
            b["penetration"]=round(b["sale"]/live*100,2) if live>0 else 0

        # 단말
        all_months=[r[0] for r in db.query(DeviceSales.yyyymm).distinct()
            .filter(DeviceSales.yyyymm!="",DeviceSales.yyyymm!="nan")
            .order_by(DeviceSales.yyyymm.desc()).limit(2).all()]
        cur_mm=all_months[0] if all_months else ""
        prev_mm=all_months[1] if len(all_months)>1 else ""

        def dev_by_mm(mm):
            if not mm: return {}
            return {r[0]:{"sale":int(r[1] or 0),"new_sale":int(r[2] or 0)} for r in
                db.query(DeviceSales.model_name,func.sum(DeviceSales.sale_count),func.sum(DeviceSales.new_sale))
                .filter(DeviceSales.yyyymm==mm,DeviceSales.model_name!="",
                        DeviceSales.model_name!="nan",DeviceSales.model_name!="ㆍ값없음")
                .group_by(DeviceSales.model_name).all()}

        cur_model=dev_by_mm(cur_mm); prev_model=dev_by_mm(prev_mm)
        device_cur =sorted([{"name":k,"value":v["sale"],"new_sale":v.get("new_sale",0)} for k,v in cur_model.items() if v["sale"]>0],key=lambda x:-x["value"])[:30]
        device_prev=sorted([{"name":k,"value":v["sale"],"new_sale":v.get("new_sale",0)} for k,v in prev_model.items() if v["sale"]>0],key=lambda x:-x["value"])[:30]

        WORKING_DAYS=21
        inv_data=[]
        for r in db.query(Inventory.model_name,Inventory.total,Inventory.jisa,
                          Inventory.youngi,Inventory.strategy,Inventory.mns,Inventory.ktshop).all():
            if not r[0] or r[0] in ("","nan","ㆍ값없음"): continue
            cs=cur_model.get(r[0],{}).get("sale",0); ps=prev_model.get(r[0],{}).get("sale",0)
            # 일평균: 현월 판매량 기준, 없으면 전월 기준
            da=round(cs/WORKING_DAYS,1) if cs>0 else (round(ps/WORKING_DAYS,1) if ps>0 else 0)
            dl=round(r[1]/da) if da>0 else None
            mom=round((cs-ps)/ps*100,1) if ps>0 else None
            # 재고일수 이상값 제거
            dl=min(dl,999) if dl is not None else None
            inv_data.append({"model":r[0],"inventory":int(r[1]),
                "jisa":int(r[2]),"youngi":int(r[3]),"strategy":int(r[4]),
                "mns":int(r[5]),"ktshop":int(r[6]),
                "cur_sale":cs,"prev_sale":ps,"daily_avg":da,"days_left":dl,"mom":mom})
        # 판매는 있지만 재고 없는 단말도 추가 (재고 소진)
        inv_models = {d["model"] for d in inv_data}
        for mn, cv in cur_model.items():
            cs = cv.get("sale",0)
            if cs > 0 and mn not in inv_models:
                ps = prev_model.get(mn, {}).get("sale", 0)
                mom = round((cs-ps)/ps*100,1) if ps>0 else None
                da = round(cs/WORKING_DAYS,1)
                inv_data.append({"model":mn,"inventory":0,
                    "jisa":0,"youngi":0,"strategy":0,"mns":0,"ktshop":0,
                    "cur_sale":cs,"prev_sale":ps,"daily_avg":da,"days_left":0,"mom":mom})
        inv_data.sort(key=lambda x:-(x["cur_sale"]+x["inventory"]))

        # 수수료
        comm_by_ag={r[0]:float(r[1] or 0) for r in
            db.query(Commission.agency_name,func.sum(Commission.amount))
            .filter(Commission.agency_name!="",Commission.agency_name!="nan")
            .group_by(Commission.agency_name).all()}
        comm_linked=[]
        for s in db.query(Sales.agency,func.sum(Sales.sale_count),
                          func.sum(Sales.revenue),func.sum(Sales.subscriber))\
                .filter(Sales.agency!="",Sales.agency!="nan").group_by(Sales.agency).all():
            if s[0] in comm_by_ag:
                sale=int(s[1] or 0); comm=comm_by_ag[s[0]]; rev=float(s[2] or 0); sub=int(s[3] or 0)
                comm_linked.append({"name":s[0],"sale":sale,"commission":comm,"revenue":rev,
                    "arpu":round(rev/sub) if sub>0 else 0,
                    "comm_per_sale":round(comm/sale) if sale>0 else 0,
                    "roi":round(rev/comm*100,1) if comm>0 else 0})
        comm_linked.sort(key=lambda x:-x["commission"])
        total_comm=float(db.query(func.sum(Commission.amount)).scalar() or 0)

        comm_by_item=[{"code":r[0],"amount":float(r[1] or 0)}
            for r in db.query(Commission.item_code,func.sum(Commission.amount))
            .filter(Commission.item_code!="",Commission.item_code!="nan")
            .group_by(Commission.item_code).order_by(func.sum(Commission.amount).desc()).all()]

        # 정책명별 집계 (정책명 + 코드 + 채널 + 금액)
        comm_by_policy=[{
            "policy_name":r[0],"policy_code":r[1],"channel":r[2],"amount":float(r[3] or 0)}
            for r in db.query(Commission.commission_policy_name,Commission.commission_policy,
                              Commission.channel_type,func.sum(Commission.amount))
            .filter(Commission.commission_policy_name!="",Commission.commission_policy_name!="nan",
                    Commission.amount>0)
            .group_by(Commission.commission_policy_name,Commission.commission_policy,Commission.channel_type)
            .order_by(func.sum(Commission.amount).desc()).limit(25).all()]

        comm_by_channel=[{"name":r[0],"amount":float(r[1] or 0)}
            for r in db.query(Commission.channel_type,func.sum(Commission.amount))
            .filter(Commission.channel_type!="",Commission.channel_type!="nan",
                    Commission.channel_type!="ㆍ값없음")
            .group_by(Commission.channel_type).order_by(func.sum(Commission.amount).desc()).all()]

        return {
            "totals":totals,"grand_totals":grand_totals,
            "bonbu":bonbu_data,"team":team_data,"channel":channel_data,
            "sale_type":type_data,"kids":kids_data,"k110":k110_data,"foreigner":foreigner_data,
            "bonbu_detail":bonbu_detail,"channel_detail":channel_detail,
            "agency_detail":agency_detail,"mnp_detail":mnp_detail,"team_detail":team_detail,
            "pareto_80_count":pareto_count,"agency_total_count":len(all_agency),
            "latest_sub_date":latest_date,
            "device_cur":device_cur,"device_prev":device_prev,
            "cur_mm":cur_mm,"prev_mm":prev_mm,"inv_data":inv_data,
            "comm_linked":comm_linked[:20],"comm_total":total_comm,
            "comm_by_item":comm_by_item,"comm_by_policy":comm_by_policy,
            "comm_by_channel":comm_by_channel,
        }
    finally: db.close()

# 수수료 정책별 필터
@app.get("/api/commission")
async def get_commission(
    bonbu_list: List[str] = Query(default=[]),
    team_list: List[str] = Query(default=[]),
    policy_list: List[str] = Query(default=[]),
    channel_list: List[str] = Query(default=[]),
):
    db = SessionLocal()
    try:
        q = db.query(Commission.commission_policy_name, Commission.commission_policy,
                     Commission.channel_type, Commission.item_code, Commission.agency_name,
                     func.sum(Commission.amount))
        if bonbu_list: q = q.filter(Commission.jisa_name.in_(bonbu_list))
        if policy_list: q = q.filter(Commission.commission_policy_name.in_(policy_list))
        if channel_list: q = q.filter(Commission.channel_type.in_(channel_list))
        rows = q.filter(Commission.amount>0)\
            .group_by(Commission.commission_policy_name,Commission.commission_policy,
                      Commission.channel_type,Commission.item_code,Commission.agency_name)\
            .order_by(func.sum(Commission.amount).desc()).limit(50).all()
        items=[]
        for r in rows:
            cls=classify_commission_policy(r[0],r[3])
            items.append({"policy_name":r[0],"policy_code":r[1],"channel":r[2],"item":r[3],
                          "agency":r[4],"amount":float(r[5] or 0),
                          "series":cls["series"],"channel_cls":cls["channel_cls"],
                          "policy_type":cls["policy_type"],"item_type":cls["item_type"]})
        total=float(db.query(func.sum(Commission.amount)).filter(Commission.amount>0).scalar() or 0)
        return {"items":items,"total":total}
    finally: db.close()


# ── Commission 실무분류 함수 ─────────────────────────────────────
import re as _re

def classify_commission_policy(policy_name: str, item_code: str) -> dict:
    """
    정책명에서 MRA/MWA/MBA 코드 파싱 → 실무 분류
    MRA = 소매(Retail Agency)
    MWA = 도매/온라인(Wholesale Agency)
    MBA = 공통기본(Basic common)
    MPA = 판매촉진(Promotion)
    MRN = 지역특별(Regional)
    코드번호: 00=인프라, 01=기본정책, 02=돈버는모델/매출성장, 03=STORAGE/2ndDevice,
              04=활력/시장대응, 05=장기고객, 06~=기타
    """
    nm = policy_name or ""
    # 코드 추출
    m = _re.search(r'(MRA|MWA|MBA|MPA|MRN|MZC)-(\d{2})', nm)
    series = m.group(1) if m else None
    num = int(m.group(2)) if m else None
    # 채널 분류
    if series in ("MRA",):
        channel_cls = "소매"
    elif series in ("MWA",):
        channel_cls = "도매/온라인"
    elif series in ("MBA", "MZC"):
        channel_cls = "공통"
    elif series in ("MPA", "MRN"):
        channel_cls = "특별/촉진"
    else:
        channel_cls = "기타"
    # 정책 유형 분류
    if num is None:
        policy_type = "기타"
    elif num == 0:
        policy_type = "인프라(기본수수료)"
    elif num == 1:
        policy_type = "기본정책"
    elif num == 2:
        policy_type = "매출성장(돈버는모델)"
    elif num == 3:
        policy_type = "STORAGE/2nd Device"
    elif num == 4:
        policy_type = "활력/시장대응"
    elif num == 5:
        policy_type = "장기고객(기변)"
    elif num == 6:
        policy_type = "부가서비스활성화"
    elif num == 7:
        policy_type = "Pre-Sales/신모델"
    elif num == 8:
        policy_type = "신모델 SCM"
    elif num == 9:
        policy_type = "목표달성/시상"
    else:
        policy_type = f"기타({num:02d})"
    # 항목유형
    item_type = {"F300":"활성화","F420":"유지","F432":"부가서비스"}.get(item_code, item_code or "기타")
    return {"series": series or "기타", "channel_cls": channel_cls,
            "policy_type": policy_type, "item_type": item_type}

# ── Subscriber Analysis ───────────────────────────────────────────
@app.get("/api/subscriber")
async def get_subscriber_analysis(
    bonbu_list: List[str] = Query(default=[]),
    team_list: List[str] = Query(default=[]),
):
    db = SessionLocal()
    try:
        # 전체 날짜 목록 (최근 60일)
        all_dates = [r[0] for r in db.query(Subscriber.ref_date).distinct()
            .filter(Subscriber.ref_date!="").order_by(Subscriber.ref_date.desc()).limit(60).all()]
        if not all_dates:
            return {"dates":[],"bonbu_trend":[],"total_trend":[]}

        def sf(q):
            if bonbu_list: q = q.filter(Subscriber.bonbu.in_(bonbu_list))
            if team_list: q = q.filter(Subscriber.team.in_(team_list))
            return q

        latest = all_dates[0]
        prev   = all_dates[1] if len(all_dates) > 1 else latest
        # 본부별 최신 가입자 수
        bonbu_latest = {r[0]: int(r[1] or 0) for r in
            sf(db.query(Subscriber.bonbu, func.sum(Subscriber.sub_count)))
            .filter(Subscriber.ref_date==latest, Subscriber.bonbu!="")
            .group_by(Subscriber.bonbu).all()}
        bonbu_prev = {r[0]: int(r[1] or 0) for r in
            sf(db.query(Subscriber.bonbu, func.sum(Subscriber.sub_count)))
            .filter(Subscriber.ref_date==prev, Subscriber.bonbu!="")
            .group_by(Subscriber.bonbu).all()}
        bonbu_list_data = []
        for nm, cnt in sorted(bonbu_latest.items(), key=lambda x:-x[1]):
            pv = bonbu_prev.get(nm, 0)
            bonbu_list_data.append({
                "name": nm, "count": cnt, "prev": pv,
                "change": cnt - pv,
                "change_pct": round((cnt-pv)/pv*100, 2) if pv>0 else 0,
            })
        # 전체 추이 (날짜별)
        dates_asc = sorted(all_dates)
        total_trend = []
        for dt in dates_asc:
            total = int(sf(db.query(func.sum(Subscriber.sub_count)))
                .filter(Subscriber.ref_date==dt).scalar() or 0)
            total_trend.append({"date": dt, "total": total})

        return {
            "latest_date": latest,
            "prev_date": prev,
            "bonbu": bonbu_list_data,
            "total_trend": total_trend,
            "total_latest": sum(bonbu_latest.values()),
            "total_prev": sum(bonbu_prev.values()),
        }
    finally:
        db.close()


# ── Device Hierarchy (4-level drilldown) ─────────────────────────
@app.get("/api/device/hierarchy")
async def get_device_hierarchy(
    bonbu_list: List[str] = Query(default=[]),
    level: str = "l1",
    parent: str = None,
):
    db = SessionLocal()
    try:
        all_months = [r[0] for r in db.query(DeviceSales.yyyymm).distinct()
            .filter(DeviceSales.yyyymm!="",DeviceSales.yyyymm!="nan")
            .order_by(DeviceSales.yyyymm.desc()).limit(2).all()]
        cur_mm = all_months[0] if all_months else ""
        prev_mm = all_months[1] if len(all_months)>1 else ""
        if not cur_mm:
            return {"items":[],"cur_mm":"","prev_mm":""}

        def af(q):
            if bonbu_list: q = q.filter(DeviceSales.bonbu.in_(bonbu_list))
            return q

        # model_name 구조: 예) SM-S962NK256BK (대표+세부+용량+색상 합쳐진 경우 많음)
        # L1: 대표단말 (model_code 앞 3~4자 or 별도 분류)
        # 단순하게: model_name을 그대로 쓰되, level별로 필터
        def get_sales(mm, name_filter=None):
            q = af(db.query(DeviceSales.model_name, func.sum(DeviceSales.sale_count)))
            q = q.filter(DeviceSales.yyyymm==mm, DeviceSales.model_name!="",
                         DeviceSales.model_name!="nan", DeviceSales.model_name!="ㆍ값없음")
            if name_filter:
                q = q.filter(DeviceSales.model_name.like(f"%{name_filter}%"))
            return {r[0]: int(r[1] or 0) for r in q.group_by(DeviceSales.model_name).all()}

        cur = get_sales(cur_mm, parent)
        prev_d = get_sales(prev_mm, parent)
        items = []
        for nm, cnt in sorted(cur.items(), key=lambda x:-x[1]):
            pv = prev_d.get(nm, 0)
            items.append({
                "name": nm, "cur": cnt, "prev": pv,
                "mom": round((cnt-pv)/pv*100,1) if pv>0 else None
            })
        return {"items": items[:30], "cur_mm": cur_mm, "prev_mm": prev_mm}
    finally:
        db.close()


@app.get("/api/store-sales")
async def get_store_sales(
    view: str = "store",
    bonbu_list: List[str] = Query(default=[]),
    team_list: List[str] = Query(default=[]),
    agency: str = None,
    channel_list: List[str] = Query(default=[]),
    limit: int = 50,
):
    db = SessionLocal()
    try:
        col = StoreSales.store
        if view == "bonbu": col = StoreSales.bonbu
        elif view == "team": col = StoreSales.team
        elif view == "agency": col = StoreSales.agency
        elif view == "contact": col = StoreSales.contact
        q = db.query(col, func.sum(StoreSales.sale), func.sum(StoreSales.new_sale),
                     func.sum(StoreSales.new010), func.sum(StoreSales.mnp),
                     func.sum(StoreSales.premium), func.sum(StoreSales.churn),
                     func.sum(StoreSales.revenue), func.avg(StoreSales.arpu))
        if bonbu_list: q = q.filter(StoreSales.bonbu.in_(bonbu_list))
        if team_list: q = q.filter(StoreSales.team.in_(team_list))
        if agency: q = q.filter(StoreSales.agency==agency)
        if channel_list: q = q.filter(StoreSales.channel.in_(channel_list))
        rows = q.filter(col!="", col!="nan").group_by(col).order_by(func.sum(StoreSales.sale).desc()).limit(limit).all()
        items=[]
        for r in rows:
            sale=int(r[1] or 0); new_sale=int(r[2] or 0); churn=int(r[6] or 0); rev=float(r[7] or 0)
            items.append({"name":r[0],"sale":sale,"new_sale":new_sale,"new_sub":new_sale,
                          "n010":int(r[3] or 0),"new010":int(r[3] or 0),"mnp":int(r[4] or 0),
                          "premium":int(r[5] or 0),"churn":churn,"net":new_sale-churn,
                          "revenue":rev,"arpu":round(float(r[8] or 0))})
        totals={"sale":sum(i["sale"] for i in items),"new_sale":sum(i["new_sale"] for i in items),
                "n010":sum(i["n010"] for i in items),"mnp":sum(i["mnp"] for i in items),
                "premium":sum(i["premium"] for i in items),"churn":sum(i["churn"] for i in items)}
        totals["net"] = totals["new_sale"] - totals["churn"]
        return {"view":view,"items":items,"totals":totals}
    finally:
        db.close()

@app.get("/api/subsidy")
async def get_subsidy(model: str = None, carrier: str = None, limit: int = 200):
    db = SessionLocal()
    try:
        q = db.query(CommonSubsidy)
        if model: q = q.filter(CommonSubsidy.model_name.like(f"%{model}%"))
        if carrier: q = q.filter(CommonSubsidy.carrier==carrier)
        rows = q.order_by(CommonSubsidy.ref_date.desc(), CommonSubsidy.amount.desc()).limit(limit).all()
        return {"items":[{"date":r.ref_date,"model":r.model_name,"code":r.model_code,
                           "carrier":r.carrier,"join_type":r.join_type,"channel":r.channel,
                           "plan_group":r.plan_group,"amount":r.amount} for r in rows]}
    finally:
        db.close()

@app.get("/api/forecast")
async def get_forecast(yyyymm: str = None):
    db = SessionLocal()
    try:
        if not yyyymm:
            yyyymm = db.query(func.max(BusinessDay.yyyymm)).scalar() or ""
        bd = db.query(BusinessDay).filter(BusinessDay.yyyymm==yyyymm).first()
        elapsed = bd.elapsed_days if bd and bd.elapsed_days else 0
        total = bd.total_days if bd and bd.total_days else 0
        cur_sale = int(db.query(func.sum(Sales.sale_count)).scalar() or 0)
        forecast = round(cur_sale / elapsed * total) if elapsed > 0 and total > 0 else cur_sale
        return {"yyyymm":yyyymm,"elapsed_days":elapsed,"total_days":total,
                "current_sale":cur_sale,"month_forecast_sale":forecast}
    finally:
        db.close()

@app.get("/api/ktoa")
async def get_ktoa():
    if not _ktoa_cache: return {"rows":[],"columns":[]}
    return {"rows":_ktoa_cache,"columns":list(_ktoa_cache[0].keys())}

@app.get("/",response_class=HTMLResponse)
async def dashboard():
    with open(os.path.join(os.path.dirname(__file__),"templates","index.html"),encoding="utf-8") as f:
        return f.read()
