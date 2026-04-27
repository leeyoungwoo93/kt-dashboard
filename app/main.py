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
APP_VERSION = "v4-retail-monthly-deploy-safe"
AUTOLOAD_EXCEL = os.environ.get("AUTOLOAD_EXCEL", "0").lower() in ("1", "true", "yes", "on")



# ── Dynamic columns / auxiliary tables ────────────────────────────
def _attach_column(model, attr, coltype, default=None):
    if not hasattr(model, attr):
        setattr(model, attr, Column(attr, coltype, default=default))

_attach_column(Sales, "yyyymm", String, "")     # 판매 년월 (YYYYMM 형식)
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


def _clean_text(v):
    """엑셀 셀 값을 화면 표시용 문자열로 안전하게 정리한다."""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    txt = str(v).strip()
    if txt.lower() in ("nan", "none", "nat"):
        return ""
    if txt.endswith(".0") and txt[:-2].isdigit():
        return txt[:-2]
    return txt


def _norm_month(v):
    """202601, 2026-01, Timestamp 등 다양한 월 표기를 YYYYMM으로 정규화한다."""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    if hasattr(v, "strftime"):
        try:
            return v.strftime("%Y%m")
        except Exception:
            pass
    txt = str(v).strip()
    if txt.lower() in ("", "nan", "none", "nat"):
        return ""
    if txt.endswith(".0"):
        txt = txt[:-2]
    digits = "".join(ch for ch in txt if ch.isdigit())
    if len(digits) >= 8:
        return digits[:6]
    if len(digits) >= 6:
        return digits[:6]
    return ""


def _next_col(df, col):
    """코드/명칭이 나란히 있는 엑셀에서 기준 컬럼의 다음 컬럼명을 반환한다."""
    if not col:
        return None
    cols = list(df.columns)
    try:
        i = cols.index(col)
        return cols[i + 1] if i + 1 < len(cols) else None
    except ValueError:
        return None


def _prefer_name(row, code_col=None, name_col=None):
    """조직 코드 옆 명칭 컬럼이 있으면 명칭을 우선 사용한다."""
    name = _clean_text(row.get(name_col, "")) if name_col else ""
    code = _clean_text(row.get(code_col, "")) if code_col else ""
    return name or code

def _load_sales(db, contents):
    """
    판매 파일 로더.
    기존 고정 컬럼 번호 방식은 파일에 컬럼이 하나만 추가되어도 '순증'을 '신규'로 읽는 문제가 있었다.
    먼저 헤더명을 기준으로 컬럼을 찾고, 실패할 때만 과거 고정 인덱스 fallback을 사용한다.
    """
    import re as _re

    def _clean_name(v):
        if pd.isna(v): return ""
        return _re.sub(r"\s+", "", str(v).replace("\n", " ").strip())

    def _read_sales_frame():
        raw = pd.read_excel(io.BytesIO(contents), header=None)
        if raw.empty:
            return pd.DataFrame(), False
        terms = ["본부", "담당", "대리점", "판매", "신규", "MNP", "기변", "해지", "ARPU"]
        best = (0, -1, None, 1)  # row, score, columns, start_row
        max_rows = min(15, len(raw))
        for i in range(max_rows):
            row = [_clean_name(v) for v in raw.iloc[i].tolist()]
            score = sum(1 for t in terms if any(t in c for c in row))
            if score > best[1]:
                best = (i, score, row, i + 1)
            if i + 1 < len(raw):
                top = pd.Series(raw.iloc[i].tolist()).ffill().tolist()
                sub = raw.iloc[i + 1].tolist()
                combo = []
                for a, b in zip(top, sub):
                    ca, cb = _clean_name(a), _clean_name(b)
                    if cb and cb.lower() != "nan":
                        combo.append((ca + "_" + cb).strip("_"))
                    else:
                        combo.append(ca)
                cscore = sum(1 for t in terms if any(t in c for c in combo))
                if cscore > best[1]:
                    best = (i, cscore, combo, i + 2)
        if best[1] < 3:
            return raw, False
        cols, used = [], {}
        for j, c in enumerate(best[2]):
            name = c if c and c.lower() != "nan" else f"col_{j}"
            if name in used:
                used[name] += 1
                name = f"{name}_{used[name]}"
            else:
                used[name] = 0
            cols.append(name)
        df = raw.iloc[best[3]:].copy()
        df.columns = cols
        return df, True

    def _pick(df, includes, excludes=()):
        cols = list(df.columns)
        norm = {c: _clean_name(c).lower() for c in cols}
        incs = [_clean_name(x).lower() for x in includes]
        excs = [_clean_name(x).lower() for x in excludes]
        # exact / suffix match first
        for inc in incs:
            for c in cols:
                n = norm[c]
                if any(e and e in n for e in excs):
                    continue
                if n == inc or n.endswith("_" + inc):
                    return c
        # contains match
        for inc in incs:
            for c in cols:
                n = norm[c]
                if any(e and e in n for e in excs):
                    continue
                if inc and inc in n:
                    return c
        return None

    def _s(row, col):
        if not col: return ""
        v = row.get(col, "")
        if pd.isna(v): return ""
        txt = str(v).strip()
        if txt.lower() in ("nan", "none"):
            return ""
        # 엑셀에서 조직코드가 123.0 형태로 들어온 경우 보정
        if txt.endswith(".0") and txt[:-2].isdigit():
            return txt[:-2]
        return txt

    def _cnt(row, col):
        return max(0, safe_int(row.get(col, 0))) if col else 0

    def _flt(row, col):
        return safe_float(row.get(col, 0)) if col else 0.0

    def _valid_org(v):
        if v is None: return False
        x = str(v).strip()
        if x in ("", "nan", "None", "합계", "총합계", "소계"):
            return False
        # 본부/담당명은 '수도권서부고객본부'처럼 문자여야 한다. 순수 숫자만 있는 행은 헤더/코드 행으로 간주.
        if x.replace("-", "").replace(".", "").isdigit():
            return False
        return True

    df, headered = _read_sales_frame()
    # 년월 컬럼이 있으면 해당 월만 삭제, 없으면 전체 삭제
    _yyyymm_col = None
    if headered:
        _yyyymm_col = _pick(df, ["년월", "기준년월", "기준월", "집계월", "월"])
    if _yyyymm_col:
        sample_months = set()
        for v in df[_yyyymm_col].dropna().unique():
            mm = _norm_month(v)
            if mm:
                sample_months.add(mm)
        if sample_months:
            for mm in sample_months:
                db.query(Sales).filter(Sales.yyyymm == mm).delete()
            db.commit()
            print(f"[판매로드] 기존 데이터 삭제 완료: {sorted(sample_months)}")
        else:
            db.query(Sales).delete(); db.commit()
    else:
        db.query(Sales).delete(); db.commit()
    buf = []

    if headered:
        c_boomun = _pick(df, ["부문", "그룹", "총괄"])
        c_bonbu = _pick(df, ["본부명", "본부"])
        c_team = _pick(df, ["담당명", "담당", "지사명", "지사"])
        c_dept = _pick(df, ["부서", "조직"])
        c_agency_code = _pick(df, ["대리점코드", "무선유통조직", "계약대리점코드", "판매조직"])
        c_agency_org = _pick(df, ["대리점조직", "대리점조직명", "판매조직명"])
        c_agency = _pick(df, ["계약대리점명", "대리점명", "대리점"])
        c_ch1 = _pick(df, ["채널대분류", "채널1", "판매채널대분류"])
        c_ch2 = _pick(df, ["채널중분류", "채널2", "판매채널중분류"])
        c_ch3 = _pick(df, ["채널소분류", "채널3", "판매채널소분류"])
        c_chsub = _pick(df, ["채널상세", "채널Sub", "채널서브", "판매유형", "판매경로", "채널"], ["대분류", "중분류", "소분류"])
        c_sale_type = _pick(df, ["판매구분", "구분", "일반중고", "일반/중고"])
        c_kids = _pick(df, ["키즈", "Kids"])
        c_foreigner = _pick(df, ["외국인", "내외국인", "내/외국인"])
        c_k110 = _pick(df, ["초이스", "110K", "110"])
        c_yyyymm = _pick(df, ["년월", "기준년월", "기준월", "집계월", "월"])

        c_sale = _pick(df, ["총판매", "판매량", "판매건수", "개통건수", "판매"], ["신규", "MNP", "번호이동", "기변", "해지", "순증", "매출", "ARPU", "율", "비중", "목표"])
        c_net = _pick(df, ["순증", "순증감", "netadd", "net_add"])
        c_new = _pick(df, ["신규판매", "신규가입", "신규개통", "신규"], ["010", "순증", "ARPU", "율", "비중", "목표", "해지"])
        c_010 = _pick(df, ["010신규", "010 신규", "순수신규", "순수 신규", "010"], ["해지", "율", "비중", "목표"])
        c_mnp = _pick(df, ["총MNP", "MNP계", "번호이동계", "MNP", "번호이동"], ["해지", "순증", "율", "비중", "목표", "S.MNP", "L.MNP", "M.MNP", "V.MNP", "SMNP", "LMNP", "MMNP", "VMNP", "SKT", "LGU", "MVNO", "자사이동"])
        c_smnp = _pick(df, ["S.MNP", "SMNP", "S_MNP", "SKT MNP", "SKT"] , ["해지", "순증", "율", "비중"])
        c_lmnp = _pick(df, ["L.MNP", "LMNP", "L_MNP", "LGU MNP", "LGU"] , ["해지", "순증", "율", "비중"])
        c_mmnp = _pick(df, ["M.MNP", "MMNP", "M_MNP", "자사이동", "MNO자사"] , ["해지", "순증", "율", "비중"])
        c_vmnp = _pick(df, ["V.MNP", "VMNP", "V_MNP", "MVNO", "알뜰"] , ["해지", "순증", "율", "비중"])
        c_churn = _pick(df, ["총해지", "해지건수", "해지"], ["MNP", "S.MNP", "L.MNP", "M.MNP", "V.MNP", "율", "비중", "목표"])
        c_mnp_churn = _pick(df, ["MNP해지", "번호이동해지"])
        c_smnp_churn = _pick(df, ["S.MNP해지", "SMNP해지", "SKT해지"])
        c_lmnp_churn = _pick(df, ["L.MNP해지", "LMNP해지", "LGU해지"])
        c_mmnp_churn = _pick(df, ["M.MNP해지", "MMNP해지", "자사이동해지"])
        c_vmnp_churn = _pick(df, ["V.MNP해지", "VMNP해지", "MVNO해지", "알뜰해지"])
        c_forced = _pick(df, ["강제해지", "직권해지", "ForcedChurn"])
        c_premium = _pick(df, ["기변", "기기변경", "우수기변", "우수"] , ["해지", "율", "비중", "목표"])
        c_arpu = _pick(df, ["신규ARPU", "ARPU", "arpu"], ["목표"])
        c_rev = _pick(df, ["매출", "판매매출", "Revenue"])
        c_subscriber = _pick(df, ["재적가입자", "유지가입자", "가입자", "Subscriber"])

        for _, row in df.iterrows():
            bonbu = _s(row, c_bonbu)
            team = _s(row, c_team)
            agency = _s(row, c_agency)
            # 본부가 없고 담당/대리점만 있는 파일도 허용하되, 완전 빈 행은 제외
            if not (_valid_org(bonbu) or _valid_org(team) or _valid_org(agency)):
                continue
            mnp_parts = _cnt(row, c_smnp) + _cnt(row, c_lmnp) + _cnt(row, c_mmnp) + _cnt(row, c_vmnp)
            mnp_val = _cnt(row, c_mnp) or mnp_parts
            new_sale = _cnt(row, c_new)
            n010 = _cnt(row, c_010)
            if new_sale == 0:
                new_sale = n010 + mnp_val
            if n010 == 0 and new_sale >= mnp_val:
                n010 = max(0, new_sale - mnp_val)
            premium_val = _cnt(row, c_premium)
            sale_cnt = _cnt(row, c_sale)
            if sale_cnt == 0:
                sale_cnt = max(0, new_sale + premium_val)
            churn_val = _cnt(row, c_churn)
            if churn_val == 0:
                churn_val = _cnt(row, c_smnp_churn) + _cnt(row, c_lmnp_churn) + _cnt(row, c_mmnp_churn) + _cnt(row, c_vmnp_churn) + _cnt(row, c_forced)
            rev_val = _flt(row, c_rev)
            sub_val = _cnt(row, c_subscriber)
            arpu_val = _flt(row, c_arpu)
            if arpu_val <= 100 and rev_val > 0 and sub_val > 0:
                arpu_val = round(rev_val / sub_val)
            # 년월 추출: 202601, 2026-01, 엑셀 날짜/Timestamp를 모두 YYYYMM으로 정규화
            yyyymm_val = _norm_month(row.get(c_yyyymm, "")) if c_yyyymm else ""
            obj = Sales(
                yyyymm=yyyymm_val,
                boomun=_s(row, c_boomun), bonbu=bonbu, team=team, dept=_s(row, c_dept),
                agency_code=_s(row, c_agency_code), agency_org=_s(row, c_agency_org), agency=agency,
                channel1=_s(row, c_ch1), channel2=_s(row, c_ch2), channel3=_s(row, c_ch3), channel_sub=_s(row, c_chsub),
                sale_type=_s(row, c_sale_type), kids=_s(row, c_kids), foreigner=_s(row, c_foreigner), k110=_s(row, c_k110),
                sale_count=sale_cnt, net_add=safe_int(row.get(c_net, new_sale - churn_val)) if c_net else new_sale - churn_val,
                new_sub=new_sale, mnp=mnp_val,
                smnp=_cnt(row, c_smnp), lmnp=_cnt(row, c_lmnp), mmnp=_cnt(row, c_mmnp), vmnp=_cnt(row, c_vmnp),
                churn=churn_val, mnp_churn=_cnt(row, c_mnp_churn),
                smnp_churn=_cnt(row, c_smnp_churn), lmnp_churn=_cnt(row, c_lmnp_churn),
                mmnp_churn=_cnt(row, c_mmnp_churn), vmnp_churn=_cnt(row, c_vmnp_churn),
                forced_churn=_cnt(row, c_forced), premium_change=premium_val,
                arpu=arpu_val, revenue=rev_val, subscriber=sub_val,
            )
            obj.new_sale = new_sale
            obj.new010 = n010
            obj.new_arpu = arpu_val
            # 완전 무실적/무조직 잡행 제거
            if obj.sale_count == 0 and obj.new_sale == 0 and obj.mnp == 0 and obj.premium_change == 0 and obj.churn == 0:
                continue
            buf.append(obj)
            if len(buf) >= BATCH:
                db.bulk_save_objects(buf); db.commit(); buf = []
        if buf: db.bulk_save_objects(buf); db.commit()
        return

    # fallback: 과거 고정 인덱스. 단, 음수 실적은 0으로 보정하고 판매량 누락 시 신규+기변으로 역산한다.
    df = pd.read_excel(io.BytesIO(contents), skiprows=2, header=None)
    for _, row in df.iterrows():
        val_bonbu = str(_row_val(row, 3, "")) if pd.notna(_row_val(row, 3, "")) else ""
        if val_bonbu in ("", "nan", "None", "합계") or val_bonbu.lstrip("-").isdigit(): continue
        sale_cnt = max(0, safe_int(_row_val(row, 21)))
        new_sale = max(0, safe_int(_row_val(row, 23)))
        n010_raw = max(0, safe_int(_row_val(row, 24)))
        mnp_val = max(0, safe_int(_row_val(row, 25)))
        n010 = n010_raw if n010_raw > 0 else max(0, new_sale - mnp_val)
        premium_val = max(0, safe_int(_row_val(row, 38)))
        if new_sale == 0:
            new_sale = n010 + mnp_val
        if sale_cnt == 0:
            sale_cnt = max(0, new_sale + premium_val)
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
            sale_count=sale_cnt, net_add=new_sale - max(0, safe_int(_row_val(row, 30))),
            new_sub=new_sale, mnp=mnp_val,
            smnp=max(0, safe_int(_row_val(row, 26))), lmnp=max(0, safe_int(_row_val(row, 27))),
            mmnp=max(0, safe_int(_row_val(row, 28))), vmnp=max(0, safe_int(_row_val(row, 29))),
            churn=max(0, safe_int(_row_val(row, 30))), mnp_churn=max(0, safe_int(_row_val(row, 32))),
            smnp_churn=max(0, safe_int(_row_val(row, 33))), lmnp_churn=max(0, safe_int(_row_val(row, 34))),
            mmnp_churn=max(0, safe_int(_row_val(row, 35))), vmnp_churn=max(0, safe_int(_row_val(row, 36))),
            forced_churn=max(0, safe_int(_row_val(row, 37))), premium_change=premium_val,
            arpu=arpu_val, revenue=safe_float(_row_val(row, 40)),
            subscriber=max(0, safe_int(_row_val(row, 41))),
        )
        obj.new_sale = new_sale
        obj.new010 = n010
        obj.new_arpu = arpu_val
        if obj.sale_count == 0 and obj.new_sale == 0 and obj.mnp == 0 and obj.premium_change == 0 and obj.churn == 0:
            continue
        buf.append(obj)
        if len(buf) >= BATCH:
            db.bulk_save_objects(buf); db.commit(); buf = []
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

def _repair_sales_sanity(db):
    """기존 DB에 남아 있는 음수/누락 판매값을 방어적으로 보정한다."""
    try:
        rows = db.query(Sales).all()
        changed = 0
        for r in rows:
            before = (r.sale_count, r.new_sub, getattr(r, "new_sale", 0), getattr(r, "new010", 0), r.mnp, r.premium_change, r.churn)
            for attr in ["sale_count", "new_sub", "new_sale", "new010", "mnp", "smnp", "lmnp", "mmnp", "vmnp", "churn", "mnp_churn", "smnp_churn", "lmnp_churn", "mmnp_churn", "vmnp_churn", "forced_churn", "premium_change", "subscriber"]:
                if hasattr(r, attr):
                    try:
                        setattr(r, attr, max(0, int(getattr(r, attr) or 0)))
                    except Exception:
                        setattr(r, attr, 0)
            if getattr(r, "new_sale", 0) == 0 and (r.new_sub or 0) > 0:
                r.new_sale = r.new_sub
            if (r.new_sub or 0) == 0 and getattr(r, "new_sale", 0) > 0:
                r.new_sub = r.new_sale
            if getattr(r, "new010", 0) == 0 and getattr(r, "new_sale", 0) >= (r.mnp or 0):
                r.new010 = max(0, r.new_sale - (r.mnp or 0))
            if (r.sale_count or 0) == 0:
                r.sale_count = max(0, getattr(r, "new_sale", 0) + (r.premium_change or 0))
            if (r.net_add is None) or abs(int(r.net_add or 0)) > max(100000, (r.sale_count or 0) * 5):
                r.net_add = getattr(r, "new_sale", 0) - (r.churn or 0)
            after = (r.sale_count, r.new_sub, getattr(r, "new_sale", 0), getattr(r, "new010", 0), r.mnp, r.premium_change, r.churn)
            if before != after:
                changed += 1
        if changed:
            db.commit()
            print(f"[데이터 보정] Sales {changed}행 보정")
    except Exception as e:
        db.rollback()
        print(f"[데이터 보정 오류] {e}")

_ktoa_cache = None


def _load_storesales(db, contents):
    """
    소매 매장실적 로더.
    원천 파일은 '본부 코드/본부명', '판매접점 코드/판매점명'처럼 코드와 명칭이
    옆 컬럼에 나란히 있는 구조가 많다. 기존 로더는 코드 컬럼만 잡아 매장명이 비거나
    월(YYYYMM)이 깨지는 문제가 있어 명칭 우선·월 정규화 방식으로 읽는다.
    """
    df = _read_headered_excel(contents, ["년월", "본부", "담당", "무선유통조직", "판매접점", "판매량", "신규", "010", "MNP", "해지", "ARPU"])

    c_month = _pick_col(df, ["년월", "기준년월", "기준월", "월"])
    c_date = _pick_col(df, ["일자", "기준일", "개통일", "판매일"])
    c_bonbu = _pick_col(df, ["본부"])
    c_team = _pick_col(df, ["담당·지사", "담당", "지사"])
    c_agency_code = _pick_col(df, ["무선유통조직", "판매조직", "대리점코드", "대표코드"])
    c_store_code = _pick_col(df, ["판매접점", "접점코드", "매장코드"])
    c_channel = _pick_col(df, ["판매채널", "채널", "판매유형", "판매경로"])
    c_sale = _pick_col(df, ["판매량", "총판매", "판매건수", "판매"])
    c_net = _pick_col(df, ["순증"])
    c_new = _pick_col(df, ["신규판매", "신규"])
    c_010 = _pick_col(df, ["010신규", "010"])
    c_mnp = _pick_col(df, ["MNP", "번호이동"])
    c_premium = _pick_col(df, ["우수기변", "기변", "기기변경"])
    c_churn = _pick_col(df, ["해지"])
    c_rev = _pick_col(df, ["판매매출", "매출", "수익"])
    c_arpu = _pick_col(df, ["ARPU", "arpu"])

    # 코드 컬럼 바로 오른쪽이 명칭인 원천 양식을 우선 지원
    c_bonbu_nm = _next_col(df, c_bonbu)
    c_team_nm = _next_col(df, c_team)
    c_agency_nm = _next_col(df, c_agency_code)
    c_store_nm = _next_col(df, c_store_code)

    incoming_months = set()
    rows_to_save = []
    for _, row in df.iterrows():
        ref_month = _norm_month(row.get(c_month, "")) if c_month else ""
        sale_date = _clean_text(row.get(c_date, ""))[:10] if c_date else ""
        bonbu = _prefer_name(row, c_bonbu, c_bonbu_nm)
        team = _prefer_name(row, c_team, c_team_nm)
        agency_code = _clean_text(row.get(c_agency_code, "")) if c_agency_code else ""
        agency = _prefer_name(row, c_agency_code, c_agency_nm)
        store_code = _clean_text(row.get(c_store_code, "")) if c_store_code else ""
        store = _prefer_name(row, c_store_code, c_store_nm)
        channel = _clean_text(row.get(c_channel, "")) if c_channel else ""

        sale = safe_int(row.get(c_sale, 0)) if c_sale else 0
        new_sale = safe_int(row.get(c_new, 0)) if c_new else 0
        mnp = safe_int(row.get(c_mnp, 0)) if c_mnp else 0
        n010 = safe_int(row.get(c_010, 0)) if c_010 else max(0, new_sale - mnp)
        premium = safe_int(row.get(c_premium, 0)) if c_premium else 0
        churn = safe_int(row.get(c_churn, 0)) if c_churn else 0

        if not new_sale and (n010 or mnp):
            new_sale = n010 + mnp
        if not sale and (new_sale or premium):
            sale = new_sale + premium
        if not churn and c_net:
            # 순증 = 신규 - 해지 구조일 때 해지를 역산할 수 있으면 활용
            net_val = safe_int(row.get(c_net, 0))
            if new_sale and new_sale - net_val >= 0:
                churn = new_sale - net_val

        if not any([bonbu, team, agency, store, channel]) and sale == 0:
            continue
        if ref_month:
            incoming_months.add(ref_month)

        rows_to_save.append(StoreSales(
            ref_month=ref_month,
            sale_date=sale_date,
            bonbu=bonbu,
            team=team,
            agency=agency,
            agency_code=agency_code,
            store=store,
            contact=store_code,
            channel=channel,
            sale=sale,
            new_sale=new_sale,
            new010=n010,
            mnp=mnp,
            premium=premium,
            churn=churn,
            revenue=safe_float(row.get(c_rev, 0)) if c_rev else 0.0,
            arpu=safe_float(row.get(c_arpu, 0)) if c_arpu else 0.0,
        ))

    # 월이 식별되면 해당 월만 교체. 월이 전혀 없으면 전체 교체.
    if incoming_months:
        db.query(StoreSales).filter(StoreSales.ref_month.in_(list(incoming_months))).delete(synchronize_session=False)
    else:
        db.query(StoreSales).delete()
    db.commit()

    buf = []
    for obj in rows_to_save:
        buf.append(obj)
        if len(buf) >= BATCH:
            db.bulk_save_objects(buf); db.commit(); buf = []
    if buf:
        db.bulk_save_objects(buf); db.commit()

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
        os.makedirs(DATA_DIR, exist_ok=True)
        if AUTOLOAD_EXCEL:
            app_dir = os.path.dirname(__file__)
            for fname, loader in [
                ("sales.xlsx", _load_sales), ("commission.xlsx", _load_commission),
                ("device.xlsx", _load_device), ("inventory.xlsx", _load_inventory),
                ("subscriber.xlsx", _load_subscriber), ("storesales.xlsx", _load_storesales),
                ("subsidy.xlsx", _load_subsidy), ("targets.xlsx", _load_targets),
                ("business_days.xlsx", _load_business_days),
            ]:
                # app/data 우선, 없으면 과거 패키징 방식(app 루트)의 xlsx도 자동 인식
                path = next((p for p in [os.path.join(DATA_DIR, fname), os.path.join(app_dir, fname)] if os.path.exists(p)), None)
                if path:
                    with open(path, "rb") as f: loader(db, f.read())
                    print(f"[자동로드] {fname} 완료: {path}")
            ktoa_path = next((p for p in [os.path.join(DATA_DIR, "ktoa_day.xlsx"), os.path.join(app_dir, "ktoa_day.xlsx")] if os.path.exists(p)), None)
            if ktoa_path:
                with open(ktoa_path, "rb") as f: _load_ktoa(f.read())
                print(f"[자동로드] ktoa_day.xlsx 완료: {ktoa_path}")
        else:
            print("[자동로드] AUTOLOAD_EXCEL=0: 대용량 엑셀 자동 적재를 건너뜁니다. UI 업로드 버튼을 사용하세요.")
    except Exception as e:
        print(f"[자동로드 오류] {e}")
    try:
        _repair_sales_sanity(db)
    except Exception as e:
        print(f"[데이터 보정 호출 오류] {e}")
    finally:
        db.close()
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
        add_col(conn, Sales.__tablename__, "yyyymm VARCHAR DEFAULT ''")
        add_col(conn, Sales.__tablename__, "new_sale INTEGER DEFAULT 0")
        add_col(conn, Sales.__tablename__, "new010 INTEGER DEFAULT 0")
        add_col(conn, Sales.__tablename__, "new_arpu FLOAT DEFAULT 0")
        add_col(conn, Commission.__tablename__, "commission_policy_name VARCHAR DEFAULT ''")
        add_col(conn, DeviceSales.__tablename__, "new_sale INTEGER DEFAULT 0")
        add_col(conn, Subscriber.__tablename__, "agency_code VARCHAR DEFAULT ''")
        add_col(conn, Subscriber.__tablename__, "ref_date VARCHAR DEFAULT ''")
        add_col(conn, Subscriber.__tablename__, "sub_count INTEGER DEFAULT 0")
        for col_def in [
            "ref_month VARCHAR DEFAULT ''", "sale_date VARCHAR DEFAULT ''",
            "bonbu VARCHAR DEFAULT ''", "team VARCHAR DEFAULT ''", "agency VARCHAR DEFAULT ''",
            "agency_code VARCHAR DEFAULT ''", "store VARCHAR DEFAULT ''", "contact VARCHAR DEFAULT ''",
            "channel VARCHAR DEFAULT ''", "sale INTEGER DEFAULT 0", "new_sale INTEGER DEFAULT 0",
            "new010 INTEGER DEFAULT 0", "mnp INTEGER DEFAULT 0", "premium INTEGER DEFAULT 0",
            "churn INTEGER DEFAULT 0", "revenue FLOAT DEFAULT 0", "arpu FLOAT DEFAULT 0"
        ]:
            add_col(conn, StoreSales.__tablename__, col_def)

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
        # 사용 가능한 년월 목록: 판매 데이터와 소매 매장실적 월을 합산
        sale_months = {r[0] for r in db.query(Sales.yyyymm).distinct()
            .filter(Sales.yyyymm!="", Sales.yyyymm!="nan").all() if r[0]}
        store_months = {r[0] for r in db.query(StoreSales.ref_month).distinct()
            .filter(StoreSales.ref_month!="", StoreSales.ref_month!="nan").all() if r[0]}
        month_all = sorted(sale_months | store_months, reverse=True)
        sale_bonbu = {r[0] for r in db.query(Sales.bonbu, func.sum(Sales.sale_count))
            .filter(Sales.bonbu!="",Sales.bonbu!="nan")
            .group_by(Sales.bonbu).having(func.sum(Sales.sale_count)>=MIN_BONBU).all() if r[0]}
        store_bonbu = {r[0] for r in db.query(StoreSales.bonbu).distinct()
            .filter(StoreSales.bonbu!="",StoreSales.bonbu!="nan").all() if r[0]}
        bonbu_all = sorted(sale_bonbu | store_bonbu)

        tq = db.query(Sales.team).distinct().filter(Sales.team!="",Sales.team!="nan")
        stq = db.query(StoreSales.team).distinct().filter(StoreSales.team!="",StoreSales.team!="nan")
        if bonbu_list:
            tq = tq.filter(Sales.bonbu.in_(bonbu_list))
            stq = stq.filter(StoreSales.bonbu.in_(bonbu_list))
        team_all = sorted({r[0] for r in tq.all() if r[0]} | {r[0] for r in stq.all() if r[0]})

        aq = db.query(Sales.agency).distinct().filter(Sales.agency!="",Sales.agency!="nan")
        saq = db.query(StoreSales.agency).distinct().filter(StoreSales.agency!="",StoreSales.agency!="nan")
        if bonbu_list:
            aq = aq.filter(Sales.bonbu.in_(bonbu_list))
            saq = saq.filter(StoreSales.bonbu.in_(bonbu_list))
        if team_list:
            aq = aq.filter(Sales.team.in_(team_list))
            saq = saq.filter(StoreSales.team.in_(team_list))
        agency_all = sorted({r[0] for r in aq.all() if r[0]} | {r[0] for r in saq.all() if r[0]})

        sale_channels = {r[0] for r in db.query(Sales.channel_sub).distinct()
            .filter(Sales.channel_sub!="",Sales.channel_sub!="nan").all() if r[0]}
        store_channels = {r[0] for r in db.query(StoreSales.channel).distinct()
            .filter(StoreSales.channel!="",StoreSales.channel!="nan").all() if r[0]}
        channel_all = sorted(sale_channels | store_channels)
        # 수수료 정책명 목록
        policy_all = [r[0] for r in db.query(Commission.commission_policy_name).distinct()
            .filter(Commission.commission_policy_name!="",Commission.commission_policy_name!="nan")
            .order_by(Commission.commission_policy_name).all() if r[0]]
        return {"bonbu_list":bonbu_all,"team_list":team_all,
                "agency_list":agency_all,"channel_list":channel_all,
                "policy_list":policy_all,"month_list":month_all}
    finally: db.close()

# ── Drilldown ─────────────────────────────────────────────────────
@app.get("/api/drilldown")
async def get_drilldown(
    level: str = "bonbu",
    bonbu_list: List[str] = Query(default=[]),
    team_list: List[str] = Query(default=[]),
    channel_list: List[str] = Query(default=[]),
    yyyymm_list: List[str] = Query(default=[]),
    agency: str = None,
):
    db = SessionLocal()
    try:
        def af(q):
            if bonbu_list: q = q.filter(Sales.bonbu.in_(bonbu_list))
            if team_list: q = q.filter(Sales.team.in_(team_list))
            if agency: q = q.filter(Sales.agency==agency)
            if channel_list: q = q.filter(Sales.channel_sub.in_(channel_list))
            if yyyymm_list: q = q.filter(Sales.yyyymm.in_(yyyymm_list))
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
    yyyymm_list: List[str] = Query(default=[]),
):
    db = SessionLocal()
    try:
        def af(q):
            if bonbu_list: q = q.filter(Sales.bonbu.in_(bonbu_list))
            if team_list: q = q.filter(Sales.team.in_(team_list))
            if agency: q = q.filter(Sales.agency==agency)
            if channel_list: q = q.filter(Sales.channel_sub.in_(channel_list))
            if yyyymm_list: q = q.filter(Sales.yyyymm.in_(yyyymm_list))
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
    yyyymm_list: List[str] = Query(default=[]),
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
        elif view == "channel": col = StoreSales.channel

        def af(q):
            if bonbu_list: q = q.filter(StoreSales.bonbu.in_(bonbu_list))
            if team_list: q = q.filter(StoreSales.team.in_(team_list))
            if yyyymm_list: q = q.filter(StoreSales.ref_month.in_(yyyymm_list))
            if agency: q = q.filter(StoreSales.agency==agency)
            if channel_list: q = q.filter(StoreSales.channel.in_(channel_list))
            return q

        metric_exprs = [
            func.sum(StoreSales.sale), func.sum(StoreSales.new_sale),
            func.sum(StoreSales.new010), func.sum(StoreSales.mnp),
            func.sum(StoreSales.premium), func.sum(StoreSales.churn),
            func.sum(StoreSales.revenue), func.sum(StoreSales.arpu * StoreSales.sale),
        ]
        q = af(db.query(col, *metric_exprs))
        rows = q.filter(col!="", col!="nan").group_by(col).order_by(func.sum(StoreSales.sale).desc()).limit(limit).all()

        items=[]
        for r in rows:
            sale=int(r[1] or 0); new_sale=int(r[2] or 0); churn=int(r[6] or 0); rev=float(r[7] or 0)
            arpu_num=float(r[8] or 0)
            items.append({"name":r[0],"sale":sale,"new_sale":new_sale,"new_sub":new_sale,
                          "n010":int(r[3] or 0),"new010":int(r[3] or 0),"mnp":int(r[4] or 0),
                          "premium":int(r[5] or 0),"churn":churn,"net":new_sale-churn,
                          "revenue":rev,"arpu":round(arpu_num/sale) if sale>0 and arpu_num>0 else 0})

        total_row = af(db.query(*metric_exprs)).first()
        t_sale = int((total_row[0] if total_row else 0) or 0)
        t_new = int((total_row[1] if total_row else 0) or 0)
        t_010 = int((total_row[2] if total_row else 0) or 0)
        t_mnp = int((total_row[3] if total_row else 0) or 0)
        t_premium = int((total_row[4] if total_row else 0) or 0)
        t_churn = int((total_row[5] if total_row else 0) or 0)
        t_rev = float((total_row[6] if total_row else 0) or 0)
        t_arpu_num = float((total_row[7] if total_row else 0) or 0)
        totals={"sale":t_sale,"new_sale":t_new,"new_sub":t_new,"n010":t_010,"new010":t_010,
                "mnp":t_mnp,"premium":t_premium,"churn":t_churn,"net":t_new-t_churn,
                "revenue":t_rev,"arpu":round(t_arpu_num/t_sale) if t_sale>0 and t_arpu_num>0 else 0,
                "store_count": int(af(db.query(func.count(func.distinct(StoreSales.store)))).scalar() or 0),
                "agency_count": int(af(db.query(func.count(func.distinct(StoreSales.agency)))).scalar() or 0)}

        months = sorted([r[0] for r in af(db.query(StoreSales.ref_month).distinct())
            .filter(StoreSales.ref_month!="", StoreSales.ref_month!="nan").all()])
        trend=[]
        for r in af(db.query(
            StoreSales.ref_month, func.sum(StoreSales.sale), func.sum(StoreSales.new_sale),
            func.sum(StoreSales.mnp), func.sum(StoreSales.premium), func.sum(StoreSales.churn),
            func.sum(StoreSales.arpu * StoreSales.sale),
        )).filter(StoreSales.ref_month!="", StoreSales.ref_month!="nan").group_by(StoreSales.ref_month).order_by(StoreSales.ref_month).all():
            sale=int(r[1] or 0); new_sale=int(r[2] or 0); churn=int(r[5] or 0); arpu_num=float(r[6] or 0)
            trend.append({"month":r[0],"sale":sale,"new_sale":new_sale,"mnp":int(r[3] or 0),
                          "premium":int(r[4] or 0),"churn":churn,"net":new_sale-churn,
                          "arpu":round(arpu_num/sale) if sale>0 and arpu_num>0 else 0})
        for i, row in enumerate(trend):
            prev = trend[i-1]["sale"] if i > 0 else 0
            row["mom"] = round((row["sale"]-prev)/prev*100, 1) if prev > 0 else None

        channel_rows = af(db.query(StoreSales.channel, func.sum(StoreSales.sale))).filter(
            StoreSales.channel!="", StoreSales.channel!="nan"
        ).group_by(StoreSales.channel).order_by(func.sum(StoreSales.sale).desc()).limit(12).all()
        channels = [{"name":r[0],"value":int(r[1] or 0)} for r in channel_rows]

        return {"view":view,"items":items,"totals":totals,"months":months,"trend":trend,"channels":channels}
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

# ── Monthly Trend API ─────────────────────────────────────────────
@app.get("/api/monthly-trend")
async def get_monthly_trend(
    bonbu_list: List[str] = Query(default=[]),
    team_list: List[str] = Query(default=[]),
    channel_list: List[str] = Query(default=[]),
    metric: str = "sale",  # sale | new_sale | mnp | churn | arpu | revenue | net
):
    db = SessionLocal()
    try:
        def af(q):
            if bonbu_list: q = q.filter(Sales.bonbu.in_(bonbu_list))
            if team_list:  q = q.filter(Sales.team.in_(team_list))
            if channel_list: q = q.filter(Sales.channel_sub.in_(channel_list))
            return q

        # 전체 월 목록: 본부/담당/채널 필터 적용 후 실제 데이터가 있는 월만 표시
        months = sorted([r[0] for r in af(db.query(Sales.yyyymm).distinct())
            .filter(Sales.yyyymm!="", Sales.yyyymm!="nan").all()])

        if not months:
            return {"months": [], "total": [], "bonbu": [], "channel": []}

        # 월별 전체 합계
        col_map = {
            "sale": Sales.sale_count, "new_sale": Sales.new_sale,
            "mnp": Sales.mnp, "churn": Sales.churn,
            "arpu": Sales.arpu, "revenue": Sales.revenue,
            "n010": Sales.new010, "premium": Sales.premium_change,
            "smnp": Sales.smnp, "lmnp": Sales.lmnp,
            "mmnp": Sales.mmnp, "vmnp": Sales.vmnp,
        }
        col = col_map.get(metric, Sales.sale_count)

        total_by_month = {}
        for r in af(db.query(Sales.yyyymm, func.sum(col))).filter(
            Sales.yyyymm!="", Sales.yyyymm!="nan"
        ).group_by(Sales.yyyymm).all():
            if metric == "arpu":
                # ARPU는 가중평균: sum(arpu*subscriber)/sum(subscriber)
                pass
            total_by_month[r[0]] = float(r[1] or 0)

        if metric == "arpu":
            for r in af(db.query(
                Sales.yyyymm,
                func.sum(Sales.arpu * Sales.subscriber),
                func.sum(Sales.subscriber),
            )).filter(Sales.yyyymm!="", Sales.yyyymm!="nan").group_by(Sales.yyyymm).all():
                sub = float(r[2] or 0)
                total_by_month[r[0]] = round(float(r[1] or 0) / sub) if sub > 0 else 0

        total_series = [{"month": m, "value": round(total_by_month.get(m, 0))} for m in months]

        # 전월 대비 증감율 추가
        for i, item in enumerate(total_series):
            prev = total_series[i-1]["value"] if i > 0 else 0
            item["mom"] = round((item["value"] - prev) / prev * 100, 1) if prev > 0 else None

        # 본부별 월별 추이 (상위 본부만)
        bonbu_months = {}
        for r in af(db.query(Sales.yyyymm, Sales.bonbu, func.sum(col))).filter(
            Sales.yyyymm!="", Sales.yyyymm!="nan",
            Sales.bonbu!="", Sales.bonbu!="nan"
        ).group_by(Sales.yyyymm, Sales.bonbu).all():
            mm, bn, val = r[0], r[1], float(r[2] or 0)
            if bn not in bonbu_months: bonbu_months[bn] = {}
            bonbu_months[bn][mm] = round(val)

        # 전체 합산 기준 상위 5 본부
        bonbu_totals = {bn: sum(v.values()) for bn, v in bonbu_months.items()}
        top_bonbu = sorted(bonbu_totals, key=lambda x: -bonbu_totals[x])[:7]
        bonbu_series = [
            {"name": bn, "data": [bonbu_months[bn].get(m, 0) for m in months]}
            for bn in top_bonbu
        ]

        # 채널별 월별 추이
        ch_months = {}
        for r in af(db.query(Sales.yyyymm, Sales.channel_sub, func.sum(col))).filter(
            Sales.yyyymm!="", Sales.yyyymm!="nan",
            Sales.channel_sub!="", Sales.channel_sub!="nan"
        ).group_by(Sales.yyyymm, Sales.channel_sub).all():
            mm, ch, val = r[0], r[1], float(r[2] or 0)
            if ch not in ch_months: ch_months[ch] = {}
            ch_months[ch][mm] = round(val)

        ch_totals = {ch: sum(v.values()) for ch, v in ch_months.items()}
        top_ch = sorted(ch_totals, key=lambda x: -ch_totals[x])[:6]
        channel_series = [
            {"name": ch, "data": [ch_months[ch].get(m, 0) for m in months]}
            for ch in top_ch
        ]

        # 월별 다지표 요약 (판매/신규/MNP/해지/순증/ARPU 한번에)
        multi = {}
        for r in af(db.query(
            Sales.yyyymm,
            func.sum(Sales.sale_count), func.sum(Sales.new_sale),
            func.sum(Sales.mnp), func.sum(Sales.churn),
            func.sum(Sales.premium_change), func.sum(Sales.new010),
            func.sum(Sales.smnp), func.sum(Sales.lmnp),
            func.sum(Sales.mmnp), func.sum(Sales.vmnp),
            func.sum(Sales.revenue), func.sum(Sales.subscriber),
            func.sum(Sales.arpu * Sales.subscriber),
        )).filter(Sales.yyyymm!="", Sales.yyyymm!="nan").group_by(Sales.yyyymm).all():
            mm = r[0]
            sub = int(r[12] or 0)
            arpu_w = round(float(r[13] or 0) / sub) if sub > 0 else 0
            new_s = int(r[2] or 0)
            churn_s = int(r[4] or 0)
            multi[mm] = {
                "sale": int(r[1] or 0), "new_sale": new_s,
                "mnp": int(r[3] or 0), "churn": churn_s,
                "net": new_s - churn_s,
                "premium": int(r[5] or 0), "n010": int(r[6] or 0),
                "smnp": int(r[7] or 0), "lmnp": int(r[8] or 0),
                "mmnp": int(r[9] or 0), "vmnp": int(r[10] or 0),
                "revenue": float(r[11] or 0), "arpu": arpu_w,
            }
        multi_series = [{"month": m, **(multi.get(m, {}))} for m in months]

        # 본부별 월별 요약 (히트맵용)
        heatmap = {}
        for r in af(db.query(
            Sales.yyyymm, Sales.bonbu, func.sum(Sales.sale_count)
        )).filter(
            Sales.yyyymm!="", Sales.yyyymm!="nan",
            Sales.bonbu!="", Sales.bonbu!="nan"
        ).group_by(Sales.yyyymm, Sales.bonbu).having(func.sum(Sales.sale_count) >= MIN_BONBU).all():
            mm, bn, val = r[0], r[1], int(r[2] or 0)
            if bn not in heatmap: heatmap[bn] = {}
            heatmap[bn][mm] = val

        heatmap_data = [
            {"bonbu": bn, "months": {m: heatmap[bn].get(m, 0) for m in months}}
            for bn in sorted(heatmap.keys(), key=lambda x: -sum(heatmap[x].values()))
        ]

        return {
            "months": months,
            "total": total_series,
            "bonbu": bonbu_series,
            "channel": channel_series,
            "multi": multi_series,
            "heatmap": heatmap_data,
            "metric": metric,
        }
    finally:
        db.close()

@app.get("/api/health")
async def health():
    db = SessionLocal()
    try:
        return {
            "status": "ok",
            "version": APP_VERSION,
            "autoload_excel": AUTOLOAD_EXCEL,
            "sales_rows": int(db.query(func.count(Sales.id)).scalar() or 0),
            "store_sales_rows": int(db.query(func.count(StoreSales.id)).scalar() or 0),
        }
    finally:
        db.close()

@app.get("/api/version")
async def version():
    return {"version": APP_VERSION}
# ── Market Intelligence APIs ─────────────────────────────────────
def _market_db_path():
    return os.getenv(
        "MARKET_DB_PATH",
        os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                "..",
                "..",
                "market_automation",
                "market_automation.db",
            )
        ),
    )


def _market_fetch(sql: str, params: tuple = ()):
    import sqlite3

    path = _market_db_path()
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return []

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


@app.get("/api/market/timeline")
async def market_timeline(limit: int = 100):
    rows = _market_fetch(
        """
        SELECT
            id,
            event_type,
            carrier,
            source_channel,
            source_sender,
            source_time,
            raw_summary,
            confidence
        FROM market_events
        ORDER BY source_time DESC, id DESC
        LIMIT ?
        """,
        (limit,),
    )
    return {"items": rows}


@app.get("/api/market/reports")
async def market_reports(limit: int = 300):
    rows = _market_fetch(
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
        ORDER BY report_date DESC, id DESC
        LIMIT ?
        """,
        (limit,),
    )
    return {"items": rows}


@app.get("/api/market/rebate-status")
async def market_rebate_status():
    rows = _market_fetch(
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
        WHERE model_group != ''
          AND model_group != 'UNKNOWN'
        ORDER BY model_group, carrier, sales_type
        """
    )
    return {"items": rows}


@app.get("/api/market/competition")
async def market_competition():
    rows = _market_fetch(
        """
        SELECT
            model_group,
            MAX(CASE WHEN carrier='KT' THEN current_delta_krw END) AS kt_delta,
            MAX(CASE WHEN carrier='SKT' THEN current_delta_krw END) AS skt_delta,
            MAX(CASE WHEN carrier='LGU' THEN current_delta_krw END) AS lgu_delta,
            COALESCE(MAX(CASE WHEN carrier='SKT' THEN current_delta_krw END), 0)
              - COALESCE(MAX(CASE WHEN carrier='KT' THEN current_delta_krw END), 0) AS skt_vs_kt_gap,
            COALESCE(MAX(CASE WHEN carrier='LGU' THEN current_delta_krw END), 0)
              - COALESCE(MAX(CASE WHEN carrier='KT' THEN current_delta_krw END), 0) AS lgu_vs_kt_gap
        FROM current_policy_state
        WHERE model_group != ''
          AND model_group != 'UNKNOWN'
        GROUP BY model_group
        ORDER BY model_group
        """
    )
    return {"items": rows}
@app.get("/",response_class=HTMLResponse)
async def dashboard():
    with open(os.path.join(os.path.dirname(__file__),"templates","index.html"),encoding="utf-8") as f:
        return f.read()
