from sqlalchemy import Column, Integer, String
from app.database import Base


class Subscriber(Base):
    __tablename__ = "subscriber"

    id = Column(Integer, primary_key=True, index=True)

    # 조직 정보
    bonbu = Column(String, index=True, default="")
    team = Column(String, index=True, default="")
    agency_code = Column(String, index=True, default="")
    agency = Column(String, index=True, default="")

    # 세그먼트 정보: 기존 화면/확장 로직 호환용
    channel = Column(String, default="")
    sale_type = Column(String, default="")
    kids = Column(String, default="")

    # main.py의 가입자 업로드/분석 API가 사용하는 표준 컬럼
    ref_date = Column(String, index=True, default="")
    sub_count = Column(Integer, default=0)

    # 과거 스키마 호환용 컬럼
    sub_today = Column(Integer, default=0)
    sub_yesterday = Column(Integer, default=0)
    sub_prev_month_end = Column(Integer, default=0)
