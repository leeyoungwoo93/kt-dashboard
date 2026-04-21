from sqlalchemy import Column, Integer, String, Float
from app.database import Base


class Sales(Base):
    __tablename__ = "sales"

    id = Column(Integer, primary_key=True, index=True)

    # 조직
    boomun = Column(String)
    bonbu = Column(String)
    team = Column(String)
    dept = Column(String)
    agency_code = Column(String)
    agency_org = Column(String)
    agency = Column(String)

    # 채널 / 구분
    channel1 = Column(String)
    channel2 = Column(String)
    channel3 = Column(String)
    sale_type = Column(String)
    kids = Column(String)
    k110 = Column(String)

    # 판매 핵심
    sale_count = Column(Integer)
    subscriber = Column(Integer)
    net_add = Column(Integer)
    new_sub = Column(Integer)
    mnp = Column(Integer)
    smnp = Column(Integer)
    lmnp = Column(Integer)
    mmnp = Column(Integer)
    vmnp = Column(Integer)

    # 해지
    churn = Column(Integer)
    mnp_churn = Column(Integer)
    smnp_churn = Column(Integer)
    lmnp_churn = Column(Integer)
    mmnp_churn = Column(Integer)
    vmnp_churn = Column(Integer)
    forced_churn = Column(Integer)

    # 기변 / 수익
    premium_change = Column(Integer)
    arpu = Column(Float)
    revenue = Column(Float)


class Commission(Base):
    __tablename__ = "commission"

    id = Column(Integer, primary_key=True, index=True)
    jisa_code = Column(String)
    jisa_name = Column(String)
    team_code = Column(String)
    team_name = Column(String)
    agency_code = Column(String)
    agency_name = Column(String)
    channel_type = Column(String)
    channel_path = Column(String)
    channel_sale = Column(String)
    sale_policy = Column(String)
    commission_policy = Column(String)
    model_code = Column(String)
    device_model = Column(String)
    product = Column(String)
    contract = Column(String)
    dept_owner = Column(String)
    item_code = Column(String)
    refund_month = Column(String)
    pay_type = Column(String)
    amount = Column(Float)