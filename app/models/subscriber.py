from sqlalchemy import Column, Integer, String
from app.database import Base

class Subscriber(Base):
    __tablename__ = "subscriber"
    id = Column(Integer, primary_key=True, index=True)
    bonbu = Column(String); team = Column(String); agency = Column(String)
    channel = Column(String); sale_type = Column(String); kids = Column(String)
    sub_today = Column(Integer)
    sub_yesterday = Column(Integer)
    sub_prev_month_end = Column(Integer)