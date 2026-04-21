from sqlalchemy import Column, Integer, String, Float
from app.database import Base

class DeviceSales(Base):
    __tablename__ = "device_sales"
    id = Column(Integer, primary_key=True, index=True)
    bonbu = Column(String); team = Column(String)
    agency_code = Column(String); agency = Column(String)
    model_code = Column(String); model = Column(String)
    sale_cur = Column(Integer); revenue_cur = Column(Float)
    sale_prev = Column(Integer); revenue_prev = Column(Float)