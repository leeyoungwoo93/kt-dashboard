from sqlalchemy import Column, Integer, String, Float
from app.database import Base

class Inventory(Base):
    __tablename__ = "inventory"
    id = Column(Integer, primary_key=True, index=True)
    date = Column(String)
    model = Column(String)
    total = Column(Integer)
    jisa = Column(Integer)
    younggi = Column(Integer)
    strategic = Column(Integer)
    ms = Column(Integer)
    ktshop = Column(Integer)
    other = Column(Integer)