from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# 데이터베이스 파일 경로 (현재 폴더에 kt_sales.db라는 이름으로 생성됨)
SQLALCHEMY_DATABASE_URL = "sqlite:///./kt_sales.db"

# engine, SessionLocal, Base 이 세 가지 이름이 정확히 있어야 합니다
engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()