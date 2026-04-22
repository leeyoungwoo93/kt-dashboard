import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# 1. Railway의 DATABASE_URL 우선 확인, 없으면 로컬 SQLite 사용
SQLALCHEMY_DATABASE_URL = os.getenv("DATABASE_URL")

if SQLALCHEMY_DATABASE_URL:
    # Railway의 postgres:// 형식을 sqlalchemy용 postgresql://로 변경
    if SQLALCHEMY_DATABASE_URL.startswith("postgres://"):
        SQLALCHEMY_DATABASE_URL = SQLALCHEMY_DATABASE_URL.replace("postgres://", "postgresql://", 1)
    
    # Postgres 연결 설정
    engine = create_engine(SQLALCHEMY_DATABASE_URL)
else:
    # 로컬 SQLite 연결 설정
    SQLALCHEMY_DATABASE_URL = "sqlite:///./kt_sales.db"
    engine = create_engine(
        SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
    )

# 공통 Session 및 Base 설정
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()