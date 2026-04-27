import os
import shutil
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "sqlite:///./kt_dashboard.db"
)

# Railway PostgreSQL URL 호환
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)


def _sqlite_path_from_url(url: str):
    """SQLAlchemy sqlite URL에서 실제 파일 경로를 계산한다."""
    if not url.startswith("sqlite:///"):
        return None
    raw = url.replace("sqlite:///", "", 1)
    if raw in ("", ":memory:"):
        return None
    if raw.startswith("/"):
        return raw
    return os.path.abspath(raw)


def _bootstrap_seed_sqlite():
    """번들 seed DB가 있으면 빈 SQLite DB를 선적재 DB로 교체한다.

    v4에서 엑셀 자동 적재를 꺼서 운영 화면이 빈 화면으로 뜬 문제가 있었다.
    v5는 app/seed/kt_dashboard_seed.db를 포함하고, 기본 SQLite 파일이 없거나
    사실상 빈 DB일 때만 seed를 복사한다.
    """
    target = _sqlite_path_from_url(DATABASE_URL)
    if not target:
        return

    seed = os.path.join(os.path.dirname(__file__), "seed", "kt_dashboard_seed.db")
    if not os.path.exists(seed):
        return

    try:
        force_seed = os.environ.get("FORCE_SEED_DB", "0").lower() in ("1", "true", "yes", "on")
        should_copy = force_seed or (not os.path.exists(target)) or os.path.getsize(target) < 1024 * 1024
        if should_copy:
            os.makedirs(os.path.dirname(target) or ".", exist_ok=True)
            shutil.copyfile(seed, target)
            print(f"[DB Seed] seeded SQLite database: {target}")
    except Exception as e:
        print(f"[DB Seed 오류] {e}")


_bootstrap_seed_sqlite()

connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, connect_args=connect_args, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
