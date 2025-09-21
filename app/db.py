# app/db.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
import urllib.parse
from pathlib import Path

DB_URL = os.environ.get("DB_URL", "sqlite:///./data/energy.db")

def _ensure_sqlite_dir(url: str):
    if not url.startswith("sqlite"):
        return
    # Extract filesystem path after sqlite:///
    raw_path = url.split("sqlite:///", 1)[-1]
    if not raw_path:
        return
    fs_path = Path(urllib.parse.unquote(raw_path))
    if not fs_path.is_absolute():
        fs_path = Path.cwd() / fs_path
    fs_path.parent.mkdir(parents=True, exist_ok=True)

_ensure_sqlite_dir(DB_URL)

_engine = create_engine(
    DB_URL,
    echo=os.environ.get("SQL_ECHO", "0") == "1",
    future=True,
    pool_pre_ping=True,
    connect_args={"check_same_thread": False} if DB_URL.startswith("sqlite") else {}
)
SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False, future=True)

def get_engine():
    return _engine
