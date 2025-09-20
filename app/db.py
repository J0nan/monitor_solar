# app/db.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

DB_URL = os.environ.get("DB_URL", "sqlite:///./data/energy.db")

_engine = create_engine(DB_URL, connect_args={"check_same_thread": False} if DB_URL.startswith("sqlite") else {})
SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False)

def get_engine():
    return _engine
