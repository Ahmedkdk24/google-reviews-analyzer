# src/db.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL (or SUPABASE_DB_URL) environment variable is required")

# If Supabase provides a full URL like: postgres://user:pass@host:5432/dbname
# SQLAlchemy 2.0 may need "postgresql+psycopg2://..." — create_engine will accept postgres:// in modern SQLAlchemy
engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)