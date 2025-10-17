# src/utils.py
from src.models import Base
from src.db import engine

def init_db():
    Base.metadata.create_all(bind=engine)
