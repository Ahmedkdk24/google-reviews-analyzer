# src/create_tables.py
from src.db import engine
from src.models import Base



def ensure_tables_exist():
    """
    Create tables in the connected Postgres database according to src.models.Base.
    """
    print("Dropping and Creating tables...")
    Base.metadata.drop_all(bind=engine)
    print("Creating tables (if not exist)...")
    Base.metadata.create_all(bind=engine)
    print("Done.")

if __name__ == "__main__":
    ensure_tables_exist()
