# agent_server.py
import os
import subprocess
import sys
from typing import Any, Dict, List

from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn
import json
import logging

# Import your project's DB session & models (adjust import path if needed)
try:
    from src.db import SessionLocal
    from src.models import Branch, Review
except Exception as e:
    raise RuntimeError("Failed to import src.db or src.models. Run this from repo root.") from e

app = FastAPI(title="Google Reviews Analyzer Agent")
logger = logging.getLogger("agent_server")
logging.basicConfig(level=logging.INFO)

PIPELINE_PATH = "src/main_pipeline.py"   # the orchestrator you already have

def run_pipeline() -> Dict[str, Any]:
    """
    Run the main pipeline script. Returns dict with status and stdout/stderr.
    """
    logger.info("Running pipeline: %s", PIPELINE_PATH)
    result = subprocess.run(
        [sys.executable, PIPELINE_PATH],
        capture_output=True,
        text=True,
        cwd=os.getcwd()
    )
    return {
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }

def fetch_insights_from_db(limit_branches: int = 10) -> Dict[str, Any]:
    """
    Try to fetch prepared insights from an 'insights' table if present.
    If that table is not present, fall back to aggregating reviews per branch
    and returning counts + a few sample reviews (per branch).
    """
    session = SessionLocal()
    try:
        # First try: does an 'insights' table exist? (common pattern)
        # We'll do a simple check by attempting a raw SQL select.
        try:
            # If you have an 'insights' table with a jsonb 'payload' column:
            res = session.execute("SELECT id, created_at, payload FROM insights ORDER BY created_at DESC LIMIT 1;")
            row = res.fetchone()
            if row:
                logger.info("Found insights row in 'insights' table, returning payload.")
                # payload may already be JSON (jsonb) - convert if needed
                payload = row["payload"] if isinstance(row["payload"], dict) else json.loads(row["payload"])
                return {"type": "insights_table", "payload": payload}
        except Exception:
            # table doesn't exist or query failed -> fall back
            pass

        # Fallback aggregation: build simple insights per branch
        logger.info("No insights table found; building fallback aggregated insights from reviews.")
        branches = session.query(Branch).order_by(Branch.scraped_at.desc()).limit(limit_branches).all()
        out = []
        for b in branches:
            # basic aggregates: total reviews, avg rating, sample top 5 reviews
            reviews_q = session.query(Review).filter(Review.branch_id == b.id)
            total = reviews_q.count()
            avg_rating = None
            try:
                avg_rating = float(session.query(func.avg(Review.rating)).filter(Review.branch_id == b.id).scalar() or 0)
            except Exception:
                # import func lazily if not available
                from sqlalchemy import func
                avg_rating = float(session.query(func.avg(Review.rating)).filter(Review.branch_id == b.id).scalar() or 0)

            samples = reviews_q.order_by(Review.id.desc()).limit(5).all()
            sample_list = [{"author": r.author, "rating": r.rating, "text": r.text[:500]} for r in samples]
            out.append({
                "branch_id": b.id,
                "branch_name": b.name,
                "url": b.url,
                "total_reviews": total,
                "avg_rating": avg_rating,
                "sample_reviews": sample_list
            })
        return {"type": "aggregated_fallback", "branches": out}
    finally:
        session.close()

@app.get("/")
def home():
    return {"message": "Google Reviews Analyzer Agent - online."}

@app.post("/run")
def run_and_return_insights():
    """
    Run the pipeline, then fetch the latest insights from Postgres and return JSON.
    """
    try:
        run_result = run_pipeline()
        if run_result["returncode"] != 0:
            return JSONResponse(
                content={
                    "status": "error",
                    "message": "Pipeline failed",
                    "stderr": run_result["stderr"],
                    "stdout": run_result["stdout"],
                },
                status_code=500
            )

        # pipeline succeeded: fetch insights from DB
        insights = fetch_insights_from_db(limit_branches=20)
        return {"status": "success", "insights": insights, "pipeline_stdout": run_result["stdout"]}
    except Exception as exc:
        logger.exception("Agent error")
        return JSONResponse(content={"status": "error", "message": str(exc)}, status_code=500)


if __name__ == "__main__":
    # run with: python agent_server.py
    uvicorn.run("agent_server:app", host="0.0.0.0", port=8000, reload=False)
