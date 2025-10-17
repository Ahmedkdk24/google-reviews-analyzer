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
    Fetch recent insights and metadata from insights_meta + insights tables.
    If no data exists, fall back to aggregated reviews.
    """
    from src.models import Insight, InsightMeta
    from sqlalchemy.orm import joinedload
    from sqlalchemy import func

    session = SessionLocal()
    try:
        # Check if there’s any record in insights_meta
        meta_exists = session.query(InsightMeta).count()
        if meta_exists == 0:
            logger.info("No rows found in insights_meta; building fallback aggregated insights from reviews.")
            return build_fallback_from_reviews(session, limit_branches)

        # Get recent meta entries and join their insights
        metas = (
            session.query(InsightMeta)
            .order_by(InsightMeta.analysis_date.desc())
            .limit(limit_branches)
            .all()
        )

        results = []
        for meta in metas:
            insights = (
                session.query(Insight)
                .filter(Insight.meta_id == meta.meta_id)
                .order_by(Insight.topic_id.asc())
                .all()
            )

            insights_data = [
                {
                    "topic_id": i.topic_id,
                    "percentage": float(i.percentage),
                    "top_keywords": i.top_keywords,
                    "aspect": i.gemini_aspect,
                    "sentiment": i.gemini_sentiment,
                    "summary": i.gemini_summary,
                    "recommendation": i.gemini_recommendation,
                }
                for i in insights
            ]

            results.append({
                "branch_id": meta.branch_id,
                "branch_name": meta.branch_name,
                "analysis_date": meta.analysis_date.isoformat(),
                "topics": insights_data,
            })

        logger.info("Returning %d insights_meta records from DB.", len(results))
        return {"type": "insights_table", "branches": results}

    except Exception as e:
        logger.error("Error fetching insights: %s", e)
        return build_fallback_from_reviews(session, limit_branches)
    finally:
        session.close()

@app.get("/")
def home():
    return {"message": "Google Reviews Analyzer Agent - online."}

@app.post("/run")
def run_and_return_insights():
    """
    Check if insights are already available in DB.
    If yes → return them immediately.
    If no → run the pipeline, then fetch and return results.
    """
    try:
        # Step 1: Try fetching existing insights
        insights = fetch_insights_from_db(limit_branches=20)
        if insights and insights.get("type") == "insights_table":
            logger.info("Returning cached insights from DB (no pipeline run).")
            return {"status": "success", "insights": insights}

        # Step 2: Otherwise, run pipeline and then fetch insights
        logger.info("No cached insights found; running pipeline...")
        run_result = run_pipeline()

        if run_result["returncode"] != 0:
            return JSONResponse(
                content={
                    "status": "error",
                    "message": "Pipeline failed",
                    "stderr": run_result["stderr"],
                    "stdout": run_result["stdout"],
                },
                status_code=500,
            )

        # Step 3: Fetch new insights from DB after successful pipeline
        insights = fetch_insights_from_db(limit_branches=20)
        return {
            "status": "success",
            "insights": insights,
            "pipeline_stdout": run_result["stdout"],
        }

    except Exception as exc:
        logger.exception("Agent error")
        return JSONResponse(
            content={"status": "error", "message": str(exc)}, status_code=500
        )


if __name__ == "__main__":
    # run with: python agent_server.py
    uvicorn.run("agent_server:app", host="0.0.0.0", port=8000, reload=False)
