# streamlit_app.py - Google Reviews Analyzer
import os
import time
import json
import traceback
import tempfile
import subprocess
import sys
from typing import List, Dict

import streamlit as st
import pandas as pd

# Attempt to import your project modules
try:
    from src.reviews_normalization import normalize_reviews
except Exception:
    normalize_reviews = None

try:
    from src.reviews_insight_pipeline import (
        extract_topics,
        analyze_topics_with_gemini,
        save_insights_to_db,
        ensure_tables_exist,
    )
except Exception:
    extract_topics = None
    analyze_topics_with_gemini = None
    save_insights_to_db = None
    ensure_tables_exist = None

try:
    from src.db import SessionLocal
    from src.models import Branch, Review
except Exception:
    SessionLocal = None
    Branch = None
    Review = None

# Utilities
def get_session():
    if SessionLocal is None:
        raise RuntimeError("SessionLocal not available. Ensure src.db exists and is importable.")
    return SessionLocal()

# Streamlit config
st.set_page_config(page_title="Google Reviews Analyzer", layout="wide")
st.title("Google Reviews Analyzer — Streamlit Dashboard")
st.markdown(
    "Provide branch name(s) and Google Maps URL(s). "
    "The app will scrape reviews, normalize text, run topic modeling, and save insights to Postgres."
)

# Sidebar input
st.sidebar.header("Input branches")
num_branches = st.sidebar.number_input("How many branches to analyze?", min_value=1, max_value=10, value=1)
branches_input: List[Dict] = []
for i in range(num_branches):
    st.sidebar.markdown(f"**Branch {i+1}**")
    name = st.sidebar.text_input(f"Branch {i+1} name", key=f"name_{i}")
    url = st.sidebar.text_input(f"Branch {i+1} Google Maps URL", key=f"url_{i}")
    if name and url:
        branches_input.append({"name": name, "url": url})

# Options
st.sidebar.header("Options")
max_reviews = st.sidebar.number_input(
    "Max reviews per branch",
    min_value=10,
    max_value=500,
    value=int(os.getenv("MAX_REVIEWS_PER_BRANCH", "50")),
)
headless = st.sidebar.checkbox(
    "Scrape Reviews",
    value=os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() in ("1", "true", "yes"),
)
run_bertopic = st.sidebar.checkbox("Topic Modelling", value=True)
run_gemini = st.sidebar.checkbox("Extract insights and provide recommendations", value=False)

st.sidebar.markdown("---")
st.sidebar.write("Environment variables used: MAX_REVIEWS_PER_BRANCH, PLAYWRIGHT_HEADLESS, GOOGLE_GEMINI_API_KEY")

# Main actions
col1, col2 = st.columns([1, 2])
with col1:
    if st.button("Run Scrape & Analyze"):
        if not branches_input:
            st.error("Please provide at least one branch name and URL in the sidebar.")
        else:
            os.environ["MAX_REVIEWS_PER_BRANCH"] = str(max_reviews)
            os.environ["PLAYWRIGHT_HEADLESS"] = "true" if headless else "false"

            # Ensure insights tables exist
            if ensure_tables_exist:
                try:
                    ensure_tables_exist()
                except Exception:
                    st.warning("ensure_tables_exist() failed — continuing, but DB tables might not exist.")

            # --- Run scraper as subprocess to avoid asyncio/Playwright issues ---
            st.info("Starting scraping (runs in separate process)...")
            progress = st.progress(0)
            status_area = st.empty()

            try:
                # Save branches_input to a temporary JSON file
                with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as f:
                    json.dump(branches_input, f)
                    f.flush()
                    temp_file = f.name

                # Open subprocess
                proc = subprocess.Popen(
                    [sys.executable, "-m", "src.run_scraper", temp_file],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                )

                results = []
                total_branches = len(branches_input)
                current_branch = 0

                for line in proc.stdout:
                    line = line.strip()
                    if not line:
                        continue
                    status_area.text(line)

                    # If your scraper prints something like: "Processed branch X"
                    if "Processed branch" in line:
                        current_branch += 1
                        pct = int(current_branch / max(1, total_branches) * 100)
                        progress.progress(pct)

                proc.wait()
                if proc.returncode != 0:
                    st.error("Scraper failed!")
                    st.text(f"Return code: {proc.returncode}")
                else:
                    st.success("Scraping completed successfully!")

            except Exception as e:
                st.error(f"Scraper subprocess failed: {e}")

            # Show progress for each branch
            for idx, r in enumerate(results):
                pct = int((idx + 1) / max(1, len(results)) * 100)
                progress.progress(pct)
                if r.get("skipped"):
                    status_area.warning(f"{r.get('name')} skipped: {r.get('reason')}")
                else:
                    status_area.success(f"{r.get('name')}: {r.get('found', 0)} reviews scraped")
                time.sleep(0.1)

            # --- Normalization, Topic Modeling, Gemini ---
            st.info("Running normalization and insights per branch...")
            overall_progress = st.progress(0)
            branch_tabs = st.tabs([b.get("name") or b.get("url") for b in branches_input])

            for i, b in enumerate(branches_input):
                with branch_tabs[i]:
                    st.header(b.get("name"))
                    try:
                        session = None
                        try:
                            session = get_session()
                            branch_rec = session.query(Branch).filter(Branch.url == b.get("url")).first()
                        except Exception:
                            branch_rec = None
                        finally:
                            if session:
                                session.close()

                        if not branch_rec:
                            st.warning("Branch record not found in DB. Ensure scraping successfully saved it.")
                            continue

                        branch_id = branch_rec.id

                        st.subheader("Normalization")
                        try:
                            session = get_session()

                            # Fetch freshly scraped reviews for this branch
                            query = f"""
                                SELECT review_id, branch_id, author, rating, text AS original_text, review_date, scraped_at
                                FROM reviews
                                WHERE branch_id = {branch_id};
                            """
                            df_raw = pd.read_sql(query, session.bind)

                            if df_raw.empty:
                                st.warning("No raw reviews found for this branch. Ensure scraping completed successfully.")
                                normalized_df = pd.DataFrame()
                            else:
                                # Normalize freshly scraped reviews
                                if normalize_reviews:
                                    normalized_df = normalize_reviews(df_raw)
                                else:
                                    # Fallback simple normalization
                                    normalized_df = df_raw.copy()
                                    normalized_df["normalized_text"] = (
                                        normalized_df["original_text"].astype(str)
                                        .str.replace("\n", " ")
                                        .str.replace(r"\s+", " ", regex=True)
                                        .str.strip()
                                        .str.lower()
                                    )

                                # Save normalized reviews into DB for caching
                                try:
                                    from src.reviews_normalization import save_normalized_reviews
                                    save_normalized_reviews(normalized_df)
                                except Exception as e:
                                    st.warning(f"Failed to save normalized reviews to DB: {e}")
                        finally:
                            session.close()

                        st.write(f"Normalized reviews: {len(normalized_df)}")
                        if normalized_df.empty:
                            st.warning("No normalized reviews to analyze for this branch.")
                            continue

                        # Topic modeling
                        st.subheader("Topic modeling")
                        if not run_bertopic:
                            st.info("BERTopic skipped by user option.")
                            continue

                        if extract_topics:
                            try:
                                df_with_topics, topic_model, meta_params = extract_topics(normalized_df)
                            except Exception as e:
                                st.error(f"extract_topics failed: {e}")
                                df_with_topics = normalized_df.copy()
                                topic_model = None
                                meta_params = {}
                        else:
                            st.info("extract_topics not available, using fallback keyword heuristic.")
                            df_with_topics = normalized_df.copy()
                            df_with_topics["topic"] = -1
                            topic_model = None
                            meta_params = {}

                        st.write(df_with_topics.head(5))

                        # Gemini summarization
                        st.subheader("Summarization & Recommendations (Gemini)")
                        summaries = []
                        if run_gemini and analyze_topics_with_gemini:
                            try:
                                summaries = analyze_topics_with_gemini(df_with_topics, topic_model)
                            except Exception as e:
                                st.error(f"analyze_topics_with_gemini failed: {e}")
                        elif run_gemini:
                            st.warning("Gemini analysis function not available; skipping.")
                        else:
                            st.info("Gemini analysis skipped by user option.")

                        st.write(summaries)

                        # Save insights
                        if save_insights_to_db and summaries:
                            try:
                                save_insights_to_db(branch_id, branch_rec.name, df_with_topics, summaries, meta_params)
                                st.success("Insights saved to DB.")
                            except Exception as e:
                                st.error(f"save_insights_to_db failed: {e}")

                    except Exception as e:
                        st.error(f"Error processing branch {b.get('name')}: {e}")
                        st.exception(traceback.format_exc())

                overall_progress.progress(int((i + 1) / max(1, len(branches_input)) * 100))

            st.success("All selected branches processed.")

with col2:
    st.header("Existing branches & insights")
    try:
        session = get_session()
        branches_in_db = session.query(Branch).order_by(Branch.scraped_at.desc()).all()
        session.close()
    except Exception:
        branches_in_db = []

    if branches_in_db:
        options = {b.name: b.id for b in branches_in_db}
        selected = st.selectbox("Choose a branch to view insights", options.keys())
        branch_id = options[selected]

        try:
            session = get_session()
            engine = session.bind
            session.close()
            query = f"""
                SELECT i.topic_id, i.percentage, i.top_keywords, i.gemini_aspect, 
                       i.gemini_sentiment, i.gemini_summary, i.gemini_recommendation
                FROM insights i
                JOIN insights_meta m ON i.meta_id = m.meta_id
                WHERE m.branch_id = {branch_id}
                ORDER BY i.topic_id;
            """
            insights_df = pd.read_sql(query, engine)
        except Exception:
            insights_df = pd.DataFrame()

        if not insights_df.empty:
            st.subheader("Insights (topics)")
            st.dataframe(insights_df)
            try:
                import altair as alt
                chart = alt.Chart(insights_df).mark_bar().encode(
                    x=alt.X('topic_id:O', title='Topic ID'),
                    y=alt.Y('percentage:Q', title='Percentage'),
                    tooltip=['top_keywords', 'gemini_aspect', 'gemini_sentiment']
                )
                st.altair_chart(chart, use_container_width=True)
            except Exception:
                st.info("Altair chart failed.")
            csv = insights_df.to_csv(index=False)
            st.download_button("Download insights CSV", csv, file_name=f"insights_branch_{branch_id}.csv")
        else:
            st.info("No insights found for this branch yet.")
    else:
        st.info("No branches found in DB. Use the left panel to scrape branches first.")

st.markdown("---")
st.caption("This app expects your project modules under `src/` and a working Postgres connection via `src.db.SessionLocal`.")
