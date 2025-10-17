# reviews_insight_pipeline.py
import sys
import os
import json
import pandas as pd
import psycopg2
from bertopic import BERTopic
from google import generativeai as genai
from sklearn.feature_extraction.text import CountVectorizer
from datetime import datetime
from typing import List, Dict # Added for type hints

sys.stdout.reconfigure(encoding='utf-8')
# Assume database objects are accessible from project root structure
try:
    from src.db import SessionLocal
    from src.models import Branch, Review
    from sqlalchemy import func
except ImportError:
    pass 

# --- Database Setup (Connection to be used by psycopg2) ---
def get_psycopg2_connection():
    """Returns a raw psycopg2 connection."""
    return psycopg2.connect(
        dbname="gymagent",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )

def ensure_tables_exist():
    """Checks for and creates the insights_meta and insights tables if they do not exist."""
    conn = get_psycopg2_connection()
    cur = conn.cursor()
    
    print("Ensuring insights tables exist...")

    # Table 1: insights_meta
    meta_table_creation = """
    CREATE TABLE IF NOT EXISTS insights_meta (
        meta_id SERIAL PRIMARY KEY,
        branch_id INTEGER NOT NULL,
        branch_name VARCHAR(255) NOT NULL,
        analysis_date TIMESTAMP WITH TIME ZONE DEFAULT now(),
        number_of_reviews_processed INTEGER NOT NULL,
        number_of_topics INTEGER NOT NULL,
        bertopic_parameters JSONB
    );
    """
    # Table 2: insights (Note: insights_meta must exist first due to Foreign Key)
    insights_table_creation = """
    CREATE TABLE IF NOT EXISTS insights (
        insight_id SERIAL PRIMARY KEY,
        meta_id INTEGER NOT NULL REFERENCES insights_meta(meta_id), 
        topic_id INTEGER NOT NULL,
        percentage NUMERIC(5, 2) NOT NULL,
        top_keywords TEXT,
        gemini_aspect VARCHAR(255),
        gemini_sentiment VARCHAR(50),
        gemini_summary TEXT,
        gemini_recommendation TEXT
    );
    """
    try:
        cur.execute(meta_table_creation)
        cur.execute(insights_table_creation)
        conn.commit()
        print("Insights tables confirmed/created.")
    except Exception as e:
        print(f"FATAL ERROR during table creation: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def fetch_branches():
    """Fetch all branches to process."""
    session = SessionLocal()
    try:
        branches = session.query(Branch).all()
        return branches
    finally:
        session.close()

def fetch_normalized_reviews(branch_id: int) -> pd.DataFrame:
    """Fetch normalized (English) reviews for a specific branch."""
    conn = get_psycopg2_connection()
    # QUERY UPDATED: Uses branch_id directly from the normalized_reviews table
    query = f"""
        SELECT 
            review_id, 
            normalized_text 
        FROM normalized_reviews 
        WHERE branch_id = {branch_id};
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# -----------------------------
# 2. Topic modeling with BERTopic
# -----------------------------
def extract_topics(df: pd.DataFrame):
    if df.empty:
        return df, None, {}

    print(f"Extracting topics from {len(df)} reviews...")

    # Set parameters for better reproducibility and explicit storage
    MIN_TOPIC_SIZE = 5 
    NGRAM_RANGE = (1, 2)
    
    vectorizer_model = CountVectorizer(
        stop_words="english", 
        ngram_range=NGRAM_RANGE, 
        min_df=1
    )

    topic_model = BERTopic(
        embedding_model=None,         # CPU-friendly
        vectorizer_model=vectorizer_model,
        language="english",
        min_topic_size=MIN_TOPIC_SIZE,
        calculate_probabilities=False
    )

    topics, _ = topic_model.fit_transform(df["normalized_text"])
    df["topic"] = topics

    topic_info = topic_model.get_topic_info()
    print("Top topics discovered:")
    print(topic_info.head())

    # Package metadata for saving
    meta_params = {
        "min_topic_size": MIN_TOPIC_SIZE,
        "ngram_range": NGRAM_RANGE
    }

    return df, topic_model, meta_params

# -----------------------------
# 3. Summarization and sentiment analysis with Gemini
# -----------------------------
def analyze_topics_with_gemini(df: pd.DataFrame, topic_model: BERTopic) -> List[Dict]:
    """Use Google Gemini to summarize each topic and extract insights."""
    # API key check is a good practice here
    api_key = os.getenv("GOOGLE_GEMINI_API_KEY")
    if not api_key:
        print("ERROR: GOOGLE_GEMINI_API_KEY not found. Skipping analysis.")
        return []

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(model_name="gemini-2.5-flash")
    summaries = []
    
    relevant_df = df[df["topic"] != -1]
    if relevant_df.empty:
        return []
        
    topic_counts = relevant_df["topic"].value_counts(normalize=True) * 100 
    
    for topic_id, pct in topic_counts.items():
        topic_keywords = ", ".join([kw for kw, _ in topic_model.get_topic(topic_id)])
        topic_reviews = df[df["topic"] == topic_id]["normalized_text"].tolist()[:20] 

        prompt = f"""
        You are analyzing customer feedback for a gym chain.
        Topic keywords: {topic_keywords}
        Sample reviews: {topic_reviews}

        Tasks:
        1. Identify what customers are talking about (aspect or issue).
        2. Summarize whether feedback is mostly positive or negative.
        3. Estimate what customers want or expect.
        4. Suggest clear recommendations for improvement.
        5. Return a short summary in this JSON format:

        {{
            "aspect": "...",
            "sentiment": "positive|negative|mixed",
            "summary": "...",
            "recommendation": "..."
        }}
        """

        try:
            response = model.generate_content(prompt)
            
            try:
                # Clean and parse the response text
                analysis_text = response.text.strip()
                if analysis_text.startswith("```json"):
                    analysis_text = analysis_text[7:].strip()
                if analysis_text.endswith("```"):
                    analysis_text = analysis_text[:-3].strip()
                analysis_data = json.loads(analysis_text)
            except (json.JSONDecodeError, AttributeError) as e:
                print(f"JSON Parsing Error for topic {topic_id}: {e}")
                analysis_data = {
                    "aspect": "Parsing Error",
                    "sentiment": "mixed",
                    "summary": f"Could not parse Gemini JSON. Raw response: {response.text}",
                    "recommendation": "Inspect raw output."
                }
            
            summaries.append({
                "topic_id": int(topic_id),
                "percentage": round(pct, 2),
                "top_keywords": topic_keywords,
                "analysis": analysis_data
            })
        except Exception as e:
            print(f"Error for topic {topic_id}: {e}")
            summaries.append({
                "topic_id": int(topic_id),
                "percentage": round(pct, 2),
                "top_keywords": topic_keywords,
                "analysis": {
                    "aspect": "AI Generation Error",
                    "sentiment": "mixed",
                    "summary": f"Error generating summary: {e}",
                    "recommendation": "Rerun pipeline."
                }
            })

    return summaries

# -----------------------------
# 4. Persistence: Save to DB
# -----------------------------
def save_insights_to_db(branch_id: int, branch_name: str, df: pd.DataFrame, 
                        summaries: List[Dict], meta_params: Dict):
    """Saves the metadata and detailed insights to the PostgreSQL tables."""
    conn = get_psycopg2_connection()
    cur = conn.cursor()
    
    # Calculate metrics
    num_reviews = len(df)
    num_topics = len(summaries) 
    
    # 1. Insert into insights_meta
    meta_sql = """
    INSERT INTO insights_meta 
        (branch_id, branch_name, number_of_reviews_processed, number_of_topics, bertopic_parameters) 
    VALUES 
        (%s, %s, %s, %s, %s) 
    RETURNING meta_id;
    """
    try:
        cur.execute(meta_sql, (
            branch_id, 
            branch_name, 
            num_reviews, 
            num_topics, 
            json.dumps(meta_params)
        ))
        meta_id = cur.fetchone()[0]
        print(f"Saved metadata for Branch {branch_id}. Meta ID: {meta_id}")
    except Exception as e:
        print(f"ERROR saving to insights_meta: {e}")
        conn.rollback()
        cur.close()
        conn.close()
        return

    # 2. Insert into insights
    insight_sql = """
    INSERT INTO insights 
        (meta_id, topic_id, percentage, top_keywords, 
         gemini_aspect, gemini_sentiment, gemini_summary, gemini_recommendation) 
    VALUES 
        (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    insight_records = []
    for item in summaries:
        analysis = item["analysis"]
        insight_records.append((
            meta_id,
            item["topic_id"],
            item["percentage"],
            item["top_keywords"],
            analysis.get("aspect", ""),
            analysis.get("sentiment", ""),
            analysis.get("summary", ""),
            analysis.get("recommendation", "")
        ))

    try:
        cur.executemany(insight_sql, insight_records)
        print(f"Successfully saved {len(insight_records)} topics to 'insights' table.")
        conn.commit()
    except Exception as e:
        print(f"ERROR saving to insights table: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

# -----------------------------
# 5. Main pipeline
# -----------------------------
def main():
    print("Starting Reviews Insight Pipeline...")
    
    # NEW STEP: Ensure tables are ready
    try:
        ensure_tables_exist()
    except Exception:
        # Stop execution if table creation fails
        return

    # 1. Fetch all branches to process
    try:
        branches = fetch_branches()
    except Exception as e:
        print(f"FATAL: Could not fetch branches. Ensure DB connection and models are set up. Error: {e}")
        return

    print(f"Found {len(branches)} branches to process.")

    for branch in branches:
        print("\n" + "="*50)
        print(f"Processing Branch ID: {branch.id} | Name: {branch.name}")
        print("="*50)
        
        # 2. Fetch data
        df = fetch_normalized_reviews(branch.id)
        
        if df.empty:
            print(f"No normalized reviews found for {branch.name}. Skipping.")
            continue
        
        # 3. Topic modeling
        df_with_topics, topic_model, meta_params = extract_topics(df)
        
        if not topic_model:
            print(f"BERTopic model could not be created (too few reviews). Skipping.")
            continue

        # 4. Summarization and sentiment (only for non-outlier topics)
        summaries = analyze_topics_with_gemini(df_with_topics, topic_model)

        # 5. Persistence
        if summaries:
            save_insights_to_db(branch.id, branch.name, df_with_topics, summaries, meta_params)
        else:
            print(f"No non-outlier topics found for {branch.name}. Skipping DB save.")


if __name__ == "__main__":
    main()