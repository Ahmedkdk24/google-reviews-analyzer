# reviews_normalization.py

import psycopg2
import pandas as pd
from langdetect import detect
from google.cloud import translate_v2 as translate
from dotenv import load_dotenv
import os


# Load .env file
load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gym-agent-service-account-key.json"

# -----------------------------
# 1. PostgreSQL connection setup
# -----------------------------
def get_connection():
    """Create and return a PostgreSQL connection."""
    return psycopg2.connect(
        dbname="gymagent",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )

# -----------------------------
# 2. Fetch reviews from DB
# -----------------------------
def fetch_reviews():
    """Fetch all necessary columns from the reviews table."""
    conn = get_connection()
    # Fetch all relevant columns:
    query = """
        SELECT 
            review_id, 
            branch_id, 
            author, 
            rating, 
            text, 
            review_date, 
            scraped_at 
        FROM reviews;
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    # Rename 'review_id' column to 'id' temporarily for compatibility with original code structure
    df.rename(columns={'review_id': 'id', 'text': 'original_text'}, inplace=True)
    return df

# -----------------------------
# 3. Translate Arabic reviews
# -----------------------------
def translate_text(text, target_language="en"):
    """Translate text using Google Translate API."""
    translate_client = translate.Client()
    result = translate_client.translate(text, target_language=target_language)
    return result["translatedText"]

def normalize_reviews(df):
    """Translate Arabic reviews and keep English ones as is."""
    normalized_texts = []
    # Loop over the 'original_text' column
    for text in df["original_text"]:
        try:
            lang = detect(text)
            if lang == "ar":  # Arabic
                translated = translate_text(text)
                normalized_texts.append(translated)
            else:
                normalized_texts.append(text)
        except Exception as e:
            # Fallback to original text on error
            print(f"Error translating: {e}")
            normalized_texts.append(text)
            
    df["normalized_text"] = normalized_texts
    return df

# -----------------------------
# 4. Store normalized reviews
# -----------------------------
def save_normalized_reviews(df):
    """Save normalized reviews to a new table in PostgreSQL."""
    conn = get_connection()
    cur = conn.cursor()

    # --- UPDATED Create new table with all columns ---
    create_table_query = """
    CREATE TABLE IF NOT EXISTS normalized_reviews (
        review_id INTEGER PRIMARY KEY,
        branch_id INTEGER,
        author VARCHAR(256),
        rating INTEGER,
        original_text TEXT,
        normalized_text TEXT,
        review_date VARCHAR(64),
        scraped_at TIMESTAMP WITHOUT TIME ZONE
    );
    """
    cur.execute(create_table_query)
    conn.commit()
    # -----------------------------------------------

    # Insert data
    for _, row in df.iterrows():
        # --- UPDATED INSERT query to include all columns ---
        cur.execute(
            """
            INSERT INTO normalized_reviews 
                (review_id, branch_id, author, rating, original_text, normalized_text, review_date, scraped_at)
            VALUES 
                (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (review_id) DO UPDATE
            SET normalized_text = EXCLUDED.normalized_text,
                branch_id = EXCLUDED.branch_id,
                author = EXCLUDED.author,
                rating = EXCLUDED.rating,
                original_text = EXCLUDED.original_text,
                review_date = EXCLUDED.review_date,
                scraped_at = EXCLUDED.scraped_at;
            """,
            (
                row["id"], 
                row["branch_id"], 
                row["author"], 
                row["rating"], 
                row["original_text"], # original text is now included
                row["normalized_text"], 
                row["review_date"], 
                row["scraped_at"]
            )
        )
        # ----------------------------------------------------
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Normalized reviews saved to database.")

# -----------------------------
# 5. Main entry point
# -----------------------------
def main():
    print("Fetching reviews...")
    df = fetch_reviews()
    print(f"Fetched {len(df)} reviews.")

    print("Translating Arabic reviews...")
    normalized_df = normalize_reviews(df)

    print("Saving normalized reviews...")
    save_normalized_reviews(normalized_df)
    print("Done!")

if __name__ == "__main__":
    main()