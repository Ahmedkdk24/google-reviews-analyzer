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
    """Fetch review_id and text from the reviews table."""
    conn = get_connection()
    query = "SELECT id, text FROM reviews;"
    df = pd.read_sql_query(query, conn)
    conn.close()
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
    for text in df["text"]:
        try:
            lang = detect(text)
            if lang == "ar":  # Arabic
                translated = translate_text(text)
                normalized_texts.append(translated)
            else:
                normalized_texts.append(text)
        except Exception as e:
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

    # Create new table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS normalized_reviews (
        review_id SERIAL PRIMARY KEY,
        original_text TEXT,
        normalized_text TEXT
    );
    """
    cur.execute(create_table_query)
    conn.commit()

    # Insert data
    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO normalized_reviews (review_id, original_text, normalized_text)
            VALUES (%s, %s, %s)
            ON CONFLICT (review_id) DO UPDATE
            SET normalized_text = EXCLUDED.normalized_text;
            """,
            (row["id"], row["text"], row["normalized_text"])
        )
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
