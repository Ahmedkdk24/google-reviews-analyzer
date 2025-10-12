import os
import pandas as pd
import psycopg2
from bertopic import BERTopic
from google import generativeai as genai
from sklearn.feature_extraction.text import CountVectorizer

# -----------------------------
# 1. PostgreSQL connection
# -----------------------------
def get_connection():
    return psycopg2.connect(
        dbname="gymagent",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )

def fetch_normalized_reviews():
    """Fetch normalized (English) reviews."""
    conn = get_connection()
    df = pd.read_sql_query("SELECT review_id, normalized_text FROM normalized_reviews;", conn)
    conn.close()
    return df

# -----------------------------
# 2. Topic modeling with BERTopic
# -----------------------------
def extract_topics(df):
    print("Extracting topics from reviews...")

    vectorizer_model = CountVectorizer(stop_words="english", ngram_range=(1, 2), min_df=1)

    topic_model = BERTopic(
        embedding_model=None,         # CPU-friendly
        vectorizer_model=vectorizer_model,
        language="english",
        min_topic_size=2,
        calculate_probabilities=False
    )

    topics, _ = topic_model.fit_transform(df["normalized_text"])
    df["topic"] = topics

    topic_info = topic_model.get_topic_info()
    print("Top topics discovered:")
    print(topic_info.head())

    return df, topic_model

# -----------------------------
# 3. Summarization and sentiment analysis with Gemini
# -----------------------------
def analyze_topics_with_gemini(df, topic_model):
    """Use Google Gemini to summarize each topic and extract insights."""
    genai.configure(api_key=os.getenv("GOOGLE_GEMINI_API_KEY"))
    model = genai.GenerativeModel(model_name="gemini-2.5-flash")
    summaries = []
    topic_counts = df["topic"].value_counts(normalize=True) * 100  # percentage

    for topic_id, pct in topic_counts.items():
        if topic_id == -1:  # -1 = outliers
            continue

        topic_keywords = ", ".join([kw for kw, _ in topic_model.get_topic(topic_id)])
        topic_reviews = df[df["topic"] == topic_id]["normalized_text"].tolist()[:20]  # sample up to 20

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
            summaries.append({
                "topic_id": topic_id,
                "percentage": round(pct, 2),
                "analysis": response.text
            })
        except Exception as e:
            summaries.append({
                "topic_id": topic_id,
                "percentage": round(pct, 2),
                "analysis": f"Error generating summary: {e}"
            })

    return summaries

# -----------------------------
# 4. Display / Export summary
# -----------------------------
def display_results(summaries):
    print("\n==== Insight Summary ====")
    for item in summaries:
        print(f"\nTopic {item['topic_id']} ({item['percentage']}% of reviews):")
        print(item["analysis"])
        print("-" * 60)

# -----------------------------
# 5. Main pipeline
# -----------------------------
def main():
    print("Fetching reviews...")
    df = fetch_normalized_reviews()
    print(f"Fetched {len(df)} reviews.")

    df, topic_model = extract_topics(df)
    summaries = analyze_topics_with_gemini(df, topic_model)
    display_results(summaries)

if __name__ == "__main__":
    main()
