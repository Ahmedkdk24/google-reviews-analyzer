# Google Reviews Analyzer

A Python pipeline for scraping Google Maps reviews, normalizing multilingual feedback, and extracting actionable insights using topic modeling and generative AI.

---

## Features

- **Scrape** reviews from Google Maps for multiple branches/locations.
- **Normalize** reviews: automatically translate Arabic reviews to English.
- **Analyze** reviews: discover topics, summarize sentiment, and generate recommendations using BERTopic and Google Gemini.

---

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/google-reviews-analyzer.git
cd google-reviews-analyzer
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Environment variables

Create a `.env` file in the root directory with the following keys:

```
DATABASE_URL=postgresql+psycopg2://postgres:admin@localhost:5432/gymagent
GOOGLE_GEMINI_API_KEY=your-gemini-api-key
USER_AGENT=Mozilla/5.0 (Windows NT 10.0; Win64; x64)
BRANCHES_JSON=branches.json
```

For Google Cloud Translation, set up your service account and download the credentials JSON file.  
Set the path in your `.env` or ensure this line is present in `reviews_normalization.py`:

```
GOOGLE_APPLICATION_CREDENTIALS=reviews-analyzer-service-account-key.json
```

---

## Usage

### 1. Prepare your branches list

Create a `branches.json` file containing a list of branches to scrape, e.g.:

```json
[
  {"name": "Gym Nation Kuwait", "url": "https://maps.google.com/maps/place/xyz"},
  {"name": "Another Branch", "url": "https://maps.google.com/maps/place/abc"}
]
```

### 2. Run the full pipeline

You can run the entire process (scraping, normalization, insight extraction) with one command:

```bash
python src/main_pipeline.py
```

Or run each step individually:

- **Scrape reviews:**  
  `python src/run_scraper.py`

- **Normalize reviews:**  
  `python src/reviews_normalization.py`

- **Extract insights:**  
  `python src/reviews_insight_pipeline.py`

---

## Output

- Reviews and insights are stored in your PostgreSQL database.
- Summaries and recommendations are printed to the console.

---

## Project Structure

```
src/
  ├── db.py
  ├── models.py
  ├── scrape_reviews.py
  ├── run_scraper.py
  ├── reviews_normalization.py
  ├── reviews_insight_pipeline.py
  ├── utils.py
  └── main_pipeline.py
requirements.txt
branches.json
.env
```

---

## Notes

- Requires Python 3.8+.
- Make sure Playwright browsers are installed:  
  `playwright install`
- Google Gemini and Google Cloud Translation require API keys/service account credentials.

---

## License

MIT License