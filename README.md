# 🧠 Google Reviews Analyzer

The **Google Reviews Analyzer** is a Streamlit-based dashboard for scraping, normalizing, analyzing, and visualizing Google Maps reviews for multiple business branches.

It automatically:

* Scrapes reviews from Google Maps using Playwright.
* Normalizes and translate non-English reviews to English.
* Performs topic modeling (via BERTopic).
* Generates insights and summaries using Gemini (optional).
* Stores results in a PostgreSQL database for persistent access and visualization.

---

## 🚀 Features

* **Scrape Reviews** — Extract reviews from Google Maps branch URLs.
* **Normalize Text** — Translates and standardize review text for analysis.
* **Topic Modeling** — Identify recurring topics in customer feedback using BERTopic.
* **Gemini Summaries** *(optional)* — Generate AI-powered summaries, sentiments, and recommendations.
* **Database Integration** — Save raw reviews, normalized reviews, and insights to PostgreSQL.
* **Streamlit Dashboard** — Interactive, data-rich web interface for managing branches and insights.

---

## 🧩 Project Structure

```
project-root/
│
├── streamlit_app.py               # Main Streamlit dashboard
├── src/
│   ├── run_scraper.py             # Playwright-based scraper
│   ├── db.py                      # Database connection (SQLAlchemy)
│   ├── models.py                  # SQLAlchemy ORM models (Branch, Review, etc.)
│   ├── reviews_normalization.py   # Text normalization logic
│   ├── reviews_insight_pipeline.py# Topic modeling + Gemini pipeline
│   └── ...
├── requirements.txt
├── Dockerfile
└── README.md
```

---

## ⚙️ Local Setup

### 1. Clone the Repository

```bash
git clone https://github.com/<your-username>/google-reviews-analyzer.git
cd google-reviews-analyzer
```

### 2. Create and Activate Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Set Environment Variables

Create a `.env` file in your project root:

```bash
DATABASE_URL=postgresql+psycopg2://<user>:<password>@<host>:<port>/<database>
MAX_REVIEWS_PER_BRANCH=50
PLAYWRIGHT_HEADLESS=true
GOOGLE_GEMINI_API_KEY=<your-gemini-api-key>  # optional
```

Then load them:

```bash
export $(cat .env | xargs)
```

### 5. Initialize Playwright (for scraping)

```bash
playwright install
```

### 6. Run the App Locally

```bash
streamlit run streamlit_app.py
```

Open your browser to [http://localhost:8501](http://localhost:8501).

---

## 🐳 Deploying to Google Cloud Run

Cloud Run allows you to deploy this Streamlit app as a fully managed container.

### 1. Enable Required Services

```bash
gcloud services enable run.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com
```

### 2. Build the Docker Image

Create a `Dockerfile` (if not already present):

```Dockerfile
# Dockerfile
FROM python:3.10-slim

# Set workdir
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y git curl chromium chromium-driver

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app
COPY . .

# Streamlit configuration
ENV PORT=8080
EXPOSE 8080

# Streamlit runs on port 8080 for Cloud Run
CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8080", "--server.address=0.0.0.0"]
```

Then build and push to Artifact Registry (replace `<REGION>` and `<PROJECT_ID>`):

```bash
gcloud builds submit --tag <REGION>-docker.pkg.dev/<PROJECT_ID>/reviews-analyzer/app
```

### 3. Deploy to Cloud Run

```bash
gcloud run deploy google-reviews-analyzer \
  --image <REGION>-docker.pkg.dev/<PROJECT_ID>/reviews-analyzer/app \
  --platform managed \
  --region <REGION> \
  --allow-unauthenticated \
  --set-env-vars DATABASE_URL=$DATABASE_URL,MAX_REVIEWS_PER_BRANCH=50,PLAYWRIGHT_HEADLESS=true,GOOGLE_GEMINI_API_KEY=$GOOGLE_GEMINI_API_KEY
```

After deployment, Cloud Run will output a public URL like:

```
Service [google-reviews-analyzer] deployed to:
https://google-reviews-analyzer-<region>-a.run.app
```

Visit that URL to access your dashboard.

---

## 🧠 Environment Variables Summary

| Variable                 | Description                                | Required    |
| ------------------------ | ------------------------------------------ | ----------- |
| `DATABASE_URL`           | SQLAlchemy-compatible Postgres URL         | ✅           |
| `MAX_REVIEWS_PER_BRANCH` | Max reviews to fetch per branch            | ✅           |
| `PLAYWRIGHT_HEADLESS`    | Run browser in headless mode               | ✅           |
| `GOOGLE_GEMINI_API_KEY`  | API key for Gemini (if using AI summaries) | ⚙️ Optional |

---

## 🧱 Database Schema (Simplified)

| Table                | Purpose                                    |
| -------------------- | ------------------------------------------ |
| `branches`           | Stores branch name, URL, and metadata      |
| `reviews`            | Stores individual scraped reviews          |
| `normalized_reviews` | Cleaned and processed text data            |
| `insights`           | Topic modeling and Gemini analysis results |
| `insights_meta`      | Metadata linking topics to branches        |

---

## 🧪 Troubleshooting

| Issue                          | Possible Fix                                            |
| ------------------------------ | ------------------------------------------------------- |
| **Playwright errors**          | Run `playwright install chromium`                       |
| **Database connection errors** | Verify `DATABASE_URL` is reachable from Cloud Run       |
| **Gemini API key missing**     | Set `GOOGLE_GEMINI_API_KEY` in environment              |
| **Scraper stuck**              | Ensure `PLAYWRIGHT_HEADLESS=true` and valid URLs        |
| **No insights showing**        | Check if `ensure_tables_exist()` ran and DB has entries |

---

## 🧰 Useful Commands

```bash
# Run app locally
streamlit run streamlit_app.py

# Rebuild Docker image
gcloud builds submit --tag gcr.io/<PROJECT_ID>/reviews-analyzer

# Update Cloud Run service
gcloud run deploy google-reviews-analyzer --image gcr.io/<PROJECT_ID>/reviews-analyzer
```

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).

---

## 💡 Author

**Google Reviews Analyzer**
Developed by Ahmed Hamza

