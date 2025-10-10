# src/run_scraper.py
from .utils import init_db
from .scrape_reviews import load_branches_from_json, scrape_reviews_from_place_urls
import os

def main():
    init_db()
    path = os.getenv("BRANCHES_JSON", "branches.json")
    branches = load_branches_from_json(path)
    results = scrape_reviews_from_place_urls(branches)
    print("Done. Summary:")
    for r in results:
        print(r)

if __name__ == "__main__":
    main()
