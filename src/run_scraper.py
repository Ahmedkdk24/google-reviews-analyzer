# src/run_scraper.py
import sys, json
from src.scrape_reviews import scrape_reviews_from_place_urls

if __name__ == "__main__":
    if len(sys.argv) > 1:
        with open(sys.argv[1], "r", encoding="utf-8") as f:
            branches_input = json.load(f)
    else:
        branches_input = []

    scrape_reviews_from_place_urls(branches_input)
