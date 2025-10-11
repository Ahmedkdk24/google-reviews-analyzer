# src/scrape_reviews.py
import os
import time
import json
import random
import logging
from datetime import datetime
from urllib.parse import urlparse, parse_qs
import re
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from .db import SessionLocal
from .models import Branch, Review

# Config via env
USER_AGENT = os.getenv("USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
MAX_REVIEWS_PER_BRANCH = int(os.getenv("MAX_REVIEWS_PER_BRANCH", "20"))
HEADLESS = os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() in ("1", "true", "yes")
PLAYWRIGHT_PROXY = os.getenv("PLAYWRIGHT_PROXY")  # optional, format: "http://user:pass@host:port"
BRANCHES_JSON = os.getenv("BRANCHES_JSON", "branches.json")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("scraper")


def _is_captcha_page(html):
    # heuristics to detect Google anti-bot page
    captcha_signs = [
        "unusual traffic",
        "Our systems have detected unusual traffic",
        "To continue, please",
        "help us confirm",
        "Please show you're not a robot",
        "Recaptcha"
    ]
    lower = html.lower()
    return any(s.lower() in lower for s in captcha_signs)


def _extract_reviews_from_html(html):
    """
    Improved extraction for Google Maps review blocks using updated selectors.
    Returns list of dicts: {author, rating, text, review_date}
    """
    # Helper function parse_rating_from_text remains the same
    def parse_rating_from_text(s):
        # ... (keep your existing parse_rating_from_text logic here) ...
        if not s:
            return None
        import re # Keep this here since re is needed
        arabic_map = {'٠':'0','١':'1','٢':'2','٣':'3','٤':'4','٥':'5','٦':'6','٧':'7','٨':'8','٩':'9'}
        norm = ''.join(arabic_map.get(ch, ch) for ch in s)
        m = re.search(r"Rated\s*[:\-]?\s*([0-9])", norm)
        if m:
            try: return int(m.group(1))
            except ValueError: return None
        m = re.search(r"([0-9])\s*out\s*of\s*[0-9]", norm)
        if m:
            try: return int(m.group(1))
            except ValueError: return None
        m = re.search(r"\b([0-9])(\.0)?\b", norm)
        if m:
            try: return int(m.group(1))
            except ValueError: return None
        return None
    # End of helper function

    soup = BeautifulSoup(html, "html.parser")
    reviews = []

    # PRIMARY FIX: Use the data-review-id attribute for the main block
    blocks = soup.find_all("div", attrs={"data-review-id": True})

    seen_texts = set()
    for b in blocks:
        try:
            # --- FIX 1: Author Extraction ---
            author = "Unknown"
            author_tag = b.find("div", class_="d4r55 fontTitleMedium")
            if author_tag:
                author = author_tag.get_text(strip=True)

            # --- FIX 2: Rating Extraction ---
            rating = None
            # The rating is in a span with role="img" and an aria-label
            rating_tag = b.find("span", role="img")
            if rating_tag:
                rating = parse_rating_from_text(rating_tag.get("aria-label"))

            # --- Text Extraction (can be simplified/retained as is) ---
            # Targeting the text container: div.MyEned
            text_container = b.find("div", class_="MyEned")
            if text_container:
                 text = text_container.get_text(separator=" ", strip=True)
            else:
                 # Fallback (your original heuristic)
                 text = b.get_text(separator=" ", strip=True) or ""


            # ignore very short or empty texts
            if not text or len(text) < 10:
                continue

            # dedupe by snippet
            snippet = text[:160]
            if snippet in seen_texts:
                continue
            seen_texts.add(snippet)

            reviews.append({
                "author": author,
                "rating": rating or 0,
                "text": text,
                "review_date": ""  # Still requires separate date extraction logic
            })

            if len(reviews) >= MAX_REVIEWS_PER_BRANCH:
                break
        except Exception:
            # avoid full failure when a single block is weird
            continue

    return reviews


def _upsert_branch_and_reviews(place_name, place_url, reviews):
    session = SessionLocal()
    try:
        # use place_url as unique key if place_id not provided
        existing = session.query(Branch).filter(Branch.url == place_url).first()
        if existing:
            branch = existing
            branch.name = place_name or branch.name
            branch.scraped_at = datetime.utcnow()
        else:
            branch = Branch(name=place_name or place_url, url=place_url, place_id=place_url, scraped_at=datetime.utcnow())
            session.add(branch)
            session.flush()

        for r in reviews:
            # naive duplicate detection by review text
            already = session.query(Review).filter(Review.branch_id == branch.id, Review.text == r["text"]).first()
            if already:
                continue
            review = Review(
                branch_id=branch.id,
                author=r.get("author"),
                rating=r.get("rating") or 0,
                text=r.get("text"),
                review_date=r.get("review_date") or ""
            )
            session.add(review)

        session.commit()
        logger.info("Saved %d reviews for branch %s", len(reviews), place_url)
    except Exception as e:
        session.rollback()
        logger.exception("DB error while saving reviews for %s: %s", place_url, e)
    finally:
        session.close()


def _prepare_context_options():
    opts = {"user_agent": USER_AGENT}
    if PLAYWRIGHT_PROXY:
        # playwright expects proxy object like {"server": "http://host:port", "username": "...", "password": "..."}
        proxy_url = PLAYWRIGHT_PROXY
        # naive parsing
        opts["proxy"] = {"server": proxy_url}
    return opts


def scrape_reviews_from_place_urls(place_list):
    """
    place_list: list of dicts: {"name": ..., "url": ...}
    """
    results = []
    ctx_opts = _prepare_context_options()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(
            **ctx_opts,
            viewport={"width": 1280, "height": 900},
            locale="en-US,ar-SA"
        )
        context.set_extra_http_headers({
            "Accept-Language": "en-US,ar-SA;q=0.9,en;q=0.8",
            "Referer": "https://www.google.com/"
        })

        page = context.new_page()

        for place in place_list:
            name = place.get("name")
            url = place.get("url")
            logger.info("Processing branch: %s -> %s", name, url)
            try:
                page.goto(url, timeout=60000)
                page.wait_for_timeout(2500 + random.randint(0, 2000))

                html = page.content()
                if _is_captcha_page(html):
                    logger.warning("CAPTCHA / unusual traffic detected for %s — skipping", url)
                    results.append({"name": name, "url": url, "skipped": True, "reason": "captcha"})
                    continue

                # Try to click "All reviews"
                try:
                    review_button_selectors = [
                        # Updated selectors based on recent Google Maps changes
                        "button[aria-label*='reviews']",
                        "button[jsaction*='pane.review']",
                        "button:has-text('All reviews')",
                        "button:has-text('Reviews')" # A common simplified label
                    ]
                    clicked = False
                    
                    for sel in review_button_selectors:
                        try:
                            # Use page.click for better reliability and wait time
                            page.click(sel, timeout=5000) 
                            clicked = True
                            logger.info("Successfully clicked review button using selector: %s", sel)
                            # Wait for a review element to appear to confirm the panel is open
                            page.wait_for_selector('div[data-review-id]', timeout=5000) 
                            page.wait_for_timeout(1500 + random.randint(0, 1500))
                            break
                        except Exception:
                            continue
                            
                    if not clicked:
                        logger.warning("Could not click any 'All reviews' button for %s. Attempting to scroll anyway.", name)
                        # Continue without clicking, hoping the reviews are already visible/loaded
                        
                except Exception as e:
                    logger.warning("Error during review button click for %s: %s", name, e)
                    pass

                # --- SCROLL REVIEWS PANEL (FULLY UNTIL END) ---
                try:
                    logger.info("Scrolling reviews panel for %s", name)

                    previous_height = 0
                    stagnant_loops = 0
                    max_stagnant_loops = 6  # stop after 6 loops with no growth

                    review_selector = 'div[data-review-id]'

                    while stagnant_loops < max_stagnant_loops:

                        # --- ADDED: Check the current number of loaded reviews ---
                        current_review_count = page.evaluate(f'document.querySelectorAll("{review_selector}").length')
                        if current_review_count >= MAX_REVIEWS_PER_BRANCH:
                            logger.info("Loaded %d reviews (target %d) for %s. Stopping scroll.", current_review_count, MAX_REVIEWS_PER_BRANCH, name)
                            break # Exit the scrolling loop
                        
                        
                        # scroll the review container
                        page.evaluate(
                            """() => {
                                const el = document.querySelector('div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde')
                                    || document.querySelector('div.section-scrollbox')
                                    || document.querySelector('div[role="region"]')
                                    || document.querySelector('#pane');
                                if (el) el.scrollBy(0, el.scrollHeight);
                                else window.scrollBy(0, 1000);
                            }"""
                        )

                        # wait for reviews to load
                        page.wait_for_timeout(2000 + random.randint(0, 1000))

                        # get current height of the container
                        current_height = page.evaluate(
                            """() => {
                                const el = document.querySelector('div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde')
                                    || document.querySelector('div.section-scrollbox')
                                    || document.querySelector('div[role="region"]')
                                    || document.querySelector('#pane');
                                return el ? el.scrollHeight : document.body.scrollHeight;
                            }"""
                        )

                        # if no growth detected, increment stagnant loop count
                        if current_height == previous_height:
                            stagnant_loops += 1
                        else:
                            stagnant_loops = 0
                            previous_height = current_height

                    # Expand any "More" buttons to show full text
                    page.evaluate("""() => {
                        document.querySelectorAll('button[jsaction*="pane.review.expandReview"]').forEach(btn => btn.click());
                    }""")
                    page.wait_for_timeout(1500)

                    logger.info("Finished scrolling all reviews for %s", name)
                except Exception as e:
                    logger.warning("Scrolling failed for %s: %s", name, e)

                html = page.content()
                if _is_captcha_page(html):
                    logger.warning("CAPTCHA detected after scrolling for %s — skipping", url)
                    results.append({"name": name, "url": url, "skipped": True, "reason": "captcha_after_scroll"})
                    continue

                reviews = _extract_reviews_from_html(html)
                logger.info("Found %d reviews (heuristic) for %s", len(reviews), name)
                _upsert_branch_and_reviews(name, url, reviews)
                results.append({"name": name, "url": url, "skipped": False, "found": len(reviews)})

                time.sleep(3 + random.random() * 4)
            except PlaywrightTimeoutError as e:
                logger.exception("Timeout while loading %s: %s", url, e)
                results.append({"name": name, "url": url, "skipped": True, "reason": "timeout"})
            except Exception as e:
                logger.exception("Error processing %s : %s", url, e)
                results.append({"name": name, "url": url, "skipped": True, "reason": str(e)})

        try:
            context.close()
            browser.close()
        except Exception:
            pass

    return results


def load_branches_from_json(path=BRANCHES_JSON):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Branches file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # normalize to list of dicts {name, url}
    out = []
    for it in data:
        if isinstance(it, dict) and it.get("url"):
            out.append({"name": it.get("name") or it.get("url"), "url": it.get("url")})
        elif isinstance(it, str):
            out.append({"name": it, "url": it})
    return out


if __name__ == "__main__":
    # quick CLI for manual runs
    try:
        branches = load_branches_from_json()
    except Exception as e:
        logger.error("Failed to load branches.json: %s", e)
        branches = []

    if not branches:
        logger.error("No branches to scrape. Provide branches.json or set BRANCHES_JSON env.")
        raise SystemExit(1)

    logger.info("Starting review scrape for %d branches", len(branches))
    out = scrape_reviews_from_place_urls(branches)
    logger.info("Finished. Summary: %s", out)
