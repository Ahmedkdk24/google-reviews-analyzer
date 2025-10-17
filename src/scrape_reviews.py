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

from src.db import SessionLocal
from src.models import Branch, Review

# Config via env
USER_AGENT = os.getenv("USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
MAX_REVIEWS_PER_BRANCH = int(os.getenv("MAX_REVIEWS_PER_BRANCH", "10"))
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
    Extract reviews (author, rating, text, date) from a Google Maps place page.
    Handles current DOM structure as of 2025, including <span class="xRkPPb"> date format.
    """
    from bs4 import BeautifulSoup
    import re
    from datetime import datetime, timedelta

    # --- Helper: parse numeric rating from aria-label text ---
    def parse_rating_from_text(s):
        if not s:
            return None
        arabic_map = {'٠':'0','١':'1','٢':'2','٣':'3','٤':'4','٥':'5','٦':'6','٧':'7','٨':'8','٩':'9'}
        norm = ''.join(arabic_map.get(ch, ch) for ch in s)
        m = re.search(r"([0-9])(\.0)?\b", norm)
        return int(m.group(1)) if m else None

    # --- Helper: parse relative date like "2 weeks ago" or "قبل شهرين" ---
    def parse_relative_date(text):
        if not text:
            return ""
        text = text.strip().lower()
        now = datetime.utcnow()

        # Remove trailing source info like "on Tripadvisor"
        text = re.sub(r"\s+on\s+.*$", "", text).strip()

        # English relative times
        patterns = [
            (r"(\d+)\s+day", "day"),
            (r"(\d+)\s+week", "week"),
            (r"(\d+)\s+month", "month"),
            (r"(\d+)\s+year", "year"),
        ]
        for pattern, unit in patterns:
            m = re.search(pattern, text)
            if m:
                num = int(m.group(1))
                if unit == "day":
                    return (now - timedelta(days=num)).strftime("%Y-%m-%d")
                elif unit == "week":
                    return (now - timedelta(weeks=num)).strftime("%Y-%m-%d")
                elif unit == "month":
                    return (now - timedelta(days=30*num)).strftime("%Y-%m-%d")
                elif unit == "year":
                    return (now - timedelta(days=365*num)).strftime("%Y-%m-%d")

        # Arabic equivalents
        arabic_patterns = [
            (r"قبل\s+(\d+)\s*يوم", "day"),
            (r"قبل\s+(\d+)\s*أسبوع", "week"),
            (r"قبل\s+(\d+)\s*شهر", "month"),
            (r"قبل\s+(\d+)\s*سنة", "year"),
        ]
        for pattern, unit in arabic_patterns:
            m = re.search(pattern, text)
            if m:
                num = int(m.group(1))
                if unit == "day":
                    return (now - timedelta(days=num)).strftime("%Y-%m-%d")
                elif unit == "week":
                    return (now - timedelta(weeks=num)).strftime("%Y-%m-%d")
                elif unit == "month":
                    return (now - timedelta(days=30*num)).strftime("%Y-%m-%d")
                elif unit == "year":
                    return (now - timedelta(days=365*num)).strftime("%Y-%m-%d")

        # fallback: return raw string (absolute dates like "January 2024" or "2023")
        return text

    # --- Main Extraction ---
    soup = BeautifulSoup(html, "html.parser")
    reviews = []
    seen_texts = set()

    blocks = soup.find_all("div", attrs={"data-review-id": True})
    for b in blocks:
        try:
            # Author
            author_tag = b.find("div", class_="d4r55 fontTitleMedium")
            author = author_tag.get_text(strip=True) if author_tag else "Unknown"

            # Rating
            rating_tag = b.find("span", role="img")
            rating = parse_rating_from_text(rating_tag.get("aria-label") if rating_tag else "")

            # Review text
            text_container = b.find("div", class_="MyEned")
            review_text = text_container.get_text(separator=" ", strip=True) if text_container else ""
            if not review_text or len(review_text) < 10:
                continue

            # ✅ Extract review date from <span class="xRkPPb">
            date_tag = b.find("span", class_="xRkPPb")
            review_date_raw = date_tag.get_text(separator=" ", strip=True) if date_tag else ""
            review_date = parse_relative_date(review_date_raw)

            # Deduplicate by text snippet
            snippet = review_text[:160]
            if snippet in seen_texts:
                continue
            seen_texts.add(snippet)

            reviews.append({
                "author": author,
                "rating": rating or 0,
                "text": review_text,
                "review_date": review_date
            })

            if len(reviews) >= MAX_REVIEWS_PER_BRANCH:
                break

        except Exception:
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
            branch = Branch(
                name=place_name or place_url,
                url=place_url,
                place_id=place_url,
                scraped_at=datetime.utcnow()
            )
            session.add(branch)
            session.flush()

        for r in reviews:
            # Use branch.id normally, Review now uses review_id as PK
            already = session.query(Review).filter(
                Review.branch_id == branch.id,
                Review.text == r["text"]
            ).first()

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
            locale="en-US, ar-SA"
        )
        context.set_extra_http_headers({
            "Accept-Language": "en-US, ar-SA;q=0.9,en;q=0.8",
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

                # --- SCROLL REVIEWS PANEL (FULLY UNTIL END) -
                    # --- CREATE A FRESH PAGE PER BRANCH ---
                page = context.new_page()
                page.goto(url, timeout=60000)
                page.wait_for_load_state("networkidle")
                page.wait_for_timeout(3000 + random.randint(0, 2000))

                html = page.content()
                if _is_captcha_page(html):
                    logger.warning("CAPTCHA / unusual traffic detected for %s — skipping", url)
                    results.append({"name": name, "url": url, "skipped": True, "reason": "captcha"})
                    page.close()
                    continue

                # --- CLICK "ALL REVIEWS" BUTTON ---
                try:
                    review_button_selectors = [
                        "button[aria-label*='reviews']",
                        "button[jsaction*='pane.review']",
                        "button:has-text('All reviews')",
                        "button:has-text('Reviews')"
                    ]
                    clicked = False
                    for sel in review_button_selectors:
                        try:
                            page.click(sel, timeout=5000)
                            clicked = True
                            logger.info("Clicked review button using selector: %s", sel)
                            page.wait_for_selector('div[data-review-id]', timeout=8000)
                            break
                        except Exception:
                            continue
                    if not clicked:
                        logger.warning("Could not click any 'All reviews' button for %s", name)
                except Exception as e:
                    logger.warning("Error clicking review button for %s: %s", name, e)

                # --- SCROLL + EXPAND LOOP ---
                try:
                    logger.info("Scrolling reviews panel for %s", name)

                    previous_height = 0
                    stagnant_loops = 0
                    max_stagnant_loops = 15  # allow more scroll cycles for slow loading
                    max_total_attempts = 5    # NEW: max number of scroll retries
                    total_attempts = 0        # counter

                    timeout_seconds = 60      # NEW: total timeout per branch
                    start_time = time.time()

                    review_selector = 'div[data-review-id]'
                    scroll_script = """
                        () => {
                            const el = document.querySelector('div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde')
                                || document.querySelector('div.section-scrollbox')
                                || document.querySelector('div[role="region"]')
                                || document.querySelector('#pane');
                            if (el) el.scrollBy(0, el.scrollHeight);
                            else window.scrollBy(0, 1000);
                        }
                    """

                    while stagnant_loops < max_stagnant_loops and total_attempts < max_total_attempts:
                        current_review_count = page.evaluate(f'document.querySelectorAll("{review_selector}").length')

                        # Stop if target reached
                        if current_review_count >= MAX_REVIEWS_PER_BRANCH:
                            logger.info(f"Loaded {current_review_count} reviews (target {MAX_REVIEWS_PER_BRANCH}) for {name}. Stopping scroll.")
                            break

                        # Stop if timeout reached
                        if time.time() - start_time > timeout_seconds:
                            logger.warning(f"Timeout reached for {name}, stopping scroll at {current_review_count} reviews.")
                            break

                        # Scroll the container
                        page.evaluate(scroll_script)
                        page.wait_for_timeout(2000 + random.randint(0, 1000))

                        # Expand "More" / "المزيد" every few loops
                        if stagnant_loops % 3 == 0:
                            try:
                                buttons = page.query_selector_all("button, span")
                                expanded = 0
                                for btn in buttons:
                                    try:
                                        text = (btn.inner_text() or "").strip()
                                        if text in ("More", "المزيد") or "expandReview" in (btn.get_attribute("jsaction") or ""):
                                            btn.scroll_into_view_if_needed()
                                            btn.click(timeout=800)
                                            expanded += 1
                                    except Exception:
                                        continue
                                if expanded:
                                    logger.info("Expanded %d 'More' buttons for %s", expanded, name)
                            except Exception:
                                pass

                        # Measure growth of scroll height
                        current_height = page.evaluate("""
                            () => {
                                const el = document.querySelector('div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde')
                                    || document.querySelector('div.section-scrollbox')
                                    || document.querySelector('div[role="region"]')
                                    || document.querySelector('#pane');
                                return el ? el.scrollHeight : document.body.scrollHeight;
                            }
                        """)

                        if current_height == previous_height:
                            stagnant_loops += 1
                            total_attempts += 1  # increment retry counter
                        else:
                            stagnant_loops = 0
                            previous_height = current_height

                        # retry if few reviews loaded
                        if stagnant_loops >= max_stagnant_loops and current_review_count < 10:
                            logger.warning(f"Only {current_review_count} reviews loaded, retrying scroll for {name}")
                            stagnant_loops = 0

                    page.wait_for_timeout(3000 + random.randint(0, 1500))
                    logger.info("Finished scrolling all reviews for %s", name)

                except Exception as e:
                    logger.warning("Scrolling failed for %s: %s", name, e)

                finally:
                    # --- Extract reviews and close page ---
                    html = page.content()
                    if _is_captcha_page(html):
                        logger.warning("CAPTCHA detected after scrolling for %s — skipping", url)
                        results.append({"name": name, "url": url, "skipped": True, "reason": "captcha_after_scroll"})
                        page.close()
                        continue

                    reviews = _extract_reviews_from_html(html)
                    logger.info("Found %d reviews (heuristic) for %s", len(reviews), name)
                    _upsert_branch_and_reviews(name, url, reviews)
                    results.append({"name": name, "url": url, "skipped": False, "found": len(reviews)})

                    page.close()
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
