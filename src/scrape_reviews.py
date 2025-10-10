# src/scrape_reviews.py
import os
import time
import json
import random
import logging
from datetime import datetime
from urllib.parse import urlparse, parse_qs

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from .db import SessionLocal
from .models import Branch, Review

# Config via env
USER_AGENT = os.getenv("USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
MAX_REVIEWS_PER_BRANCH = int(os.getenv("MAX_REVIEWS_PER_BRANCH", "50"))
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
    Heuristic extraction: looks for nodes that contain both rating text ("Rated X") and sizeable text content.
    Returns list of dicts: {author, rating, text, review_date}
    """
    soup = BeautifulSoup(html, "html.parser")
    reviews = []

    # Primary: search for elements whose aria-label mentions "Rated X" (common in Google Maps)
    candidates = soup.find_all(lambda tag: tag.name in ("div", "span") and tag.get("aria-label") and "Rated" in tag.get("aria-label"))
    # fallback: find long text blocks that look like reviews
    if not candidates:
        candidates = soup.find_all("div", string=lambda t: t and len(t.strip()) > 60)

    seen_texts = set()
    for c in candidates:
        text = c.get_text(separator=" ", strip=True)
        if not text or len(text) < 20:
            continue
        # try to parse rating
        aria = (c.get("aria-label") or "")
        rating = None
        m = None
        try:
            import re
            m = re.search(r"Rated\s+([0-5])", aria)
            if m:
                rating = int(m.group(1))
        except Exception:
            rating = None

        # attempt author/date: find nearest preceding element that looks like an author
        author = "Unknown"
        review_date = ""
        prev = c.find_previous(lambda tag: tag.name in ("span", "div") and len(tag.get_text(strip=True)) < 40 and len(tag.get_text(strip=True)) > 0)
        if prev:
            atext = prev.get_text(strip=True)
            if any(ch.isalpha() for ch in atext):
                author = atext

        # dedupe by a snippet of text
        snippet = text[:160]
        if snippet in seen_texts:
            continue
        seen_texts.add(snippet)

        reviews.append({
            "author": author,
            "rating": rating or 0,
            "text": text,
            "review_date": review_date
        })

        if len(reviews) >= MAX_REVIEWS_PER_BRANCH:
            break

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
        # allow both English and Arabic reviews
        context.set_extra_http_headers({
            "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
            "Referer": "https://www.google.com/"
        })


        page = context.new_page()

        for place in place_list:
            name = place.get("name")
            url = place.get("url")
            logger.info("Processing branch: %s -> %s", name, url)
            try:
                page.goto(url, timeout=60000)
                # wait a bit for dynamic content
                page.wait_for_timeout(2500 + random.randint(0, 2000))

                html = page.content()
                if _is_captcha_page(html):
                    logger.warning("CAPTCHA / unusual traffic detected for %s — skipping", url)
                    results.append({"name": name, "url": url, "skipped": True, "reason": "captcha"})
                    continue

                # Try to click "All reviews" or open review panel.
                # Common patterns: button with aria-label containing "reviews" or text "All reviews"
                try:
                    # try several selectors until one succeeds
                    review_button_selectors = [
                        "button[aria-label*='reviews']",
                        "button[jsaction*='pane.review']",
                        "button:has-text('All reviews')",
                        "a[href*='#reviews']"
                    ]
                    clicked = False
                    for sel in review_button_selectors:
                        try:
                            btns = page.query_selector_all(sel)
                            if btns:
                                btns[0].click(timeout=3000)
                                clicked = True
                                break
                        except PlaywrightTimeoutError:
                            continue
                    if clicked:
                        page.wait_for_timeout(1500 + random.randint(0, 1500))
                except Exception:
                    # ignore - maybe reviews are already visible
                    pass

                # Scroll the reviews panel to load more items.
                # We'll attempt to find the review container and scroll it multiple times.
                try:
                    # heuristics: there is often a scrollable div with role="region" that contains reviews
                    review_scrolled = False
                    for _ in range(6):
                        # run page.evaluate to scroll the main element that holds reviews
                        page.evaluate(
                            """() => {
                                const sel = document.querySelector('div[role=\"main\"]') || document.querySelector('#pane');
                                if (sel) {
                                    sel.scrollBy(0, 1000);
                                } else {
                                    window.scrollBy(0, 1000);
                                }
                            }"""
                        )
                        page.wait_for_timeout(900 + random.randint(0, 800))
                        review_scrolled = True
                    if not review_scrolled:
                        page.wait_for_timeout(2000)
                except Exception:
                    page.wait_for_timeout(2000)

                # get HTML after scrolling
                html = page.content()
                if _is_captcha_page(html):
                    logger.warning("CAPTCHA detected after scrolling for %s — skipping", url)
                    results.append({"name": name, "url": url, "skipped": True, "reason": "captcha_after_scroll"})
                    continue

                # parse reviews
                reviews = _extract_reviews_from_html(html)
                logger.info("Found %d reviews (heuristic) for %s", len(reviews), name)
                # save to DB
                _upsert_branch_and_reviews(name, url, reviews)
                results.append({"name": name, "url": url, "skipped": False, "found": len(reviews)})
                # polite random delay between branches
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
