# scrape_reviews_to_csv.py
import os
import time
import random
import logging
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs
import re
import pandas as pd
from bs4 import BeautifulSoup
from langdetect import detect, DetectorFactory
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# Set langdetect to be deterministic for consistent results
DetectorFactory.seed = 0

# ==============================================================================
# 🎯 SIMPLE CONFIGURATION VARIABLES (Replaces branches.json)
# ==============================================================================
BRANCH_NAME = "Atasawoq Alshamil"
BRANCH_URL = "https://www.google.com/maps/place/Altasawoq+Alshamel/@24.6675671,46.7800534,17z/data=!3m1!4b1!4m6!3m5!1s0x3e2f078442eeeb85:0xbf76bc1b676f0108!8m2!3d24.6675622!4d46.7826283!16s%2Fg%2F11s5s6n82b?entry=ttu&g_ep=EgoyMDI1MTEyMy4xIKXMDSoASAFQAw%3D%3D"
OUTPUT_CSV_FILE = "scraped_reviews.csv"

# ==============================================================================
# ⚙️ GENERAL CONFIG (from environment variables)
# ==============================================================================
USER_AGENT = os.getenv("USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
MAX_REVIEWS_PER_BRANCH = int(os.getenv("MAX_REVIEWS_PER_BRANCH", "200"))  # Target more reviews
SCROLL_PAUSE_TIME = float(os.getenv("SCROLL_PAUSE_TIME", "4.0"))  # Longer pause between scrolls
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))  # More retry attempts
SCROLL_TIMEOUT = int(os.getenv("SCROLL_TIMEOUT", "300"))  # 5 minutes timeout for scrolling
HEADLESS = os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() in ("1", "true", "yes")  # Run in visible mode by default
PLAYWRIGHT_PROXY = os.getenv("PLAYWRIGHT_PROXY")  # optional, format: "http://user:pass@host:port"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
EXTRA_SNAPSHOT_ROUNDS = int(os.getenv("EXTRA_SNAPSHOT_ROUNDS", "30"))  # how many aggressive snapshot rounds to try
CLICK_TRANSLATIONS = os.getenv("CLICK_TRANSLATIONS", "false").lower() in ("1", "true", "yes")
APPEND_TO_CSV = os.getenv("APPEND_TO_CSV", "false").lower() in ("1", "true", "yes")

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("scraper")

# ==============================================================================
# 🛠️ HELPER FUNCTIONS
# ==============================================================================

def _is_captcha_page(html):
    """Heuristics to detect Google anti-bot page."""
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
    """
    # --- Helper: parse numeric rating from various text forms ---
    def parse_rating_from_text(s):
        if not s:
            return None
        # handle '5/5' or 'Rating of 5' or Arabic digits
        s = s.strip()
        m = re.search(r"(\d+)\s*/\s*5", s)
        if m:
            return int(m.group(1))
        m = re.search(r"Rating of\s*(\d+)", s, re.IGNORECASE)
        if m:
            return int(m.group(1))
        arabic_map = {'٠':'0','١':'1','٢':'2','٣':'3','٤':'4','٥':'5','٦':'6','٧':'7','٨':'8','٩':'9'}
        norm = ''.join(arabic_map.get(ch, ch) for ch in s)
        m = re.search(r"(\d+)(\.0)?\b", norm)
        return int(m.group(1)) if m else None


    # --- Helper: parse relative date like "2 weeks ago" or "قبل شهرين" ---
    def parse_relative_date(text):
        if not text:
            return ""
        text = text.strip().lower()
        now = datetime.utcnow()

        # Remove trailing source info like "on Tripadvisor" or "on Google"
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

        # fallback: return raw string
        return text


    # --- Main Extraction ---
    soup = BeautifulSoup(html, "html.parser")
    reviews = []
    seen_texts = set()

    # Strategy 1: modern blocks with data-review-id / jftiEf
    blocks = soup.find_all("div", attrs={"data-review-id": True}) or soup.find_all("div", class_="jftiEf")

    # Strategy 2 (fallback): reviews page layout (ml reviews page) using 'hjmQqc' wrappers
    if not blocks:
        blocks = soup.find_all("div", class_="hjmQqc")

    # Strategy 3 (additional fallback): look for generic review list items
    if not blocks:
        blocks = soup.find_all("div", class_="VjjEkf")

    for b in blocks:
        try:
            # Try multiple approaches to get author
            author = None
            # common author selectors (updated with more selector patterns)
            author_tag = b.select_one(".d4r55, .IaK8zc, .CVo7Bb, .kyCNSe, button[aria-label*='reviews'], a[class*='review'], .PrHqRd, .NiCqic")
            if author_tag:
                author = author_tag.get_text(strip=True)
                logger.debug("Found author using common selectors: %s", author)

            if not author:
                # sometimes author is inside a button with id like ml-reviews-page-user-review-name-*
                name_btn = b.find(attrs={"id": re.compile(r"ml-reviews-page-user-review-name-.*")})
                if name_btn:
                    author = name_btn.get_text(strip=True)
                    logger.debug("Found author using name button: %s", author)

            if not author:
                # Try finding author in buttons or links
                for el in b.find_all(['button', 'a']):
                    if el.get('aria-label', '').lower().endswith('profile'):
                        author = el.get_text(strip=True)
                        logger.debug("Found author using profile link: %s", author)
                        break

            if not author:
                logger.debug("No author found in review block, using Unknown")
                author = "Unknown"

            # rating: try aria-label on role=img, or span with numeric text
            rating = None
            rating_node = b.find(attrs={"role": "img", "aria-label": re.compile(r".*\d.*")})
            if rating_node and rating_node.has_attr("aria-label"):
                rating = parse_rating_from_text(rating_node.get("aria-label"))
                logger.debug("Found rating using aria-label: %s", rating)

            if rating is None:
                # Updated rating selectors
                rating_span = b.select_one(".fontBodyLarge.fzvQIb, .Rab10, .kvMYJc, div[role='img'][aria-label*='stars']")
                if rating_span:
                    rating = parse_rating_from_text(rating_span.get_text(strip=True) if not rating_span.has_attr("aria-label") else rating_span["aria-label"])
                    logger.debug("Found rating using span: %s", rating)

            if rating is None:
                # Try finding a number between 1-5 in any element
                for el in b.find_all(text=re.compile(r'\d')):
                    txt = el.strip()
                    if txt and any(str(n) in txt for n in range(1, 6)):
                        rating = parse_rating_from_text(txt)
                        if rating:
                            logger.debug("Found rating using number scan: %s", rating)
                            break

            rating = rating or 0
            if rating == 0:
                logger.debug("No valid rating found in review block")

            # review text: usual selectors (updated with more patterns)
            review_text = ""
            text_span = b.select_one(".MyEned .wiI7pd, .d5K5Pd, .d5K5Pd, .review-full-text, [data-review-text], .wiI7pd")
            if text_span:
                review_text = text_span.get_text(separator=" ", strip=True)
                logger.debug("Found review text using common selectors (%d chars)", len(review_text))
            
            if not review_text:
                # Try finding the largest text block
                max_len = 0
                for tag in b.find_all(text=True):
                    txt = tag.strip()
                    parent = tag.parent
                    # Skip if parent is a button, link, or rating/date element
                    if parent and (parent.name in ['button', 'a'] or 
                                any(cls in (parent.get('class', []) or []) 
                                    for cls in ['rating', 'date', 'time'])):
                        continue
                    if txt and len(txt) > max_len:
                        max_len = len(txt)
                        review_text = txt
                if review_text:
                    logger.debug("Found review text using largest text block (%d chars)", len(review_text))

            if not review_text or len(review_text) < 10:
                logger.debug("Skipping review - text too short or missing (%d chars)", len(review_text) if review_text else 0)
                continue

            # date (updated with more selector patterns)
            review_date_raw = ""
            date_tag = b.select_one(".xRkPPb, .bHyEBc, .dehysf, time, .review-date, [data-review-date]")
            if date_tag:
                review_date_raw = date_tag.get_text(separator=" ", strip=True)
                logger.debug("Found date using common selectors: %s", review_date_raw)
            
            if not review_date_raw:
                # Try to find a date-like string in any element
                date_patterns = [
                    r"\d+\s+(day|week|month|year)s?\s+ago",
                    r"قبل\s+\d+\s+(يوم|اسبوع|شهر|سنة)",
                    r"\d{4}-\d{2}-\d{2}",
                    r"\d{1,2}/\d{1,2}/\d{4}"
                ]
                for el in b.find_all(text=re.compile("|".join(date_patterns), re.I)):
                    txt = el.strip()
                    if txt:
                        review_date_raw = txt
                        logger.debug("Found date using pattern match: %s", review_date_raw)
                        break

            review_date = parse_relative_date(review_date_raw)
            if review_date:
                logger.debug("Parsed relative date: %s -> %s", review_date_raw, review_date)

            # attempt to get a stable review id (data-review-id exists in some layouts)
            review_id = None
            try:
                if b.has_attr("data-review-id"):
                    review_id = b.get("data-review-id")
            except Exception:
                review_id = None

            # fallback id: first 160 chars of text + author
            snippet = (review_text[:160] or "")
            fallback_id = f"snippet:{snippet}|author:{author}"
            review_id = review_id or fallback_id

            # dedupe by snippet/constructed id
            if review_id in seen_texts:
                continue
            seen_texts.add(review_id)

            reviews.append({
                "branch_name": BRANCH_NAME,
                "branch_url": BRANCH_URL,
                "review_id": review_id,
                "author": author,
                "rating": rating,
                "text": review_text,
                "review_date": review_date,
                "date_raw": review_date_raw,
                "scraped_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            })

            if len(reviews) >= MAX_REVIEWS_PER_BRANCH:
                break

        except Exception:
            continue

    return reviews

def _prepare_context_options():
    opts = {"user_agent": USER_AGENT}
    if PLAYWRIGHT_PROXY:
        # playwright expects proxy object like {"server": "http://host:port", "username": "...", "password": "..."}
        proxy_url = PLAYWRIGHT_PROXY
        # naive parsing
        opts["proxy"] = {"server": proxy_url}
    return opts

# ==============================================================================
# 🔄 MAIN SCRAPING LOGIC
# ==============================================================================

def scrape_reviews_from_place_url(name, url):
    """
    Scrapes reviews for a single place URL and returns the list of reviews.
    """
    all_reviews = []
    ctx_opts = _prepare_context_options()

    with sync_playwright() as p:
        # Simple browser setup that was working before
        browser = p.chromium.launch(
            headless=HEADLESS,
            args=["--disable-blink-features=AutomationControlled"]
        )
        
        # Basic context configuration
        context = browser.new_context(
            **ctx_opts,
            viewport={"width": 1280, "height": 900},
            locale="en-US, ar-SA"
        )
        
        # Simple headers that worked before
        context.set_extra_http_headers({
            "Accept-Language": "en-US, ar-SA;q=0.9,en;q=0.8",
            "Referer": "https://www.google.com/"
        })
        
        # Add JavaScript fingerprint evasion
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            
            // Fake touch support
            Object.defineProperty(navigator, 'maxTouchPoints', {
                get: () => 5
            });
        """)

        page = context.new_page()
        logger.info("Processing branch: %s -> %s", name, url)

        try:
            # Navigate with a longer timeout since it's loading reviews directly
            page.goto(url, timeout=90000, wait_until="networkidle")
            
            # Wait for the page to be truly ready. `goto(..., wait_until="networkidle")`
            # already attempts to wait for network idle, but some pages never reach a
            # perfectly idle state (long-polling, analytics). Be tolerant: wait for
            # DOMContentLoaded and then try to wait for networkidle but continue on
            # timeout so scraping can proceed.
            page.wait_for_load_state("domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=30000)
            except PlaywrightTimeoutError:
                logger.debug("networkidle wait timed out - continuing without strict idle")
            
            # Additional wait for dynamic content
            page.wait_for_timeout(3000)
            
            logger.debug("Page loaded, checking for reviews...")
            
            html = page.content()
            if _is_captcha_page(html):
                logger.warning("CAPTCHA / unusual traffic detected for %s — skipping", url)
                return {"name": name, "url": url, "skipped": True, "reason": "captcha", "reviews": []}

            # Since we're using a direct reviews URL, wait for the reviews panel to load
            try:
                logger.debug("Waiting for reviews panel to load...")
                
                # First wait for the reviews container
                main_panel = 'div[role="main"], div[role="region"]'
                try:
                    page.wait_for_selector(main_panel, timeout=10000, state='visible')
                    logger.debug("Main panel loaded")
                except Exception as e:
                    logger.debug(f"Main panel not immediately visible: {e}")
                
                # Give the reviews a moment to populate
                page.wait_for_timeout(2000)
                
                # Check if we can find any review elements
                review_count = page.evaluate('''() => {
                    const reviews = document.querySelectorAll('div.jftiEf[data-review-id], div[data-review-id]');
                    return reviews.length;
                }''')
                
                if review_count > 0:
                    logger.info(f"Found {review_count} reviews after initial load")
                else:
                    logger.debug("No reviews found yet, waiting longer...")
                    page.wait_for_timeout(5000)
                    
                    # Scroll the main container to trigger review loading
                    page.evaluate('''() => {
                        const panel = document.querySelector('div[role="main"], div[role="region"]');
                        if (panel) {
                            panel.scrollTo(0, 200);
                            return true;
                        }
                        return false;
                    }''')
                    
                    page.wait_for_timeout(2000)
                    review_count = page.evaluate('document.querySelectorAll("div.jftiEf[data-review-id], div[data-review-id]").length')
                    logger.info(f"Found {review_count} reviews after waiting")

            except Exception as e:
                logger.warning("Error waiting for reviews panel to load: %s", e)
                pass

                # --- SCROLL + EXPAND LOOP ---
            try:
                logger.info("Scrolling reviews panel for %s", name)

                previous_height = 0
                stagnant_loops = 0
                max_stagnant_loops = 40  # More patience for stagnant loops
                max_total_attempts = MAX_RETRIES * 2  # Double the retry attempts
                total_attempts = 0
                timeout_seconds = 300  # 5 minutes timeout for thorough scrolling
                start_time = time.time()
                last_review_count = 0
                no_new_reviews_count = 0

                review_selector = 'div[data-review-id], div.jftiEf, div.MyEned'  # Multiple selectors for review blocks
                scroll_script = """
                    () => {
                        // Find all potential scrollable containers
                        const containers = [
                            document.querySelector('div[role="feed"]'),
                            document.querySelector('#pane'),
                            document.querySelector('div.m6QErb.DxyBCb.kA9KIf.dS8AEf'),
                            document.querySelector('div.m6QErb.DxyBCb.kA9KIf.dS8AEf.ecceSd'),
                            document.querySelector('div[role="main"]'),
                            document.querySelector('div[role="region"]'),
                            document.querySelector('div.section-layout'),
                            document.querySelector('div.section-scrollbox'),
                            document.querySelector('div[data-review-id]')?.parentElement,
                            document.querySelector('div.jftiEf')?.parentElement
                        ].filter(el => el); // Remove nulls
                        
                        let scrolled = false;
                        for (const container of containers) {
                            try {
                                // Check if container is actually scrollable
                                if (container.scrollHeight > container.clientHeight) {
                                    // More aggressive scroll
                                    const scrollAmount = Math.floor(Math.random() * 500) + 800;
                                    container.scrollTo({
                                        top: container.scrollTop + scrollAmount,
                                        behavior: 'smooth'
                                    });
                                    scrolled = true;
                                }
                            } catch (e) {
                                continue;
                            }
                        }
                        
                        // Fallback to document scroll
                        if (!scrolled) {
                            window.scrollTo({
                                top: window.scrollY + 1000,
                                behavior: 'smooth'
                            });
                            scrolled = true;
                        }
                        
                        return scrolled;
                    }
                """
                
                # Double check we're on reviews tab/section and try to open the full reviews modal
                modal_open = False
                try:
                    # First try a few possible selectors that open the reviews modal/page
                    modal_triggers = [
                        "button[aria-label*='Reviews']",
                        "button[aria-label*='See all reviews']",
                        "button[aria-label*='Show all reviews']",
                        "a[href*='/reviews']",
                        "button[jsaction*='pane.review']",
                        "div[role='button'][aria-label*='reviews']",
                    ]
                    for sel in modal_triggers:
                        try:
                            btn = page.query_selector(sel)
                            if btn:
                                try:
                                    btn.scroll_into_view_if_needed()
                                except Exception:
                                    pass
                                try:
                                    btn.click(timeout=3000)
                                except Exception:
                                    try:
                                        page.evaluate("el => el.click()", btn)
                                    except Exception:
                                        pass
                                # give modal a moment to appear
                                page.wait_for_timeout(1200)
                                # check for dialog/modal
                                if page.query_selector('div[role="dialog"], div.section-scrollbox, div[aria-modal="true"]'):
                                    modal_open = True
                                    logger.debug('Opened reviews modal using selector: %s', sel)
                                    break
                        except Exception:
                            continue
                    # Fallback: try a generic text-match click for elements that contain the word 'review(s)' or Arabic equivalents with digits
                    try:
                        if not modal_open:
                            clicked = page.evaluate("""
                                () => {
                                    const patterns = [/\\breviews?\\b/i, /\\breview\\b/i, /\\bالتقييمات\\b/, /\\bمراجعات\\b/];
                                    function clickableAncestor(el){
                                        let cur = el;
                                        while(cur){
                                            if(typeof cur.click === 'function') return cur;
                                            cur = cur.parentElement;
                                        }
                                        return null;
                                    }
                                    const candidates = Array.from(document.querySelectorAll('*')).filter(e => {
                                        const t = (e.innerText || '').trim();
                                        if(!t) return false;
                                        // require digits (counts) and a keyword
                                        return /\\d/.test(t) && patterns.some(p => p.test(t)) && t.length < 200;
                                    });
                                    if(candidates.length){
                                        const el = candidates[0];
                                        try{ el.scrollIntoView(); el.click(); return true;}catch(e){}
                                        const anc = clickableAncestor(el);
                                        if(anc){ try{ anc.scrollIntoView(); anc.click(); return true;}catch(e){} }
                                    }
                                    return false;
                                }
                            """)
                            if clicked:
                                # give modal a moment to appear
                                page.wait_for_timeout(1200)
                                if page.query_selector('div[role="dialog"], div.section-scrollbox, div[aria-modal="true"]'):
                                    modal_open = True
                                    logger.debug('Opened reviews modal using generic text-match click')
                    except Exception as e:
                        logger.debug('Generic text-match modal click failed: %s', e)
                except Exception:
                    pass  # best-effort; continue even if no modal is opened

                # If a modal/dialog was opened, attempt a modal-specific scroll to ensure we scroll inside the dialog
                if modal_open:
                    try:
                        logger.info("Modal opened - attempting modal-specific scrolling for %s", name)
                        modal_selector = 'div[role="dialog"], div.section-scrollbox, div[aria-modal="true"]'

                        # Save initial modal HTML for debugging
                        try:
                            modal_el = page.query_selector(modal_selector)
                            if modal_el:
                                modal_html = page.evaluate('(el) => el.outerHTML', modal_el)
                                modal_debug_path = os.path.join(os.getcwd(), "debug_modal.html")
                                with open(modal_debug_path, "w", encoding="utf-8") as mh:
                                    mh.write(modal_html)
                                logger.debug('Wrote initial debug modal HTML to %s', modal_debug_path)
                        except Exception as e:
                            logger.debug('Failed to write initial modal HTML: %s', e)

                        # Modal-specific id collection + scrolling routine with targeted container detection
                        modal_seen_ids = set()
                        modal_no_new = 0
                        max_modal_no_new = 15  # More patience for no-new-ids condition
                        modal_rounds = 0
                        max_modal_rounds = 300  # More rounds to try

                        while modal_rounds < max_modal_rounds:
                            modal_rounds += 1
                            try:
                                # First expand any "More" buttons to get full text
                                try:
                                    # If translation clicking is enabled, do a scoped click inside each review node and return counts for debugging.
                                    if CLICK_TRANSLATIONS:
                                        result = page.evaluate("""
                                            (modalSel) => {
                                                const modal = document.querySelector(modalSel) || document;
                                                let reviewNodes = [];
                                                try { reviewNodes = Array.from(modal.querySelectorAll('div[data-review-id], div.jftiEf, div.MyEned, div.hjmQqc, div.VjjEkf')); } catch(e) { reviewNodes = []; }
                                                let transClicked = 0;
                                                let expanded = 0;
                                                try {
                                                    const transRegexes = [/see translation/i, /see translation \(english\)/i, /عرض الترجمة/i, /ترجمة/i, /translate/i];
                                                    for (const rn of reviewNodes) {
                                                        try {
                                                            const candidates = Array.from(rn.querySelectorAll('button, a'));
                                                            for (const c of candidates) {
                                                                const txt = (c.innerText || '').trim() || '';
                                                                const aria = (c.getAttribute && c.getAttribute('aria-label')) || '';
                                                                if ((txt && transRegexes.some(r => r.test(txt))) || (aria && /see translation/i.test(aria))) {
                                                                    try { c.scrollIntoView(); c.click(); transClicked += 1; } catch(e) {}
                                                                    break;
                                                                }
                                                            }
                                                        } catch(e) { continue; }
                                                    }
                                                } catch(e) {}
                                                try {
                                                    for (const rn of reviewNodes) {
                                                        try {
                                                            const buttons = Array.from(rn.querySelectorAll('button'));
                                                            for (const btn of buttons) {
                                                                try {
                                                                    const t = (btn.innerText || '').trim() || '';
                                                                    if (/^\s*(more|see more|المزيد)\b/i.test(t) || btn.getAttribute('aria-expanded') === 'false' || btn.getAttribute('aria-controls')) {
                                                                        btn.click(); expanded += 1;
                                                                    }
                                                                } catch(e) { continue; }
                                                            }
                                                        } catch(e) { continue; }
                                                    }
                                                } catch(e) {}
                                                return {transClicked, expanded};
                                            }
                                        """, modal_selector)
                                        logger.info("Translation click result: %s", result)
                                    else:
                                        # If translation clicking disabled, only expand 'More' buttons inside modal (safe default)
                                        result = page.evaluate("""
                                            (modalSel) => {
                                                const modal = document.querySelector(modalSel) || document;
                                                let expanded = 0;
                                                try {
                                                    const expandButtons = Array.from(modal.querySelectorAll('button.w8nwRe.kyuRq[aria-expanded="false"], button[jsaction*="review.expandReview"], button.kyuRq[aria-label="See more"], button[aria-controls]:not([aria-expanded="true"])'));
                                                    for (const btn of expandButtons) {
                                                        try { btn.click(); expanded += 1; } catch(e) { continue; }
                                                    }
                                                } catch(e) {}
                                                return {transClicked: 0, expanded};
                                            }
                                        """, modal_selector)
                                        logger.info("Modal expand (no-translation) result: %s", result)

                                    page.wait_for_timeout(500)  # Brief pause after expansions
                                except Exception as e:
                                    logger.debug('Modal expand-text failed: %s', e)

                                ids = page.evaluate("""
                                    (sel) => {
                                        const modal = document.querySelector(sel);
                                        if(!modal) return [];
                                        
                                        // More thorough review node detection
                                        const items = Array.from(modal.querySelectorAll(
                                            'div[data-review-id], div.jftiEf, div.MyEned, ' +
                                            'div.section-review-content, div[role="article"], ' + 
                                            'div.hjmQqc, div.VjjEkf'
                                        ));
                                        
                                        const ids = items.map(i => {
                                            const directId = i.getAttribute('data-review-id') || i.getAttribute('data-reviewid');
                                            if(directId) return directId;
                                            // Fallback composite ID from visible text and attributes
                                            const text = (i.innerText || '').slice(0,160);
                                            const rating = i.querySelector('[aria-label*="star"]')?.getAttribute('aria-label');
                                            const author = i.querySelector('[class*="author"], [class*="name"]')?.innerText;
                                            return `${text}|${rating || ''}|${author || ''}`;
                                        }).filter(id => id && id.length > 10);  // Filter out empty/tiny IDs
                                        
                                        // Smart scrollable container detection
                                        function findBestScrollable(el) {
                                            const candidates = [];
                                            function check(node) {
                                                if(!node) return;
                                                const style = window.getComputedStyle(node);
                                                const hasScroll = style.overflowY === 'auto' || style.overflowY === 'scroll';
                                                const isScrollable = node.scrollHeight > node.clientHeight;
                                                const score = (hasScroll ? 2 : 0) + (isScrollable ? 1 : 0);
                                                if(score > 0) {
                                                    candidates.push({node, score, 
                                                        height: node.scrollHeight,
                                                        visible: node.clientHeight
                                                    });
                                                }
                                                Array.from(node.children).forEach(check);
                                            }
                                            check(el);
                                            if(!candidates.length) return el;
                                            // Prefer explicitly scrollable + actually has content to scroll
                                            candidates.sort((a,b) => 
                                                (b.score * b.height) - (a.score * a.height)
                                            );
                                            return candidates[0].node;
                                        }
                                        
                                        const container = findBestScrollable(modal);
                                        try {
                                            // Smaller, more frequent scrolls
                                            const scrollAmount = Math.floor(Math.random() * 300) + 300;
                                            container.scrollTo({
                                                top: container.scrollTop + scrollAmount,
                                                behavior: 'smooth'
                                            });
                                            
                                            // Also try keyboard scroll
                                            if(modal_rounds % 3 === 0) {
                                                container.dispatchEvent(new KeyboardEvent('keydown', {
                                                    key: 'PageDown',
                                                    code: 'PageDown',
                                                    bubbles: true
                                                }));
                                            }
                                        } catch(e) {}
                                        
                                        return ids;
                                    }
                                """, modal_selector)
                            except Exception as e:
                                logger.debug('Modal evaluate failed: %s', e)
                                ids = []

                            new_ids = [i for i in ids if i and i not in modal_seen_ids]
                            if new_ids:
                                for nid in new_ids:
                                    modal_seen_ids.add(nid)
                                modal_no_new = 0
                            else:
                                modal_no_new += 1

                            logger.debug('Modal round %d: total ids=%d, new=%d', modal_rounds, len(modal_seen_ids), len(new_ids))

                            if len(modal_seen_ids) >= MAX_REVIEWS_PER_BRANCH:
                                logger.info('Modal collected %d review ids (target reached)', len(modal_seen_ids))
                                break

                            if modal_no_new >= max_modal_no_new:
                                logger.info('No new modal ids after %d rounds, stopping modal scroll', modal_no_new)
                                break

                            page.wait_for_timeout(int((SCROLL_PAUSE_TIME + random.random()*1.5) * 1000))

                        # Save final modal snapshot for debugging
                        try:
                            modal_el = page.query_selector(modal_selector)
                            if modal_el:
                                modal_html = page.evaluate('(el) => el.outerHTML', modal_el)
                                modal_debug_path = os.path.join(os.getcwd(), "debug_modal.html")
                                with open(modal_debug_path, "w", encoding="utf-8") as mh:
                                    mh.write(modal_html)
                                logger.debug('Wrote final debug modal HTML to %s', modal_debug_path)
                        except Exception as e:
                            logger.debug('Failed to write final modal HTML: %s', e)

                    except Exception as e:
                        logger.debug('Modal-specific scrolling failed: %s', e)

                while stagnant_loops < max_stagnant_loops and total_attempts < max_total_attempts:
                    # Use broader selector set for counting reviews
                    current_review_count = page.evaluate(f'''() => {{
                        const reviewElements = document.querySelectorAll('{review_selector}');
                        return Array.from(reviewElements).filter(el => 
                            el.textContent.length > 20 || 
                            el.querySelector('span[aria-label*="stars"]')
                        ).length;
                    }}''')
                    
                    # Check if we have new reviews
                    if current_review_count > last_review_count:
                        no_new_reviews_count = 0
                        last_review_count = current_review_count
                    else:
                        no_new_reviews_count += 1

                    if current_review_count >= MAX_REVIEWS_PER_BRANCH:
                        logger.info(f"Loaded {current_review_count} reviews (target {MAX_REVIEWS_PER_BRANCH}) for {name}. Stopping scroll.")
                        break

                    if time.time() - start_time > timeout_seconds:
                        logger.warning(f"Timeout reached for {name}, stopping scroll at {current_review_count} reviews.")
                        break

                    # Randomize scroll timing for more human-like behavior
                    scroll_wait = SCROLL_PAUSE_TIME + (random.random() * 2)
                    
                    # Scroll with retry mechanism
                    scroll_success = False
                    for _ in range(3):  # Try scrolling up to 3 times
                        try:
                            scroll_success = page.evaluate(scroll_script)
                            if scroll_success:
                                break
                            page.wait_for_timeout(500)  # Short pause between retry attempts
                        except Exception as e:
                            logger.debug(f"Scroll attempt failed: {e}")
                            continue

                    page.wait_for_timeout(int(scroll_wait * 1000))

                    # More aggressive expansion of review text
                    try:
                        # Try to expand all available "More" buttons every other scroll
                        if current_review_count % 2 == 0:
                            expand_buttons = [
                                "button.w8nwRe.kyuRq[aria-expanded='false']",  # Primary More button class
                                "button[jsaction*='review.expandReview']",      # Action-based selector
                                "button.kyuRq[aria-label='See more']",         # Label-based selector
                                "button[aria-controls]:not([aria-expanded='true'])", # Generic expand buttons
                                "button:has-text('More')",                     # Text-based selector
                                "button:has-text('See more')",                 # Alternative text
                                "button:has-text('المزيد')"                    # Arabic More button
                            ]
                            
                            expanded = 0
                            for selector in expand_buttons:
                                try:
                                    buttons = page.query_selector_all(selector)
                                    for btn in buttons:
                                        try:
                                            if btn.is_visible():
                                                btn.scroll_into_view_if_needed()
                                                btn.click(timeout=800)
                                                expanded += 1
                                                page.wait_for_timeout(100)  # Short pause between clicks
                                        except Exception:
                                            continue
                                except Exception:
                                    continue
                            
                            if expanded:
                                logger.info("Expanded %d 'More' buttons for %s", expanded, name)
                                page.wait_for_timeout(1000)  # Wait for expansions to complete
                        
                        # Try sorting options occasionally to potentially load different reviews
                        if no_new_reviews_count >= 5:
                            try:
                                sort_button = page.query_selector("button[aria-label*='Sort reviews']")
                                if sort_button:
                                    sort_button.click()
                                    page.wait_for_timeout(1000)
                                    # Try different sort options
                                    sort_options = page.query_selector_all("div[role='menuitemradio']")
                                    if sort_options:
                                        random.choice(sort_options).click()
                                    no_new_reviews_count = 0
                            except Exception:
                                pass
                    except Exception as e:
                        logger.debug(f"Error during review expansion: {e}")

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
                        total_attempts += 1
                    else:
                        stagnant_loops = 0
                        previous_height = current_height

                    if stagnant_loops >= max_stagnant_loops and current_review_count < 10:
                        logger.warning(f"Only {current_review_count} reviews loaded, retrying scroll for {name}")
                        stagnant_loops = 0

                page.wait_for_timeout(3000 + random.randint(0, 1500))
                logger.info("Finished scrolling all reviews for %s", name)

            except Exception as e:
                logger.warning("Scrolling failed for %s: %s", name, e)

            finally:
                # Ensure we have some reviews loaded before saving debug HTML
                try:
                    review_count = page.evaluate('document.querySelectorAll("div.jftiEf[data-review-id], div[data-review-id]").length')
                    logger.debug(f"Current review count before saving HTML: {review_count}")
                    
                    if review_count > 0:
                        html = page.content()
                        debug_path = os.path.join(os.getcwd(), "debug_page.html")
                        with open(debug_path, "w", encoding="utf-8") as fh:
                            fh.write(html)
                        logger.debug(f"Wrote debug HTML with {review_count} reviews to {debug_path}")
                    else:
                        logger.warning("No reviews found in page, waiting 5s and retrying once...")
                        page.wait_for_timeout(5000)
                        html = page.content()
                        debug_path = os.path.join(os.getcwd(), "debug_page.html")
                        with open(debug_path, "w", encoding="utf-8") as fh:
                            fh.write(html)
                        logger.debug("Wrote retry debug HTML to %s", debug_path)
                except Exception as e:
                    logger.debug("Failed to write debug HTML: %s", e)
                if _is_captcha_page(html):
                    logger.warning("CAPTCHA detected after scrolling for %s — skipping", url)
                    return {"name": name, "url": url, "skipped": True, "reason": "captcha_after_scroll", "reviews": []}

                # Attempt additional live scrolling rounds inside the page to load more reviews
                try:
                    get_ids_and_scroll = '''(selectors) => {
                        const sel = selectors.join(',');
                        const items = Array.from(document.querySelectorAll(sel));
                        function findScrollable(el){
                            while(el){
                                const style = window.getComputedStyle(el);
                                const overflowY = style.overflowY;
                                if ((overflowY === 'auto' || overflowY === 'scroll') && el.scrollHeight > el.clientHeight){
                                    return el;
                                }
                                el = el.parentElement;
                            }
                            return document.scrollingElement || document.documentElement;
                        }
                        const first = items[0] || document.querySelector('div[role="feed"]') || document.querySelector('#pane') || document.body;
                        const container = findScrollable(first);
                        const ids = items.map(i => i.getAttribute('data-review-id') || i.getAttribute('data-reviewid') || (i.innerText || '').slice(0,160));
                        try{ container.scrollTop = container.scrollHeight; }catch(e){ try{ window.scrollTo({top: document.body.scrollHeight, behavior: 'auto'}); }catch(e){} }
                        return ids;
                    }'''

                    fancy_selectors = [
                        'div[data-review-id]',
                        'div.jftiEf',
                        'div.hjmQqc',
                        'div.VjjEkf',
                        '.section-review, .section-result, .section-listing'
                    ]

                    seen_live_ids = set()
                    no_new_rounds = 0
                    max_no_new_rounds = 8
                    rounds = 0
                    max_rounds = 200

                    while rounds < max_rounds:
                        rounds += 1
                        try:
                            ids = page.evaluate(get_ids_and_scroll, fancy_selectors)
                        except Exception as e:
                            logger.debug('evaluate failed while getting ids: %s', e)
                            ids = []

                        new_ids = [i for i in ids if i and i not in seen_live_ids]
                        if new_ids:
                            for nid in new_ids:
                                seen_live_ids.add(nid)
                            no_new_rounds = 0
                        else:
                            no_new_rounds += 1

                        logger.debug('Live scroll round %d: total ids=%d, new=%d', rounds, len(seen_live_ids), len(new_ids))

                        if len(seen_live_ids) >= MAX_REVIEWS_PER_BRANCH:
                            logger.info('Collected %d live review ids (target reached)', len(seen_live_ids))
                            break

                        if no_new_rounds >= max_no_new_rounds:
                            logger.info('No new review ids after %d rounds, stopping live scroll', no_new_rounds)
                            break

                        # Wait a bit before next round
                        page.wait_for_timeout(int((SCROLL_PAUSE_TIME + random.random()*2) * 1000))

                    logger.info('Live scroll rounds finished: %d rounds, %d ids', rounds, len(seen_live_ids))

                    # refresh HTML snapshot after live scrolling
                    html = page.content()
                    debug_path = os.path.join(os.getcwd(), "debug_page.html")
                    with open(debug_path, "w", encoding="utf-8") as fh:
                        fh.write(html)
                    logger.debug("Wrote updated debug HTML with live scroll to %s", debug_path)

                    # Additional aggressive snapshot+scroll rounds to capture more reviews
                    try:
                        accum = {}
                        extra_rounds = 12
                        for extra in range(extra_rounds):
                            # extract from current HTML snapshot
                            cur_html = page.content()
                            extracted = _extract_reviews_from_html(cur_html)
                            for r in extracted:
                                rid = r.get('review_id')
                                if rid and rid not in accum:
                                    accum[rid] = r

                            logger.debug('Aggressive snapshot round %d: total unique reviews=%d', extra+1, len(accum))

                            if len(accum) >= MAX_REVIEWS_PER_BRANCH:
                                logger.info('Reached target during aggressive snapshots: %d', len(accum))
                                break

                            # try to scroll a bit more (smaller, frequent scrolls)
                            try:
                                page.evaluate(scroll_script)
                            except Exception:
                                pass

                            # try keyboard 'End' to force loading
                            try:
                                page.keyboard.press('End')
                            except Exception:
                                pass

                            page.wait_for_timeout(int((SCROLL_PAUSE_TIME/2 + random.random()*1.5) * 1000))

                        # if we collected anything, replace html and proceed
                        if accum:
                            reviews_from_accum = list(accum.values())
                            # create a combined html snapshot again for final parsing
                            html = page.content()
                            # prefer the accumulated reviews when deciding counts
                            logger.info('Aggressive accumulation finished: %d unique reviews collected', len(reviews_from_accum))
                    except Exception as e:
                        logger.debug('Aggressive snapshot rounds failed: %s', e)

                except Exception as e:
                    logger.debug("Live scroll rounds failed: %s", e)

                # Extract reviews from the HTML - no deduplication
                reviews = _extract_reviews_from_html(html)
                logger.info("Found %d reviews (heuristic) for %s", len(reviews), name)

                # Use all reviews found in this run
                new_reviews = reviews

                # append new reviews to CSV immediately to avoid losing progress
                if new_reviews:
                    csv_path = os.path.join(os.getcwd(), OUTPUT_CSV_FILE)
                    try:
                        # Filter / clean reviews to remove non-English text fragments before saving
                        # Strategy:
                        #  - If the review text contains a marker like '(Original)', keep only the part before it
                        #  - Remove non-ASCII characters (drops Arabic/other scripts) so CSV contains only English/ASCII
                        #  - If cleaning yields an empty string, skip that review
                        def _cleanup_keep_english(raw_text: str) -> str:
                            if not raw_text:
                                return raw_text
                            txt = raw_text
                            # If the page included both translated and original, many examples use '(Original)'
                            # Keep only the portion before that marker when present.
                            markers = ['(Original)', '(original)', '\n(Original)', '\n(Original)']
                            for m in markers:
                                idx = txt.find(m)
                                if idx != -1:
                                    txt = txt[:idx]
                                    break
                            # Remove leftover markers like '(Translated by Google)'
                            txt = re.sub(r'\(Translated by Google\)', ' ', txt, flags=re.I)
                            # Remove non-ASCII characters (this drops Arabic, Cyrillic, etc.)
                            txt = re.sub(r'[^\x00-\x7F]+', ' ', txt)
                            # Collapse whitespace
                            txt = re.sub(r'\s+', ' ', txt).strip()
                            return txt

                        english_reviews = []
                        skipped_non_english = 0
                        for review in new_reviews:
                            try:
                                raw = (review.get('text') or '')
                                # If text is very short, keep it (it might be a one-word English review)
                                if len(raw.strip()) <= 10:
                                    # still clean non-ascii chars
                                    review['text'] = _cleanup_keep_english(raw)
                                    if review['text']:
                                        english_reviews.append(review)
                                    else:
                                        skipped_non_english += 1
                                    continue

                                # Try to detect language; if detection fails, fall back to cleaning heuristics
                                try:
                                    lang = detect(raw)
                                except Exception:
                                    lang = None

                                cleaned = _cleanup_keep_english(raw)

                                # If cleaned content is empty, skip
                                if not cleaned:
                                    skipped_non_english += 1
                                    continue

                                # If lang is English or detection failed but cleaned text is present, keep cleaned text
                                if lang == 'en' or lang is None:
                                    review['text'] = cleaned
                                    english_reviews.append(review)
                                else:
                                    # Detected non-English, but cleaned text may still contain an English translation part
                                    # e.g. '(Translated by Google) ... (Original) ...' — cleaned will keep the translated part
                                    review['text'] = cleaned
                                    if cleaned:
                                        english_reviews.append(review)
                                    else:
                                        skipped_non_english += 1
                            except Exception as e:
                                logger.debug("Error cleaning review text: %s", e)
                                # As a safe fallback, keep the review unchanged
                                english_reviews.append(review)

                        if skipped_non_english > 0:
                            logger.info("Skipped %d non-English reviews after cleaning", skipped_non_english)

                        df_new = pd.DataFrame(english_reviews)
                        # Write CSV: overwrite by default for single-branch runs.
                        # Set environment variable `APPEND_TO_CSV=1` to preserve append behaviour.
                        if APPEND_TO_CSV:
                            if not os.path.exists(csv_path):
                                df_new.to_csv(csv_path, index=False, encoding="utf-8")
                            else:
                                df_new.to_csv(csv_path, mode="a", header=False, index=False, encoding="utf-8")
                        else:
                            # Overwrite the output file with the current run's reviews
                            df_new.to_csv(csv_path, index=False, encoding="utf-8")
                        logger.info("Appended %d English reviews to %s", len(english_reviews), csv_path)
                    except Exception as e:
                        logger.debug("Failed to append reviews to csv: %s", e)

                # persist the new reviews into the in-memory accumulator
                all_reviews.extend(new_reviews)
                # found_count: number of reviews heuristically discovered on the page
                found_count = len(reviews)
                # saved_count: number of NEW reviews appended (not previously seen)
                saved_count = len(new_reviews)

        except PlaywrightTimeoutError as e:
            logger.exception("Timeout while loading %s: %s", url, e)
            return {"name": name, "url": url, "skipped": True, "reason": "timeout", "found": len(all_reviews), "reviews": all_reviews}
        except Exception as e:
            logger.exception("Error processing %s : %s", url, e)
            return {"name": name, "url": url, "skipped": True, "reason": str(e), "found": len(all_reviews), "reviews": all_reviews}
        finally:
            try:
                page.close()
                context.close()
                browser.close()
            except Exception:
                pass

    # Return both the heuristic found count and how many new reviews were saved
    return {"name": name, "url": url, "skipped": False, "found": found_count, "new": saved_count if 'saved_count' in locals() else 0, "reviews": all_reviews}

# ==============================================================================
# 🚀 ENTRY POINT
# ==============================================================================

if __name__ == "__main__":
    if not BRANCH_URL:
        logger.error("No branch URL is defined. Please set BRANCH_URL at the top of the script.")
        raise SystemExit(1)

    logger.info("Starting review scrape for branch: %s", BRANCH_NAME)
    
    # Scrape the single branch defined at the top
    result = scrape_reviews_from_place_url(BRANCH_NAME, BRANCH_URL)
    
    scraped_reviews = result["reviews"]

    # result['found'] is the heuristic count discovered on the page
    # result['new'] is the number of NEW reviews appended in this run
    found = result.get("found", 0)
    new = result.get("new", 0)

    if new > 0:
        logger.info(f"Successfully scraped and saved {new} new reviews to {OUTPUT_CSV_FILE}")
    else:
        if found > 0:
            logger.warning(f"Found {found} reviews on the page but 0 new reviews to save (all already seen).")
        else:
            logger.warning("No reviews were successfully scraped.")

    #logger.info("Finished. Summary: %s", result)