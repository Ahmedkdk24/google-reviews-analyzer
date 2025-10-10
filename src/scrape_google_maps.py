# src/scrape_google_maps.py
import time
import re
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
from .db import SessionLocal
from .models import Branch, Review
from datetime import datetime
import os

USER_AGENT = os.getenv("USER_AGENT") or "Mozilla/5.0"

def extract_place_id_from_maps_url(url):
    # Google Maps can encode place ID in URL as 'place/<name>/@lat,lng,...' or use query param 'cid' — fuzzy extraction
    # We'll try to find 'placeid' or 'cid' or fallback to full url
    parsed = urlparse(url)
    q = parse_qs(parsed.query)
    if "cid" in q:
        return q["cid"][0]
    # else fallback using url as identifier
    return url

def parse_reviews_from_place_html(html):
    # This is fragile but for a start we can look for review blocks using class names heuristically
    soup = BeautifulSoup(html, "html.parser")
    reviews = []

    # Look for <div role="article"> or typical Google review containers
    # We'll search text blocks that look like reviews: author + rating + text
    # This is a heuristic and should be hardened per observed markup.
    review_divs = soup.find_all(lambda tag: tag.name == "div" and tag.get("aria-label") and "Rated" in tag.get("aria-label", ""))
    if not review_divs:
        # fallback: find elements that look like review text
        review_divs = soup.find_all("div", string=lambda t: t and len(t) > 50)

    for d in review_divs[:10]:  # limit
        text = d.get_text(separator=" ", strip=True)
        # attempt to find rating in aria-label
        aria = d.get("aria-label") or ""
        m = re.search(r"Rated (\d) stars", aria)
        rating = int(m.group(1)) if m else None
        # very naive author extraction
        author = d.find_previous(lambda tag: tag.name in ("div", "span") and tag.get("role") == "link")
        author_text = author.get_text(strip=True) if author else "Unknown"
        reviews.append({"author": author_text, "rating": rating or 0, "text": text[:2000], "review_date": ""})
    return reviews

def scrape_maps_for_query(query, max_places=5):
    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=500)
        context = browser.new_context(
                        user_agent=USER_AGENT,
                        viewport={"width": 1280, "height": 800},
                        locale="en-US",
                    )

        page = context.new_page()
        # perform a Google search for query + "site:google.com/maps"
        search_url = f"https://www.google.com/search?q={query.replace(' ', '+')}+site:google.com/maps"
        print("Opening", search_url)
        page.goto(search_url, timeout=60000)
        time.sleep(1.5)

        # get search results links
        # wait for results to load
        page.wait_for_timeout(70000)

        # extract all hrefs robustly (Google keeps changing markup)
        html = page.content()
        soup = BeautifulSoup(html, "html.parser")
        map_links = []

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/maps/place" in href or "google.com/maps" in href:
                if href.startswith("/url?q="):
                    href = href.split("/url?q=")[1].split("&")[0]
                if href not in map_links:
                    map_links.append(href)

        map_links = map_links[:max_places]
        print(f"Found {len(map_links)} map links:")
        for link in map_links:
            print(" →", link)

        # fallback: find result items with href containing maps
        map_links = map_links[:max_places]
        print(f"Found {len(map_links)} map links")

        for href in map_links:
            try:
                # normalize url
                if href.startswith("/url?q="):
                    href = href.split("/url?q=")[1].split("&")[0]
                print("Visiting", href)
                page.goto(href, timeout=60000)
                time.sleep(2.5)  # allow dynamic load
                html = page.content()
                # try to extract name/address from page title/meta
                title = page.title()
                place_id = extract_place_id_from_maps_url(href)
                reviews = parse_reviews_from_place_html(html)
                # attempt to find address/phone via selectors
                # naive: look for 'seniors' or 'address' labels — we'll just store raw html pieces for now
                results.append({
                    "name": title,
                    "url": href,
                    "place_id": place_id,
                    "reviews": reviews,
                    "raw_html": html[:2000]
                })
            except Exception as e:
                print("Error visiting", href, e)
        browser.close()
    return results

def save_places_to_db(places):
    session = SessionLocal()
    try:
        for p in places:
            # upsert by place_id or url
            existing = session.query(Branch).filter((Branch.place_id==p["place_id"]) | (Branch.url==p["url"])).first()
            if existing:
                branch = existing
                branch.scraped_at = datetime.utcnow()
            else:
                branch = Branch(
                    name=p["name"],
                    url=p["url"],
                    place_id=p["place_id"],
                    address=None
                )
                session.add(branch)
                session.flush()  # get branch.id
            # save reviews
            for r in p["reviews"]:
                # naive duplicate check by text snippet
                already = session.query(Review).filter(Review.branch_id==branch.id, Review.text==r["text"]).first()
                if not already:
                    review = Review(
                        branch_id=branch.id,
                        author=r.get("author"),
                        rating=r.get("rating") or 0,
                        text=r.get("text"),
                        review_date=r.get("review_date")
                    )
                    session.add(review)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == "__main__":
    # quick test run if executed directly
    q = os.getenv("GOOGLE_SEARCH_QUERY", "Gym Nation Kuwait")
    places = scrape_maps_for_query(q, max_places=3)
    print("Scraped", len(places))
    save_places_to_db(places)
    print("Saved to DB")
