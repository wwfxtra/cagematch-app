"""
scraper.py — Cagematch.net data fetcher
Scrapes match ratings, wrestler search, and filters.
"""

import requests
from bs4 import BeautifulSoup
import time
import re
from datetime import datetime, timedelta

BASE_URL = "https://www.cagematch.net"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Simple in-memory cache: {cache_key: (timestamp, data)}
_cache = {}
CACHE_TTL_SECONDS = 1800  # 30 minutes


def _cached(key, fetch_fn):
    now = datetime.now()
    if key in _cache:
        ts, data = _cache[key]
        if now - ts < timedelta(seconds=CACHE_TTL_SECONDS):
            return data
    data = fetch_fn()
    _cache[key] = (now, data)
    return data


def fetch_soup(url, delay=1.0):
    """Fetch a URL and return a BeautifulSoup object."""
    try:
        time.sleep(delay)  # Be polite to cagematch
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        print(f"[scraper] Error fetching {url}: {e}")
        return None


def rating_to_stars(rating):
    """Convert cagematch 0-10 rating to star display string."""
    if not rating:
        return ""
    stars = rating / 2  # 10 = 5 stars
    full = int(stars)
    half = 1 if (stars - full) >= 0.5 else 0
    return "★" * full + ("½" if half else "") + "☆" * (5 - full - half)


def parse_match_table(soup):
    """Parse the match ratings table from a cagematch ratings page."""
    matches = []

    # Cagematch uses various table classes — try multiple selectors
    table = (
        soup.find("table", class_="TBase")
        or soup.find("table", class_="SearchResults")
        or soup.find("table", id=re.compile(r"match", re.I))
    )
    if not table:
        # Fallback: find the largest table on the page (likely the results table)
        all_tables = soup.find_all("table")
        if all_tables:
            table = max(all_tables, key=lambda t: len(t.find_all("tr")))
    if not table:
        print("[scraper] No table found on page")
        return matches

    rows = table.find_all("tr")
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 5:
            continue

        try:
            date = cells[0].get_text(strip=True)
            # Skip header rows
            if date.lower() in ("date", ""):
                continue

            match_cell = cells[1]
            match_text = match_cell.get_text(" vs ", strip=False).strip()
            # Clean up extra whitespace
            match_text = re.sub(r'\s+', ' ', match_text)
            match_link_tag = match_cell.find("a")
            match_link = (BASE_URL + "/" + match_link_tag["href"].lstrip("/")
                          if match_link_tag and match_link_tag.get("href") else None)

            event_cell = cells[2]
            event = event_cell.get_text(strip=True)

            promotion_cell = cells[3]
            promotion = promotion_cell.get_text(strip=True)

            rating_cell = cells[4]
            rating_text = rating_cell.get_text(strip=True)
            try:
                rating = float(rating_text)
            except (ValueError, TypeError):
                rating = 0.0

            votes_cell = cells[5] if len(cells) > 5 else None
            votes = votes_cell.get_text(strip=True) if votes_cell else "0"
            # Remove non-numeric chars
            votes = re.sub(r'[^\d]', '', votes) or "0"

            if rating > 0:
                matches.append({
                    "date": date,
                    "match": match_text,
                    "event": event,
                    "promotion": promotion,
                    "rating": rating,
                    "votes": int(votes),
                    "link": match_link,
                    "stars_display": rating_to_stars(rating),
                    "stars_numeric": round(rating / 2, 2),
                })
        except Exception as e:
            print(f"[scraper] Row parse error: {e}")
            continue

    # Sort by rating descending
    matches.sort(key=lambda m: m["rating"], reverse=True)
    return matches


def build_ratings_url(worker=None, year=None, promotion_id=None, min_rating=None, offset=0):
    params = {
        "id": "111",
        "view": "matches",
        "s": str(offset),
    }
    if worker:
        params["worker"] = worker
    if year:
        params["year"] = str(year)
    if promotion_id:
        params["promotion"] = str(promotion_id)
    if min_rating is not None:
        # Cagematch uses 0-10 scale; convert if needed
        params["minrating"] = str(int(float(min_rating) * 10))
    return BASE_URL + "/?" + "&".join(f"{k}={v}" for k, v in params.items())


def get_matches(worker=None, year=None, promotion_id=None, min_rating=None, pages=1):
    """Get rated matches with optional filters. Fetches up to `pages` pages."""
    cache_key = f"matches|{worker}|{year}|{promotion_id}|{min_rating}|{pages}"

    def fetch():
        all_matches = []
        for page in range(pages):
            url = build_ratings_url(
                worker=worker,
                year=year,
                promotion_id=promotion_id,
                min_rating=min_rating,
                offset=page * 100,
            )
            print(f"[scraper] Fetching: {url}")
            soup = fetch_soup(url, delay=0.8 if page == 0 else 1.5)
            if not soup:
                break
            page_matches = parse_match_table(soup)
            if not page_matches:
                break
            all_matches.extend(page_matches)

        # Re-sort combined results
        all_matches.sort(key=lambda m: m["rating"], reverse=True)
        return all_matches

    return _cached(cache_key, fetch)


def search_wrestlers(query):
    """Search for wrestlers by name on cagematch."""
    if len(query) < 2:
        return []

    cache_key = f"wrestlers|{query.lower()}"

    def fetch():
        url = f"{BASE_URL}/?id=2&view=workers&search={requests.utils.quote(query)}"
        print(f"[scraper] Searching wrestlers: {url}")
        soup = fetch_soup(url, delay=0.5)
        if not soup:
            return []

        wrestlers = []
        rows = soup.find_all("tr", class_=["TRow1", "TRow2"])
        for row in rows:
            cells = row.find_all("td")
            if not cells:
                continue
            link_tag = cells[0].find("a") if cells else None
            if link_tag:
                name = link_tag.get_text(strip=True)
                href = link_tag.get("href", "")
                wrestlers.append({"name": name, "href": href})

        return wrestlers[:20]

    return _cached(cache_key, fetch)


def get_promotions():
    """Return a curated list of major promotions with their cagematch IDs."""
    return [
        {"id": "1",   "name": "WWE"},
        {"id": "2",   "name": "WCW"},
        {"id": "3",   "name": "ECW"},
        {"id": "5",   "name": "NWA"},
        {"id": "6",   "name": "NJPW"},
        {"id": "8",   "name": "ROH"},
        {"id": "14",  "name": "CMLL"},
        {"id": "22",  "name": "TNA / Impact"},
        {"id": "25",  "name": "AAA"},
        {"id": "74",  "name": "NXT"},
        {"id": "447", "name": "AEW"},
    ]
