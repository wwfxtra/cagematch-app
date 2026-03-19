"""
scraper.py — Cagematch.net data fetcher
Scrapes match ratings, wrestler search, and filters.
"""

import requests
from bs4 import BeautifulSoup
import time
import re
from datetime import datetime, timedelta
from urllib.parse import urlencode

BASE_URL = "https://www.cagematch.net"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.cagematch.net/",
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
        time.sleep(delay)
        resp = requests.get(url, headers=HEADERS, timeout=20)
        print(f"[scraper] HTTP {resp.status_code} — {len(resp.text)} chars from {url}")
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "lxml")
    except Exception as e:
        print(f"[scraper] Error fetching {url}: {e}")
        return None


def fetch_raw(url, delay=1.0):
    """Fetch a URL and return raw text (for debugging)."""
    try:
        time.sleep(delay)
        resp = requests.get(url, headers=HEADERS, timeout=20)
        return resp.status_code, resp.text
    except Exception as e:
        return 0, str(e)


def rating_to_stars(rating):
    """Convert cagematch 0-10 rating to star display string."""
    if not rating:
        return ""
    stars = rating / 2  # 10 = 5 stars
    full = int(stars)
    half = 1 if (stars - full) >= 0.25 else 0
    return "★" * full + ("½" if half else "") + "☆" * (5 - full - half)


def parse_match_table(soup):
    """Parse the match ratings table from a cagematch ratings page."""
    matches = []

    # Log what tables exist on the page
    all_tables = soup.find_all("table")
    print(f"[scraper] Found {len(all_tables)} tables on page")
    for i, t in enumerate(all_tables):
        rows = t.find_all("tr")
        print(f"[scraper]   table[{i}] class={t.get('class')} id={t.get('id')} rows={len(rows)}")

    # Try specific cagematch table classes first
    table = (
        soup.find("table", class_="TBase")
        or soup.find("table", class_="SearchResults")
        or soup.find("table", id=re.compile(r"match", re.I))
    )
    if not table:
        # Fallback: largest table on the page
        if all_tables:
            table = max(all_tables, key=lambda t: len(t.find_all("tr")))
    if not table:
        print("[scraper] No table found on page")
        body = soup.find("body")
        if body:
            print(f"[scraper] Body snippet: {str(body)[:500]}")
        return matches

    rows = table.find_all("tr")
    print(f"[scraper] Parsing table with {len(rows)} rows")

    # Auto-detect which column holds the rating (a float 0–10).
    # Scan the first real data row (skip header rows with < 5 cells or "date" in cell 0).
    rating_col = None
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 5:
            continue
        if cells[0].get_text(strip=True).lower() in ("date", "", "datum"):
            continue
        # Log first row cells for diagnosis
        cell_texts = [c.get_text(strip=True) for c in cells]
        print(f"[scraper] First data row cells: {cell_texts}")
        # Search from index 3 onwards for a valid rating float
        for ci in range(3, len(cells)):
            txt = cells[ci].get_text(strip=True)
            try:
                v = float(txt)
                if 0 < v <= 10:
                    rating_col = ci
                    break
            except (ValueError, TypeError):
                pass
        break  # Only need the first data row

    if rating_col is None:
        print("[scraper] Could not auto-detect rating column — defaulting to 4")
        rating_col = 4

    votes_col = rating_col + 1
    # promotion is assumed to be the column just before rating
    promo_col = rating_col - 1
    print(f"[scraper] Column map: date=0, match=1, event=2, promo={promo_col}, rating={rating_col}, votes={votes_col}")

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < rating_col + 1:
            continue

        try:
            date = cells[0].get_text(strip=True)
            if date.lower() in ("date", "", "datum"):
                continue

            match_cell = cells[1]
            match_text = match_cell.get_text(" vs ", strip=False).strip()
            match_text = re.sub(r'\s+', ' ', match_text)
            match_link_tag = match_cell.find("a")
            match_link = None
            if match_link_tag and match_link_tag.get("href"):
                href = match_link_tag["href"]
                if href.startswith("http"):
                    match_link = href
                else:
                    match_link = BASE_URL + "/" + href.lstrip("/")

            event = cells[2].get_text(strip=True)
            promotion = cells[promo_col].get_text(strip=True) if promo_col >= 0 else ""

            rating_text = cells[rating_col].get_text(strip=True)
            try:
                rating = float(rating_text)
            except (ValueError, TypeError):
                rating = 0.0

            votes_cell = cells[votes_col] if votes_col < len(cells) else None
            votes = votes_cell.get_text(strip=True) if votes_cell else "0"
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

    matches.sort(key=lambda m: m["rating"], reverse=True)
    print(f"[scraper] Parsed {len(matches)} matches")
    return matches


def build_ratings_url(worker=None, year=None, promotion_id=None, min_rating=None, offset=0):
    params = [
        ("id", "111"),
        ("view", "matches"),
        ("s", str(offset)),
    ]
    if worker:
        params.append(("worker", worker))
    if year:
        params.append(("year", str(year)))
    if promotion_id:
        params.append(("promotion", str(promotion_id)))
    if min_rating is not None:
        # Cagematch uses 0-10 scale (10 = 5★); multiply stars by 2
        cm_rating = round(float(min_rating) * 2, 1)
        params.append(("minrating", str(cm_rating)))
    url = BASE_URL + "/?" + urlencode(params)
    return url


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

        all_matches.sort(key=lambda m: m["rating"], reverse=True)
        return all_matches

    return _cached(cache_key, fetch)


def search_wrestlers(query):
    """Search for wrestlers by name on cagematch."""
    if len(query) < 2:
        return []

    cache_key = f"wrestlers|{query.lower()}"

    def fetch():
        url = BASE_URL + "/?" + urlencode([("id", "2"), ("view", "workers"), ("search", query)])
        print(f"[scraper] Searching wrestlers: {url}")
        soup = fetch_soup(url, delay=0.5)
        if not soup:
            return []

        wrestlers = []
        # Try multiple row selectors
        rows = soup.find_all("tr", class_=["TRow1", "TRow2"])
        if not rows:
            rows = soup.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if not cells:
                continue
            link_tag = cells[0].find("a") if cells else None
            if link_tag:
                name = link_tag.get_text(strip=True)
                href = link_tag.get("href", "")
                if name:
                    wrestlers.append({"name": name, "href": href})

        print(f"[scraper] Found {len(wrestlers)} wrestlers for '{query}'")
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
