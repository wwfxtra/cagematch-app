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


def _extract_worker_nr(href):
    """Extract numeric worker ID from a cagematch href (e.g. ?id=2&nr=80&...)."""
    if not href:
        return None
    m = re.search(r'nr=(\d+)', href)
    return m.group(1) if m else None


def rating_to_stars(rating):
    """Convert cagematch 0-10 rating to star display string."""
    if not rating:
        return ""
    stars = rating / 2  # 10 = 5 stars
    full = int(stars)
    half = 1 if (stars - full) >= 0.25 else 0
    return "★" * full + ("½" if half else "") + "☆" * (5 - full - half)


def _parse_won_stars(text):
    """Parse Meltzer/WON star text (e.g. '****1/4', '*****', '***3/4') to 0-10 float.
    Returns 0.0 if unparseable."""
    if not text:
        return 0.0
    text = text.strip()
    full = text.count('*')
    if full == 0:
        return 0.0
    frac = 0.0
    if '3/4' in text or '\u00be' in text:
        frac = 0.75
    elif '1/2' in text or '\u00bd' in text:
        frac = 0.5
    elif '1/4' in text or '\u00bc' in text:
        frac = 0.25
    stars = round(full + frac, 2)
    return round(stars * 2, 2)  # Convert to 0-10 scale for consistency


def parse_match_table(soup):
    """Parse the match ratings table from a cagematch ratings page.

    Cagematch table structure (7 columns):
      [0] #  |  [1] Date  |  [2] Promotion (img)  |  [3] Match fixture
      [4] WON  |  [5] Rating  |  [6] Votes
    """
    matches = []

    # Find the matches table (class TBase on cagematch)
    all_tables = soup.find_all("table")
    table = (
        soup.find("table", class_="TBase")
        or soup.find("table", class_="SearchResults")
    )
    if not table and all_tables:
        table = max(all_tables, key=lambda t: len(t.find_all("tr")))
    if not table:
        print("[scraper] No table found on page")
        return matches

    rows = table.find_all("tr")
    print(f"[scraper] Parsing table with {len(rows)} rows")

    # Detect column positions from the header row
    # Header cells contain: '#', 'Date', 'Promotion', 'Match fixture', 'WON', 'Rating', 'Votes'
    date_col = 1      # default: column 1 is Date
    match_col = 3     # default: column 3 is Match fixture
    promo_col = 2     # default: column 2 is Promotion
    rating_col = 5    # default: column 5 is Rating
    votes_col = 6     # default: column 6 is Votes

    header_cells = rows[0].find_all(["th", "td"]) if rows else []
    header_texts = [c.get_text(strip=True).lower() for c in header_cells]
    print(f"[scraper] Header row: {header_texts}")
    if header_texts:
        for i, h in enumerate(header_texts):
            if h in ("date", "datum"):
                date_col = i
            elif "match" in h or "fixture" in h:
                match_col = i
            elif h == "rating":
                rating_col = i
            elif h == "votes":
                votes_col = i
            elif h == "promotion":
                promo_col = i

    print(f"[scraper] Column map: date={date_col}, promo={promo_col}, match={match_col}, rating={rating_col}, votes={votes_col}")

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < rating_col + 1:
            continue

        try:
            # Skip header rows (cell at date_col contains "date" or "datum")
            date_text = cells[date_col].get_text(strip=True) if date_col < len(cells) else ""
            if not date_text or date_text.lower() in ("date", "datum"):
                continue

            date = date_text

            match_cell = cells[match_col] if match_col < len(cells) else cells[1]
            match_text = match_cell.get_text(" vs ", strip=False).strip()
            match_text = re.sub(r'\s+', ' ', match_text)
            match_link_tag = match_cell.find("a")
            match_link = None
            if match_link_tag and match_link_tag.get("href"):
                href = match_link_tag["href"]
                match_link = href if href.startswith("http") else BASE_URL + "/" + href.lstrip("/")

            # Promotion: prefer image alt text (logo cell), fallback to text
            promo_cell = cells[promo_col] if promo_col < len(cells) else None
            if promo_cell:
                img = promo_cell.find("img")
                promotion = img.get("title") or img.get("alt") or "" if img else promo_cell.get_text(strip=True)
            else:
                promotion = ""

            # WON column (index 4) — parse Meltzer star rating AND extract event
            won_col = match_col + 1
            won_text = cells[won_col].get_text(strip=True) if won_col < len(cells) and won_col != rating_col else ""
            won_rating = _parse_won_stars(won_text)

            # Event info (use WON column text if it's not the rating col, else empty)
            event = won_text if won_col < len(cells) and won_col != rating_col and not won_text.startswith('*') else ""

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
                    "won_rating": won_rating,
                    "votes": int(votes),
                    "link": match_link,
                    "stars_display": rating_to_stars(rating),
                    "stars_numeric": round(rating / 2, 2),
                    "won_stars_display": rating_to_stars(won_rating) if won_rating else "",
                    "won_stars_numeric": round(won_rating / 2, 2) if won_rating else 0,
                    "rating_source": "community",
                })
        except Exception as e:
            print(f"[scraper] Row parse error: {e}")
            continue

    matches.sort(key=lambda m: m["rating"], reverse=True)
    print(f"[scraper] Parsed {len(matches)} matches")
    return matches


def build_matchguide_url(nr, offset=0, sortby="colMeltzer", sorttype="DESC"):
    """Build URL for wrestler's matchguide sorted by Meltzer rating."""
    params = [
        ("id", "2"),
        ("nr", str(nr)),
        ("page", "10"),
        ("sortby", sortby),
        ("sorttype", sorttype),
        ("s", str(offset)),
    ]
    return BASE_URL + "/?" + urlencode(params)


def parse_matchguide_table(soup):
    """Parse the wrestler matchguide table (?id=2&nr=X&page=10).

    Table columns (6):
      [0] #  |  [1] Date  |  [2] Promotion  |  [3] Match fixture
      [4] WON (Meltzer stars text)  |  [5] Match Type
    """
    matches = []
    all_tables = soup.find_all("table")
    table = (
        soup.find("table", class_="TBase")
        or soup.find("table", class_="SearchResults")
    )
    if not table and all_tables:
        table = max(all_tables, key=lambda t: len(t.find_all("tr")))
    if not table:
        print("[scraper] No matchguide table found")
        return matches

    rows = table.find_all("tr")
    print(f"[scraper] Parsing matchguide table with {len(rows)} rows")

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 5:
            continue
        try:
            date_text = cells[1].get_text(strip=True)
            if not date_text or date_text.lower() in ("date", "datum"):
                continue

            promo_cell = cells[2]
            img = promo_cell.find("img")
            promotion = img.get("title") or img.get("alt") or "" if img else promo_cell.get_text(strip=True)

            match_cell = cells[3]
            match_text = re.sub(r'\s+', ' ', match_cell.get_text(" vs ", strip=False).strip())
            match_link_tag = match_cell.find("a")
            match_link = None
            if match_link_tag and match_link_tag.get("href"):
                href = match_link_tag["href"]
                match_link = href if href.startswith("http") else BASE_URL + "/" + href.lstrip("/")

            won_text = cells[4].get_text(strip=True) if len(cells) > 4 else ""
            won_rating = _parse_won_stars(won_text)
            if won_rating <= 0:
                continue

            matches.append({
                "date": date_text,
                "match": match_text,
                "event": "",
                "promotion": promotion,
                "rating": won_rating,        # 0-10 scale (Meltzer * 2)
                "won_rating": won_rating,
                "votes": 0,
                "link": match_link,
                "stars_display": rating_to_stars(won_rating),
                "stars_numeric": round(won_rating / 2, 2),
                "won_stars_display": rating_to_stars(won_rating),
                "won_stars_numeric": round(won_rating / 2, 2),
                "rating_source": "meltzer",
            })
        except Exception as e:
            print(f"[scraper] Matchguide row parse error: {e}")
            continue

    print(f"[scraper] Parsed {len(matches)} matches from matchguide")
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


def get_worker_nr(worker_name):
    """Resolve a wrestler name to their cagematch numeric ID (nr=)."""
    results = search_wrestlers(worker_name)
    if results:
        nr = results[0].get("nr")
        print(f"[scraper] Resolved '{worker_name}' -> nr={nr}")
        return nr
    print(f"[scraper] Could not resolve worker nr for '{worker_name}'")
    return None


def get_matches(worker=None, year=None, promotion_id=None, min_rating=None, pages=1):
    """Get rated matches with optional filters. Fetches up to `pages` pages.

    Worker searches use the wrestler's matchguide sorted by Meltzer rating.
    Year/promo searches use the global ratings page (community /10 ratings).
    """
    cache_key = f"matches|{worker}|{year}|{promotion_id}|{min_rating}|{pages}"

    def fetch():
        all_matches = []

        if worker:
            # Worker search: use matchguide sorted by Meltzer (colMeltzer DESC)
            nr = get_worker_nr(worker)
            if not nr:
                print(f"[scraper] Could not resolve worker nr for '{worker}'")
                return []
            for page in range(pages):
                url = build_matchguide_url(nr, offset=page * 100)
                print(f"[scraper] Fetching matchguide: {url}")
                soup = fetch_soup(url, delay=0.8 if page == 0 else 1.5)
                if not soup:
                    break
                page_matches = parse_matchguide_table(soup)
                if not page_matches:
                    break
                all_matches.extend(page_matches)
            # Apply min_rating filter: API sends 0-5 star scale, rating stored as 0-10
            if min_rating is not None:
                min_10 = float(min_rating) * 2
                all_matches = [m for m in all_matches if m["rating"] >= min_10]
        else:
            # Year/promo search: use global "On This Day" ratings page
            for page in range(pages):
                url = build_ratings_url(
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
                    wrestlers.append({
                        "name": name,
                        "href": href,
                        "nr": _extract_worker_nr(href),
                    })

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
