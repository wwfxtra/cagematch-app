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
from concurrent.futures import ThreadPoolExecutor, as_completed

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

_cache = {}
CACHE_TTL_SECONDS = 1800


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
    try:
        time.sleep(delay)
        resp = requests.get(url, headers=HEADERS, timeout=20)
        return resp.status_code, resp.text
    except Exception as e:
        return 0, str(e)


def _extract_worker_nr(href):
    if not href:
        return None
    m = re.search(r'nr=(\d+)', href)
    return m.group(1) if m else None


def rating_to_stars(rating):
    if not rating:
        return ""
    stars = rating / 2
    full = int(stars)
    half = 1 if (stars - full) >= 0.25 else 0
    return "*" * full + (".5" if half else "")


def _parse_won_stars(text):
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
    return round(round(full + frac, 2) * 2, 2)


PROMOTION_NAMES = {
    "1":   "World Wrestling Entertainment",
    "2":   "World Championship Wrestling",
    "3":   "Extreme Championship Wrestling",
    "5":   "National Wrestling Alliance",
    "6":   "New Japan Pro Wrestling",
    "8":   "Ring Of Honor",
    "14":  "Consejo Mundial de Lucha Libre",
    "22":  "Total Nonstop Action",
    "25":  "Asistencia",
    "74":  "NXT",
    "447": "All Elite Wrestling",
}


def build_promo_matchguide_url(nr, year=None, offset=0):
    params = [
        ("id", "8"),
        ("nr", str(nr)),
        ("page", "7"),
        ("sortby", "colRating"),
        ("sorttype", "DESC"),
        ("s", str(offset)),
    ]
    if year:
        params.append(("year", str(year)))
    return BASE_URL + "/?" + urlencode(params)


def parse_promo_matchguide(soup, promotion_name):
    matches = []
    all_tables = soup.find_all("table")
    table = soup.find("table", class_="TBase") or soup.find("table", class_="SearchResults")
    if not table and all_tables:
        table = max(all_tables, key=lambda t: len(t.find_all("tr")))
    if not table:
        print(f"[scraper] No table found for {promotion_name}")
        return matches

    rows = table.find_all("tr")
    print(f"[scraper] Parsing {promotion_name}: {len(rows)} rows")

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 4:
            continue
        try:
            date_text = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            if not date_text or date_text.lower() in ("date", "datum"):
                continue
            match_cell = cells[2] if len(cells) > 2 else cells[1]
            match_text = re.sub(r'\s+', ' ', match_cell.get_text(" vs ", strip=False).strip())
            match_link_tag = match_cell.find("a")
            match_link = None
            if match_link_tag and match_link_tag.get("href"):
                href = match_link_tag["href"]
                match_link = href if href.startswith("http") else BASE_URL + "/" + href.lstrip("/")
            won_text = cells[3].get_text(strip=True) if len(cells) > 3 else ""
            won_rating = _parse_won_stars(won_text)
            rating_text = cells[4].get_text(strip=True) if len(cells) > 4 else "0"
            try:
                rating = float(rating_text)
            except (ValueError, TypeError):
                rating = 0.0
            votes_text = cells[5].get_text(strip=True) if len(cells) > 5 else "0"
            votes = re.sub(r'[^\d]', '', votes_text) or "0"
            if rating > 0:
                matches.append({
                    "date": date_text, "match": match_text, "event": "",
                    "promotion": promotion_name, "rating": rating,
                    "won_rating": won_rating, "votes": int(votes), "link": match_link,
                    "stars_display": rating_to_stars(rating),
                    "stars_numeric": round(rating / 2, 2),
                    "won_stars_display": rating_to_stars(won_rating) if won_rating else "",
                    "won_stars_numeric": round(won_rating / 2, 2) if won_rating else 0,
                    "rating_source": "community",
                })
        except Exception as e:
            print(f"[scraper] Parse error ({promotion_name}): {e}")
            continue
    matches.sort(key=lambda m: m["rating"], reverse=True)
    print(f"[scraper] Got {len(matches)} from {promotion_name}")
    return matches


def parse_match_table(soup):
    matches = []
    all_tables = soup.find_all("table")
    table = soup.find("table", class_="TBase") or soup.find("table", class_="SearchResults")
    if not table and all_tables:
        table = max(all_tables, key=lambda t: len(t.find_all("tr")))
    if not table:
        return matches
    rows = table.find_all("tr")
    date_col = 1; match_col = 3; promo_col = 2; rating_col = 5; votes_col = 6
    header_cells = rows[0].find_all(["th", "td"]) if rows else []
    header_texts = [c.get_text(strip=True).lower() for c in header_cells]
    for i, h in enumerate(header_texts):
        if h in ("date", "datum"): date_col = i
        elif "match" in h or "fixture" in h: match_col = i
        elif h == "rating": rating_col = i
        elif h == "votes": votes_col = i
        elif h == "promotion": promo_col = i
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < rating_col + 1: continue
        try:
            date_text = cells[date_col].get_text(strip=True) if date_col < len(cells) else ""
            if not date_text or date_text.lower() in ("date", "datum"): continue
            match_cell = cells[match_col] if match_col < len(cells) else cells[1]
            match_text = re.sub(r'\s+', ' ', match_cell.get_text(" vs ", strip=False).strip())
            match_link_tag = match_cell.find("a")
            match_link = None
            if match_link_tag and match_link_tag.get("href"):
                href = match_link_tag["href"]
                match_link = href if href.startswith("http") else BASE_URL + "/" + href.lstrip("/")
            promo_cell = cells[promo_col] if promo_col < len(cells) else None
            if promo_cell:
                img = promo_cell.find("img")
                promotion = img.get("title") or img.get("alt") or "" if img else promo_cell.get_text(strip=True)
            else:
                promotion = ""
            won_col = match_col + 1
            won_text = cells[won_col].get_text(strip=True) if won_col < len(cells) and won_col != rating_col else ""
            won_rating = _parse_won_stars(won_text)
            event = won_text if won_col < len(cells) and won_col != rating_col and not won_text.startswith('*') else ""
            rating_text = cells[rating_col].get_text(strip=True)
            try: rating = float(rating_text)
            except: rating = 0.0
            votes_cell = cells[votes_col] if votes_col < len(cells) else None
            votes = re.sub(r'[^\d]', '', votes_cell.get_text(strip=True) if votes_cell else "") or "0"
            if rating > 0:
                matches.append({
                    "date": date_text, "match": match_text, "event": event,
                    "promotion": promotion, "rating": rating, "won_rating": won_rating,
                    "votes": int(votes), "link": match_link,
                    "stars_display": rating_to_stars(rating),
                    "stars_numeric": round(rating / 2, 2),
                    "won_stars_display": rating_to_stars(won_rating) if won_rating else "",
                    "won_stars_numeric": round(won_rating / 2, 2) if won_rating else 0,
                    "rating_source": "community",
                })
        except Exception as e:
            print(f"[scraper] Row parse error: {e}")
    matches.sort(key=lambda m: m["rating"], reverse=True)
    return matches


def build_matchguide_url(nr, offset=0, sortby="colMeltzer", sorttype="DESC"):
    params = [("id", "2"), ("nr", str(nr)), ("page", "10"),
              ("sortby", sortby), ("sorttype", sorttype), ("s", str(offset))]
    return BASE_URL + "/?" + urlencode(params)


def build_ratings_url(offset=0):
    params = [("id", "111"), ("view", "matches"), ("s", str(offset))]
    return BASE_URL + "/?" + urlencode(params)


def parse_matchguide_table(soup):
    matches = []
    all_tables = soup.find_all("table")
    table = soup.find("table", class_="TBase") or soup.find("table", class_="SearchResults")
    if not table and all_tables:
        table = max(all_tables, key=lambda t: len(t.find_all("tr")))
    if not table:
        return matches
    rows = table.find_all("tr")
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 5: continue
        try:
            date_text = cells[1].get_text(strip=True)
            if not date_text or date_text.lower() in ("date", "datum"): continue
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
            if won_rating <= 0: continue
            matches.append({
                "date": date_text, "match": match_text, "event": "",
                "promotion": promotion, "rating": won_rating, "won_rating": won_rating,
                "votes": 0, "link": match_link,
                "stars_display": rating_to_stars(won_rating),
                "stars_numeric": round(won_rating / 2, 2),
                "won_stars_display": rating_to_stars(won_rating),
                "won_stars_numeric": round(won_rating / 2, 2),
                "rating_source": "meltzer",
            })
        except Exception as e:
            print(f"[scraper] Matchguide row error: {e}")
    print(f"[scraper] Parsed {len(matches)} from matchguide")
    return matches


def get_worker_nr(worker_name):
    results = search_wrestlers(worker_name)
    if results:
        nr = results[0].get("nr")
        print(f"[scraper] Resolved '{worker_name}' -> nr={nr}")
        return nr
    return None


def get_matches(worker=None, year=None, promotion_id=None, min_rating=None, pages=1):
    cache_key = f"matches|{worker}|{year}|{promotion_id}|{min_rating}|{pages}"

    def fetch():
        all_matches = []

        if worker:
            nr = get_worker_nr(worker)
            if not nr:
                return []
            for page in range(pages):
                url = build_matchguide_url(nr, offset=page * 100)
                print(f"[scraper] Fetching matchguide: {url}")
                soup = fetch_soup(url, delay=0.8 if page == 0 else 1.5)
                if not soup: break
                page_matches = parse_matchguide_table(soup)
                if not page_matches: break
                all_matches.extend(page_matches)
            if min_rating is not None:
                min_10 = float(min_rating) * 2
                all_matches = [m for m in all_matches if m["rating"] >= min_10]

        elif promotion_id:
            promo_name = PROMOTION_NAMES.get(str(promotion_id), "Unknown")
            for page in range(pages):
                url = build_promo_matchguide_url(promotion_id, offset=page * 100)
                print(f"[scraper] Fetching {promo_name}: {url}")
                soup = fetch_soup(url, delay=0.8 if page == 0 else 1.5)
                if not soup: break
                page_matches = parse_promo_matchguide(soup, promo_name)
                if not page_matches: break
                all_matches.extend(page_matches)
            if min_rating is not None:
                min_10 = float(min_rating) * 2
                all_matches = [m for m in all_matches if m["rating"] >= min_10]

        elif year:
            def fetch_one(nr, name):
                try:
                    url = build_promo_matchguide_url(nr, year=year)
                    print(f"[scraper] year={year} {name}: {url}")
                    soup = fetch_soup(url, delay=0.5)
                    return parse_promo_matchguide(soup, name) if soup else []
                except Exception as e:
                    print(f"[scraper] Error {name}: {e}")
                    return []
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {executor.submit(fetch_one, nr, name): name for nr, name in PROMOTION_NAMES.items()}
                for future in as_completed(futures):
                    try: all_matches.extend(future.result())
                    except Exception as e: print(f"[scraper] Thread error: {e}")
            if min_rating is not None:
                min_10 = float(min_rating) * 2
                all_matches = [m for m in all_matches if m["rating"] >= min_10]

        else:
            for page in range(pages):
                url = build_ratings_url(offset=page * 100)
                soup = fetch_soup(url, delay=0.8 if page == 0 else 1.5)
                if not soup: break
                page_matches = parse_match_table(soup)
                if not page_matches: break
                all_matches.extend(page_matches)
            if min_rating is not None:
                min_10 = float(min_rating) * 2
                all_matches = [m for m in all_matches if m["rating"] >= min_10]

        all_matches.sort(key=lambda m: m["rating"], reverse=True)
        return all_matches

    return _cached(cache_key, fetch)


def search_wrestlers(query):
    if len(query) < 2:
        return []
    cache_key = f"wrestlers|{query.lower()}"
    def fetch():
        url = BASE_URL + "/?" + urlencode([("id", "2"), ("view", "workers"), ("search", query)])
        soup = fetch_soup(url, delay=0.5)
        if not soup: return []
        wrestlers = []
        rows = soup.find_all("tr", class_=["TRow1", "TRow2"])
        if not rows: rows = soup.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if not cells: continue
            link_tag = cells[1].find("a") if len(cells) > 1 else (cells[0].find("a") if cells else None)
            if link_tag:
                name = link_tag.get_text(strip=True)
                href = link_tag.get("href", "")
                if name:
                    wrestlers.append({"name": name, "href": href, "nr": _extract_worker_nr(href)})
        return wrestlers[:20]
    return _cached(cache_key, fetch)


def get_promotions():
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
