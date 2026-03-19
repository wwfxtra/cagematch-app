"""
app.py — Flask backend for the Cagematch app
"""

from flask import Flask, jsonify, render_template, request
import scraper

app = Flask(__name__)


# ── Frontend ─────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


# ── API ──────────────────────────────────────────────────────────────────────

@app.route("/api/wrestlers")
def api_wrestlers():
    """Search for wrestlers by name."""
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify([])
    results = scraper.search_wrestlers(query)
    return jsonify(results)


@app.route("/api/matches")
def api_matches():
    """
    Get matches with filters.
    Params:
      worker      — wrestler name (as on cagematch)
      year        — 4-digit year
      promotion   — promotion ID (numeric)
      min_rating  — minimum star rating (0.0 – 5.0 scale)
      pages       — number of pages to fetch (default 1, max 3)
    """
    worker = request.args.get("worker", "").strip() or None
    year = request.args.get("year", "").strip() or None
    promotion_id = request.args.get("promotion", "").strip() or None
    min_rating_raw = request.args.get("min_rating", "").strip()
    pages = min(int(request.args.get("pages", 1)), 3)

    # Pass star rating (0–5) directly to scraper; conversion to 0–10 happens there
    min_rating = None
    if min_rating_raw:
        try:
            min_rating = float(min_rating_raw)
        except ValueError:
            pass

    matches = scraper.get_matches(
        worker=worker,
        year=year,
        promotion_id=promotion_id,
        min_rating=min_rating,
        pages=pages,
    )

    return jsonify({
        "count": len(matches),
        "matches": matches,
    })


@app.route("/api/promotions")
def api_promotions():
    """Return the list of major promotions."""
    return jsonify(scraper.get_promotions())


@app.route("/api/years")
def api_years():
    """Return a list of years to filter by (1980 to current)."""
    from datetime import datetime
    current = datetime.now().year
    years = list(range(current, 1979, -1))
    return jsonify(years)


@app.route("/api/debug")
def api_debug():
    """Debug endpoint — shows raw structure from cagematch for a given URL."""
    worker = request.args.get("worker", "CM Punk")
    url = scraper.build_ratings_url(worker=worker)
    status, html = scraper.fetch_raw(url, delay=0)

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml") if status == 200 else None

    table_info = []
    body_snippet = ""
    title = ""

    if soup:
        title = soup.title.get_text(strip=True) if soup.title else ""
        body_snippet = str(soup.body)[:1500] if soup.body else ""
        for i, t in enumerate(soup.find_all("table")):
            rows = t.find_all("tr")
            first_row_cells = len(rows[0].find_all("td")) if rows else 0
            table_info.append({
                "index": i,
                "class": t.get("class"),
                "id": t.get("id"),
                "rows": len(rows),
                "first_row_cells": first_row_cells,
            })

    return jsonify({
        "url": url,
        "http_status": status,
        "page_title": title,
        "tables": table_info,
        "body_snippet": body_snippet,
    })


# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
