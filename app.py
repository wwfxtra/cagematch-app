"""
app.py — Flask backend for the Cagematch app
"""

from flask import Flask, jsonify, render_template, request
import scraper

app = Flask(__name__)


# ── Frontend ────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


# ── API ─────────────────────────────────────────────────────────────────────

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

    # Convert star rating (0–5) to cagematch scale (0–10)
    min_rating_cagematch = None
    if min_rating_raw:
        try:
            stars = float(min_rating_raw)
            # cagematch minrating param is 0-100 (percentage of max)
            # Actually cagematch uses 0-10 scale directly for minrating
            min_rating_cagematch = stars * 2  # 4 stars = 8 on their scale
        except ValueError:
            pass

    matches = scraper.get_matches(
        worker=worker,
        year=year,
        promotion_id=promotion_id,
        min_rating=min_rating_cagematch,
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


# ── Run ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
