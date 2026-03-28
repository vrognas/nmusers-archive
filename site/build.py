"""
Build the NMusers Archive static site from Parquet data.

Reads data/messages_all.parquet and generates a complete static site
using Jinja2 templates. Run pagefind after this to add search indexing.

Usage:
    uv run python site/build.py
    uv run python site/build.py --output site/output
    uv run python site/build.py --serve   # Build + local dev server
"""

import argparse
import calendar
import html
import logging
import re
import shutil
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

import polars as pl
from jinja2 import Environment, FileSystemLoader
from markupsafe import Markup
from slugify import slugify

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("build")

SITE_DIR = Path(__file__).parent
TEMPLATE_DIR = SITE_DIR / "templates"
STATIC_DIR = SITE_DIR / "static"
DATA_PATH = Path("data/messages_all.parquet")
MONTH_NAMES = {i: calendar.month_name[i] for i in range(1, 13)}


def commafy(value: int) -> str:
    return f"{value:,}"


def clean_body(text: str) -> Markup:
    """Clean HTML remnants and auto-link URLs in message bodies."""
    # Convert <br>, <br/>, <br /> to newlines
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    # Convert <p> / </p> to double newlines
    text = re.sub(r"</?p\s*/?>", "\n\n", text, flags=re.IGNORECASE)
    # Decode HTML entities
    text = html.unescape(text)
    # Clean email-client URL patterns: "display text<http://url>" → "http://url"
    text = re.sub(
        r"[^\s<]*<(https?://[^>]+)>",
        r"\1",
        text,
    )
    # Clean bare angle-bracket URLs: <http://url> → http://url
    text = re.sub(r"<(https?://[^>]+)>", r"\1", text)
    # Collapse excessive blank lines (3+ → 2)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = text.strip()

    # HTML-escape the content (so user content is safe)
    text = html.escape(text)

    # Auto-link URLs (after escaping, so the <a> tags we insert are preserved)
    text = re.sub(
        r"(https?://[^\s&lt;)&\]]+)",
        r'<a href="\1">\1</a>',
        text,
    )
    # Auto-link www. URLs without protocol
    text = re.sub(
        r"(?<!/)(www\.[^\s&lt;)&\]]+)",
        r'<a href="http://\1">\1</a>',
        text,
    )
    return Markup(text)


def normalize_subject(subject: str) -> str:
    """Strip Re:/FW: prefixes and [NMusers] tag for thread grouping."""
    cleaned = subject.strip().replace("[NMusers]", "").strip()
    while True:
        new = cleaned
        for prefix in ["Re:", "RE:", "Fwd:", "FW:", "Fw:", "re:"]:
            if new.startswith(prefix):
                new = new[len(prefix) :].strip()
        if new == cleaned:
            break
        cleaned = new
    return cleaned.lower().strip()


# Improved category patterns (applied at build time to override source data)
_CATEGORY_PATTERNS = {
    "admin": re.compile(
        r"\bunsubscri|\bsubscri|remove me|sign.?off\b|"
        r"\bvirus\b|\bspam\b|do not open|phishing",
        re.IGNORECASE,
    ),
    "job": re.compile(
        r"hiring|position[s]?\b|opportunit|career|recruit|"
        r"job\b|talent|now hiring|"
        r"director\b.*(?:role|search|quantitative|pharmacol|pharmacomet)|"
        r"scientist\b|researcher\b|looking for|fellowship|postdoc|"
        r"post.?doc|vacancy|vacanc|openings?\b|"
        r"associate director|senior.*(?:scientist|manager)|"
        r"pharmacometrician|apply\b.*(?:role|position)|"
        r"we are seeking|join our|join the",
        re.IGNORECASE,
    ),
    "workshop": re.compile(
        r"workshop|course\b|training|registration|PAGE\s?\d{4}|"
        r"PAGANZ|webinar|symposium|conference|summer school|"
        r"\bmeeting\b.*\d{4}|\d{4}.*\bmeeting\b",
        re.IGNORECASE,
    ),
    "announcement": re.compile(
        r"\brelease\b|now available|version \d|new member|"
        r"Wings for NONMEM",
        re.IGNORECASE,
    ),
}


def classify_subject(subject: str) -> str:
    """Classify a message by its subject line."""
    for category, pattern in _CATEGORY_PATTERNS.items():
        if pattern.search(subject):
            return category
    return "technical"


def clean_from_name(name: str) -> str:
    """Clean up email-as-name entries to something more readable."""
    if not name:
        return "Unknown"
    # Handle "[email protected]" placeholder from mail-archive
    if "email\xa0protected" in name or "[email" in name:
        return "Unknown"
    # If it looks like an email, extract the local part and titlecase it
    if "@" in name and " " not in name:
        local = name.split("@")[0]
        # Convert dot/underscore-separated names: john.doe → John Doe
        parts = re.split(r"[._-]", local)
        return " ".join(p.capitalize() for p in parts if p)
    return name


def load_data() -> pl.DataFrame:
    """Load and enrich the message dataset."""
    log.info(f"Loading {DATA_PATH}...")
    df = pl.read_parquet(DATA_PATH)

    # Reclassify with improved patterns
    df = df.with_columns(
        pl.col("subject")
        .map_elements(classify_subject, return_dtype=pl.Utf8)
        .alias("category"),
        pl.col("from_name")
        .map_elements(clean_from_name, return_dtype=pl.Utf8)
        .alias("from_name"),
    )

    # Strip [NMusers] tag from display subject
    df = df.with_columns(
        pl.col("subject")
        .str.replace(r"\[NMusers\]\s*", "")
        .alias("subject"),
    )

    df = df.with_columns(
        pl.col("date").dt.year().alias("year"),
        pl.col("date").dt.month().alias("month"),
        pl.col("from_name")
        .map_elements(lambda x: slugify(x) if x else "unknown", return_dtype=pl.Utf8)
        .alias("author_slug"),
        pl.col("subject")
        .map_elements(normalize_subject, return_dtype=pl.Utf8)
        .alias("thread_key"),
    )

    # Sort by date for consistent ordering
    df = df.sort("date")

    # Assign sequential IDs within each year-month group
    df = df.with_columns(
        pl.cum_count("date").over("year", "month").alias("msg_seq"),
    )

    log.info(f"Loaded {len(df)} messages")
    return df


def msg_url(row: dict) -> str:
    """Generate URL path for a message."""
    if row["year"] is None or row["month"] is None:
        return f"/undated/{row['msg_seq']}.html"
    return f"/{row['year']}/{row['month']:02d}/{row['msg_seq']}.html"


def msg_date_short(row: dict) -> str:
    """Format date as 'Mar 15'."""
    if row["date"] is None:
        return "?"
    return row["date"].strftime("%b %d")


def msg_date_long(row: dict) -> str:
    """Format date as 'March 15, 2006'."""
    if row["date"] is None:
        return "Unknown date"
    return row["date"].strftime("%B %d, %Y")


def build_site(output_dir: Path):
    """Generate the full static site."""
    # Clean output
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)

    # Set up Jinja2
    env = Environment(
        loader=FileSystemLoader(TEMPLATE_DIR),
        autoescape=True,
    )
    env.filters["commafy"] = commafy

    df = load_data()
    rows = df.to_dicts()

    # Enrich rows with computed fields
    for row in rows:
        row["url"] = msg_url(row)
        row["date_short"] = msg_date_short(row)
        row["date_long"] = msg_date_long(row)

    # Copy static assets
    log.info("Copying static assets...")
    static_out = output_dir / "static"
    shutil.copytree(STATIC_DIR, static_out)

    # --- Home page ---
    log.info("Generating home page...")
    years_data = (
        df.group_by("year")
        .len()
        .sort("year", descending=True)
        .rename({"len": "count"})
        .to_dicts()
    )
    recent = [r for r in reversed(rows) if r["date"] is not None][:30]

    source_counts = df.group_by("source").len().to_dicts()
    source_map = {r["source"]: r["len"] for r in source_counts}

    home_html = env.get_template("home.html").render(
        total_messages=len(rows),
        total_authors=df["from_name"].n_unique(),
        total_technical=df.filter(pl.col("category") == "technical").height,
        year_min=df["year"].min(),
        year_max=df["year"].max(),
        years=years_data,
        recent_messages=recent,
    )
    (output_dir / "index.html").write_text(home_html, encoding="utf-8")

    # --- Search page ---
    log.info("Generating search page...")
    search_html = env.get_template("search.html").render(
        total_messages=len(rows),
    )
    search_dir = output_dir / "search"
    search_dir.mkdir()
    (search_dir / "index.html").write_text(search_html, encoding="utf-8")

    # --- About page ---
    log.info("Generating about page...")
    about_html = env.get_template("about.html").render(
        total_messages=len(rows),
        count_mail_archive=source_map.get("mail_archive", 0),
        count_cognigencorp=source_map.get("cognigencorp", 0),
        count_pipermail=source_map.get("pipermail", 0),
    )
    about_dir = output_dir / "about"
    about_dir.mkdir()
    (about_dir / "index.html").write_text(about_html, encoding="utf-8")

    # --- Year pages ---
    log.info("Generating year pages...")
    years = df["year"].unique().sort().to_list()
    for year in years:
        if year is None:
            continue
        year_rows = [r for r in rows if r["year"] == year]
        months_in_year = {}
        for r in year_rows:
            m = r["month"]
            if m not in months_in_year:
                months_in_year[m] = 0
            months_in_year[m] += 1

        month_data = sorted(
            [
                {
                    "month_num": f"{m:02d}",
                    "month_name": MONTH_NAMES[m],
                    "count": count,
                }
                for m, count in months_in_year.items()
            ],
            key=lambda x: x["month_num"],
        )

        year_dir = output_dir / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        year_html = env.get_template("year.html").render(
            year=year,
            total_messages=len(year_rows),
            months=month_data,
        )
        (year_dir / "index.html").write_text(year_html, encoding="utf-8")

    # --- Month pages ---
    log.info("Generating month pages...")
    month_groups = {}
    for r in rows:
        key = (r["year"], r["month"])
        if key not in month_groups:
            month_groups[key] = []
        month_groups[key].append(r)

    for (year, month), msgs in month_groups.items():
        if year is None or month is None:
            continue
        month_dir = output_dir / str(year) / f"{month:02d}"
        month_dir.mkdir(parents=True, exist_ok=True)

        month_html = env.get_template("month.html").render(
            year=year,
            month_name=MONTH_NAMES[month],
            messages=msgs,
        )
        (month_dir / "index.html").write_text(month_html, encoding="utf-8")

    # --- Message pages ---
    log.info("Generating message pages...")
    # Build thread groups for "related messages" feature
    thread_groups: dict[str, list[dict]] = {}
    for r in rows:
        tk = r["thread_key"]
        if tk:
            if tk not in thread_groups:
                thread_groups[tk] = []
            thread_groups[tk].append(r)

    # Build prev/next index (messages sorted by date)
    dated_rows = [r for r in rows if r["date"] is not None]
    msg_template = env.get_template("message.html")
    for i, row in enumerate(dated_rows):
        if row["year"] is None or row["month"] is None:
            continue

        # Thread context
        tk = row["thread_key"]
        thread_msgs = thread_groups.get(tk, [])
        thread_context = None
        if len(thread_msgs) > 1:
            thread_context = [
                {
                    "date_short": t["date_short"],
                    "from_name": t["from_name"],
                    "subject": t["subject"],
                    "url": t["url"],
                    "is_current": t["msg_seq"] == row["msg_seq"]
                    and t["year"] == row["year"]
                    and t["month"] == row["month"],
                }
                for t in thread_msgs
            ]

        prev_url = dated_rows[i - 1]["url"] if i > 0 else None
        next_url = dated_rows[i + 1]["url"] if i < len(dated_rows) - 1 else None

        msg_html = msg_template.render(
            subject=row["subject"],
            from_name=row["from_name"],
            author_slug=row["author_slug"],
            date_long=row["date_long"],
            date_sort=row["date"].strftime("%Y-%m-%d") if row["date"] else "",
            category=row["category"],
            year=row["year"],
            month_num=f"{row['month']:02d}",
            month_name=MONTH_NAMES[row["month"]],
            body=clean_body(row["body_clean"]),
            thread_messages=thread_context,
            prev_url=prev_url,
            next_url=next_url,
        )

        msg_path = (
            output_dir
            / str(row["year"])
            / f"{row['month']:02d}"
            / f"{row['msg_seq']}.html"
        )
        msg_path.parent.mkdir(parents=True, exist_ok=True)
        msg_path.write_text(msg_html, encoding="utf-8")

    log.info(f"Generated {len(dated_rows)} message pages")

    # --- Author pages ---
    log.info("Generating author pages...")
    author_groups: dict[str, list[dict]] = {}
    for r in rows:
        slug = r["author_slug"]
        if slug not in author_groups:
            author_groups[slug] = []
        author_groups[slug].append(r)

    authors_dir = output_dir / "authors"
    authors_dir.mkdir()

    # Author index
    author_list = sorted(
        [
            {"name": msgs[0]["from_name"], "slug": slug, "count": len(msgs)}
            for slug, msgs in author_groups.items()
        ],
        key=lambda a: a["name"].lower(),
    )
    author_index_html = env.get_template("author_index.html").render(
        authors=author_list,
    )
    (authors_dir / "index.html").write_text(author_index_html, encoding="utf-8")

    # Individual author pages
    author_template = env.get_template("author.html")
    for slug, msgs in author_groups.items():
        author_dir = authors_dir / slug
        author_dir.mkdir(exist_ok=True)

        dated_msgs = [m for m in msgs if m["date"] is not None]
        years = [m["year"] for m in dated_msgs if m["year"] is not None]

        author_html = author_template.render(
            author_name=msgs[0]["from_name"],
            messages=msgs,
            year_min=min(years) if years else "?",
            year_max=max(years) if years else "?",
        )
        (author_dir / "index.html").write_text(author_html, encoding="utf-8")

    log.info(f"Generated {len(author_groups)} author pages")

    # --- Category pages ---
    log.info("Generating category pages...")
    category_template = env.get_template("category.html")
    categories = df["category"].unique().to_list()
    cat_dir = output_dir / "category"
    cat_dir.mkdir()

    for cat in categories:
        cat_msgs = [r for r in rows if r["category"] == cat]
        cat_page_dir = cat_dir / cat
        cat_page_dir.mkdir()
        cat_html = category_template.render(
            category=cat,
            messages=cat_msgs,
        )
        (cat_page_dir / "index.html").write_text(cat_html, encoding="utf-8")

    # --- Search index data ---
    log.info("Exporting search data...")
    import json

    search_docs = []
    for r in rows:
        if r["date"] is None:
            continue
        search_docs.append(
            {
                "subject": r["subject"],
                "from_name": r["from_name"],
                "body": r["body_clean"][:500],  # Snippet for search + display
                "category": r["category"],
                "year": r["year"],
                "date": r["date"].strftime("%Y-%m-%d"),
                "url": r["url"],
            }
        )
    search_data_path = output_dir / "search-data.json"
    search_data_path.write_text(json.dumps(search_docs), encoding="utf-8")
    size_mb = search_data_path.stat().st_size / 1024**2
    log.info(f"Exported {len(search_docs)} docs to {search_data_path} ({size_mb:.1f} MB)")

    # Count output files
    file_count = sum(1 for _ in output_dir.rglob("*.html"))
    log.info(f"Site built: {file_count} HTML files in {output_dir}")


def serve(output_dir: Path, port: int = 8000):
    """Start a local dev server."""
    import os

    os.chdir(output_dir)
    server = HTTPServer(("localhost", port), SimpleHTTPRequestHandler)
    log.info(f"Serving {output_dir} at http://localhost:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


def main():
    parser = argparse.ArgumentParser(description="Build NMusers Archive static site")
    parser.add_argument(
        "--output",
        type=str,
        default="site/output",
        help="Output directory for generated site",
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Start a local dev server (no rebuild)",
    )
    parser.add_argument(
        "--no-build",
        action="store_true",
        help="Skip build, just serve (use with --serve)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port for local dev server",
    )
    args = parser.parse_args()

    output_dir = Path(args.output)
    if not args.no_build:
        build_site(output_dir)

    if args.serve:
        serve(output_dir, args.port)


if __name__ == "__main__":
    main()
