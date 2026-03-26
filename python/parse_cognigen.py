"""
Parse recovered Cognigen HTML files into structured Parquet data.

Handles both formats:
  1. Old format (cognigencorp.com): Multi-message pages with date-based filenames
  2. Pipermail format (cognigen.com/nmusers): One message per page

Usage:
    python python/parse_cognigen.py --source old
    python python/parse_cognigen.py --source pipermail
    python python/parse_cognigen.py --source all
"""

import argparse
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("parse_cognigen")

DISCLAIMER_PATTERNS = [
    re.compile(r"ICON plc made the following annotations\..*$", re.DOTALL),
    re.compile(r"This e-?mail (transmission )?may contain confidential.*$", re.DOTALL),
    re.compile(r"NOTICE: The information contained in this electronic.*$", re.DOTALL),
    re.compile(r"De inhoud van dit bericht is vertrouwelijk.*$", re.DOTALL),
    re.compile(r"Merck Serono does not accept liability.*$", re.DOTALL),
    re.compile(r"-{10,}\s*$"),
]

CATEGORY_PATTERNS = {
    "job": re.compile(
        r"hiring|position[s]?\b|opportunit|career|recruit|"
        r"job\b|talent|now hiring|director.*(role|search)|"
        r"scientist role|looking for",
        re.IGNORECASE,
    ),
    "workshop": re.compile(
        r"workshop|course\b|training|registration|PAGE \d{4}|"
        r"webinar|symposium|conference|summer school",
        re.IGNORECASE,
    ),
    "announcement": re.compile(
        r"\brelease\b|now available|version \d|new member|"
        r"PDx-Pop \d|Wings for NONMEM",
        re.IGNORECASE,
    ),
}


def strip_disclaimers(body: str) -> str:
    for pattern in DISCLAIMER_PATTERNS:
        body = pattern.sub("", body)
    return body.strip()


def classify_subject(subject: str) -> str:
    for category, pattern in CATEGORY_PATTERNS.items():
        if pattern.search(subject):
            return category
    return "technical"


def parse_date_flexible(date_string: str) -> datetime | None:
    """Try multiple date formats."""
    cleaned = re.sub(r"^\w+,\s*", "", date_string.strip())
    formats = [
        "%d %b %Y %H:%M:%S %z",
        "%d %b %Y %H:%M:%S",
        "%b %d, %Y %I:%M %p",
        "%B %d, %Y %I:%M %p",
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(cleaned, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            continue
    return None


# --- Old format parser (cognigencorp.com) ---

def parse_old_format_page(filepath: Path) -> list[dict]:
    """Parse a cognigencorp old-format page.

    These pages often contain multiple messages separated by
    horizontal rules, with From/Subject/Date headers inline.
    """
    try:
        html = filepath.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []

    soup = BeautifulSoup(html, "html.parser")

    title_el = soup.find("title")
    page_title = title_el.get_text(strip=True) if title_el else ""

    body_text = soup.get_text(separator="\n")

    messages = []

    # Split on separator patterns (horizontal rules between messages)
    message_blocks = re.split(r"_{10,}|={10,}|\*{10,}", body_text)

    if len(message_blocks) <= 1:
        # Single message page
        msg = _extract_message_from_block(body_text, page_title, filepath.name)
        if msg:
            messages.append(msg)
    else:
        for block in message_blocks:
            block = block.strip()
            if len(block) < 50:
                continue
            msg = _extract_message_from_block(block, page_title, filepath.name)
            if msg:
                messages.append(msg)

    return messages


def _extract_message_from_block(block: str, page_title: str, filename: str) -> dict | None:
    """Extract a single message dict from a text block."""
    from_match = re.search(r"From:\s*(.+?)(?:\n|$)", block)
    subject_match = re.search(r"Subject:\s*(.+?)(?:\n|$)", block)
    date_match = re.search(r"Date:\s*(.+?)(?:\n|$)", block)

    from_name = from_match.group(1).strip() if from_match else ""
    from_name = re.sub(r"\s*<[^>]+>", "", from_name).strip()
    from_name = re.sub(r"\s*\S+@\S+", "", from_name).strip()

    subject = subject_match.group(1).strip() if subject_match else page_title
    date_raw = date_match.group(1).strip() if date_match else ""
    date = parse_date_flexible(date_raw) if date_raw else None

    return {
        "source": "cognigencorp",
        "source_file": filename,
        "date": date,
        "date_raw": date_raw,
        "from_name": from_name,
        "subject": subject,
        "category": classify_subject(subject),
        "body_raw": block,
        "body_clean": strip_disclaimers(block),
    }


# --- Pipermail format parser (cognigen.com/nmusers) ---

def parse_pipermail_page(filepath: Path) -> dict | None:
    """Parse a cognigen pipermail page (one message per page)."""
    try:
        html = filepath.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None

    soup = BeautifulSoup(html, "html.parser")

    title_el = soup.find("title")
    subject = title_el.get_text(strip=True) if title_el else ""

    body_parts = [pre.get_text() for pre in soup.find_all("pre")]
    body_raw = "\n".join(body_parts)

    if not body_raw.strip():
        body_raw = soup.get_text(separator="\n")

    from_match = re.search(r"From:\s*(.+?)(?:\n|$)", body_raw)
    date_match = re.search(r"Date:\s*(.+?)(?:\n|$)", body_raw)

    from_name = from_match.group(1).strip() if from_match else ""
    from_name = re.sub(r"\s*<[^>]+>", "", from_name).strip()
    from_name = re.sub(r"\s*\S+@\S+", "", from_name).strip()

    date_raw = date_match.group(1).strip() if date_match else ""
    date = parse_date_flexible(date_raw) if date_raw else None

    # 2006-December_0015.html → month + id
    file_match = re.match(r"(\d{4}-\w+)_(\d+)\.html", filepath.name)
    cognigen_month = file_match.group(1) if file_match else ""
    cognigen_id = int(file_match.group(2)) if file_match else None

    return {
        "source": "cognigen_pipermail",
        "source_file": filepath.name,
        "cognigen_month": cognigen_month,
        "cognigen_id": cognigen_id,
        "date": date,
        "date_raw": date_raw,
        "from_name": from_name,
        "subject": subject,
        "category": classify_subject(subject),
        "body_raw": body_raw,
        "body_clean": strip_disclaimers(body_raw),
    }


def parse_all_old(input_dir: Path) -> pl.DataFrame:
    html_files = sorted(input_dir.glob("*.html"))
    if not html_files:
        raise FileNotFoundError(f"No HTML files in {input_dir}")

    log.info(f"Parsing {len(html_files)} old-format pages...")
    all_messages = []
    for filepath in html_files:
        all_messages.extend(parse_old_format_page(filepath))

    log.info(f"Extracted {len(all_messages)} messages from {len(html_files)} pages")
    return pl.DataFrame(all_messages)


def parse_all_pipermail(input_dir: Path) -> pl.DataFrame:
    html_files = sorted(input_dir.glob("*.html"))
    if not html_files:
        raise FileNotFoundError(f"No HTML files in {input_dir}")

    log.info(f"Parsing {len(html_files)} pipermail pages...")
    records = []
    failed = 0
    for filepath in html_files:
        result = parse_pipermail_page(filepath)
        if result:
            records.append(result)
        else:
            failed += 1

    log.info(f"Parsed {len(records)} messages ({failed} failed)")
    return pl.DataFrame(records)


def main():
    parser = argparse.ArgumentParser(description="Parse Cognigen archives")
    parser.add_argument(
        "--source",
        choices=["old", "pipermail", "all"],
        default="all",
    )
    args = parser.parse_args()

    sources = ["old", "pipermail"] if args.source == "all" else [args.source]

    for source in sources:
        config = {
            "old": ("data/raw_cognigencorp", "data/cognigencorp_messages.parquet"),
            "pipermail": ("data/raw_cognigen_pipermail", "data/cognigen_pipermail_messages.parquet"),
        }
        input_dir, output_path = config[source]
        input_path = Path(input_dir)
        out = Path(output_path)

        if not input_path.exists():
            log.warning(f"{input_path} not found — run wayback_recover.py first")
            continue

        if source == "old":
            df = parse_all_old(input_path)
        else:
            df = parse_all_pipermail(input_path)

        categories = df.group_by("category").len().sort("len", descending=True)
        log.info(f"Content breakdown ({source}):")
        for row in categories.iter_rows(named=True):
            log.info(f"  {row['category']:15s} {row['len']:>6d}")

        if "date" in df.columns:
            dates = df.filter(pl.col("date").is_not_null())["date"]
            if len(dates) > 0:
                log.info(f"Date range: {dates.min()} – {dates.max()}")

        out.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(out)
        size_mb = out.stat().st_size / 1024**2
        log.info(f"Wrote {out} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
