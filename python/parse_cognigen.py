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

def read_html(filepath: Path) -> str:
    """Read an HTML file with charset detection.

    Checks the HTML <meta> charset declaration first, then falls back
    to trying UTF-8 and Latin-1.
    """
    raw = filepath.read_bytes()

    # Check for <meta charset="..."> or <meta content="text/html; charset=...">
    head = raw[:2048].lower()
    charset_match = (
        re.search(rb'charset=["\']?([a-z0-9_-]+)', head)
        or re.search(rb'encoding=["\']?([a-z0-9_-]+)', head)
    )
    if charset_match:
        charset = charset_match.group(1).decode("ascii")
        try:
            return raw.decode(charset)
        except (UnicodeDecodeError, LookupError):
            pass

    # Try UTF-8 strictly, fall back to Latin-1 (which never fails)
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("latin-1")


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
    """Try multiple date formats, stripping timezone noise first."""
    cleaned = date_string.strip()
    # Strip leading "Date:" prefix (doubled in some records)
    cleaned = re.sub(r"^(Date:\s*)+", "", cleaned).strip()
    # Strip parenthesized timezone names: (PST), (Eastern Daylight Time), etc.
    cleaned = re.sub(r"\s*\([A-Za-z\s]+\)\s*$", "", cleaned)
    # Collapse double spaces (mbox dates: "Apr  3" → "Apr 3")
    cleaned = re.sub(r"  +", " ", cleaned)
    # Normalize am/pm: lowercase → uppercase, add space if missing
    cleaned = re.sub(r"(\d)(am|pm|AM|PM)", r"\1 \2", cleaned)
    cleaned = re.sub(r"\bam\b", "AM", cleaned)
    cleaned = re.sub(r"\bpm\b", "PM", cleaned)
    # Strip truncated timezone offsets (3 digits): -050, +020
    cleaned = re.sub(r"\s+[+-]\d{3}\s*$", "", cleaned)
    # Strip trailing bare timezone abbreviations: GMT, EDT, MET, SAST-2, GMT0BST, etc.
    # Negative lookbehind avoids stripping AM/PM from 12-hour times.
    cleaned = re.sub(r"\s+(?!AM$|PM$)[A-Z]{2,5}[\dA-Z]*(?:[+-]\d{1,2})?\s*$", "", cleaned)
    # Strip leading day name (with or without comma): "Monday, " / "Thursday "
    cleaned = re.sub(
        r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun|Monday|Tuesday|Wednesday|"
        r"Thursday|Friday|Saturday|Sunday)\.?,?\s+",
        "", cleaned, flags=re.IGNORECASE,
    )
    # Strip dots after day numbers: "29. April" → "29 April"
    cleaned = re.sub(r"(\d)\.\s+", r"\1 ", cleaned)
    # Strip Swedish "den" prefix: "den 4 juli" → "4 juli"
    cleaned = re.sub(r"^den\s+", "", cleaned, flags=re.IGNORECASE)
    # Normalize Swedish/non-English month names to English
    _swedish_months = {
        "januari": "January", "februari": "February", "mars": "March",
        "maj": "May", "juni": "June", "juli": "July",
        "augusti": "August", "oktober": "October",
    }
    for sv, en in _swedish_months.items():
        cleaned = re.sub(r"\b" + sv + r"\b", en, cleaned, flags=re.IGNORECASE)
    # Title-case month names so strptime matches: "june" → "June"
    cleaned = re.sub(
        r"\b(january|february|march|april|may|june|july|august|"
        r"september|october|november|december)\b",
        lambda m: m.group(0).capitalize(),
        cleaned,
        flags=re.IGNORECASE,
    )
    # Normalize "04/17/95, Time:15:51:31" → "04/17/95 15:51:31"
    cleaned = re.sub(r",\s*Time:", " ", cleaned)
    # Strip trailing junk after date (e.g., "14:56:15 -0500Mark,")
    # Only match after HH:MM:SS (with seconds) to avoid clobbering AM/PM.
    cleaned = re.sub(
        r"(\d{2}:\d{2}:\d{2}(?:\s+[+-]\d{4})?)\s*[A-Za-z].*$",
        r"\1", cleaned,
    )
    # Strip trailing colons: "09:44:25 -0500:"
    cleaned = re.sub(r":\s*$", "", cleaned)
    # Strip subject-line junk between month name and day number:
    # "MarchRe: [NMusers] Ln(DV 13," → "March 13,"
    cleaned = re.sub(
        r"(January|February|March|April|May|June|July|August|"
        r"September|October|November|December)"
        r"[A-Za-z:&\[\]\s\(\)]+?(\d{1,2}[,\s])",
        r"\1 \2", cleaned, flags=re.IGNORECASE,
    )
    # Strip AM/PM when hour is 13-23: "14:45 PM" → "14:45"
    cleaned = re.sub(r"(\b(?:1[3-9]|2[0-3]):\d{2})\s+[AP]M", r"\1", cleaned)
    # Remove duplicate year: "2002 2002" → "2002"
    cleaned = re.sub(r"(\d{4})\s+\1", r"\1", cleaned)
    formats = [
        # ISO 8601 (from hypermail <meta name="created">)
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        # RFC 2822 variants
        "%d %b %Y %H:%M:%S %z",
        "%d %b %Y %H:%M:%S",
        "%d %b %Y %H:%M %p",
        "%d %b %Y %H:%M",
        "%d %b %Y",
        # US formats: M/D/YYYY
        "%m/%d/%Y %I:%M %p",
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y",
        # 2-digit year
        "%m/%d/%y %H:%M:%S",
        "%m/%d/%y %H:%M",
        "%m/%d/%y %I:%M %p",
        # Long month name variants
        "%B %d, %Y %I:%M %p",
        "%B %d, %Y %H:%M",
        "%B %d %Y %H:%M",
        "%B %d, %Y %H:%M:%S %p",
        "%B %d %Y",
        # Short month name variants
        "%b %d, %Y %I:%M %p",
        # DD-Mon-YYYY
        "%d-%b-%Y %H:%M:%S",
        "%d-%b-%Y %H:%M",
        # Day Month Year variants
        "%d %B %Y %H:%M",
        "%d %B %Y %I:%M %p",
        "%d %B %Y",
        # Mbox timestamp: "May 20 06:01:45 1997"
        "%b %d %H:%M:%S %Y",
        # 2-digit year short month: "16 Oct 99"
        "%d %b %y %H:%M:%S",
        "%d %b %y",
        # US short: 10/16/95
        "%m/%d/%y",
        # Month Day, Year (no time)
        "%B %d, %Y",
        # Short month: "Oct 16, 2003 10:43 AM"
        "%b %d, %Y %H:%M %p",
        "%b %d, %Y %H:%M",
        "%b %d, %Y",
        # Time-first US: "11:38 AM 9/17/02"
        "%I:%M %p %m/%d/%y",
        # Day-only: "11 Aug 2005"
        "%d %b %Y",
    ]
    # Try each format with the cleaned string, then again with trailing
    # 4-digit UTC offset stripped (for dates where offset isn't in the format).
    variants = [cleaned]
    stripped = re.sub(r"\s+[+-]\d{4}\s*$", "", cleaned)
    if stripped != cleaned:
        variants.append(stripped)
    for candidate in variants:
        for fmt in formats:
            try:
                dt = datetime.strptime(candidate, fmt)
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
        html = read_html(filepath)
    except OSError:
        return []

    soup = BeautifulSoup(html, "html.parser")

    title_el = soup.find("title")
    page_title = title_el.get_text(strip=True) if title_el else ""

    body_text = soup.get_text(separator="\n")

    messages = []

    # Split on separator patterns (horizontal rules between messages)
    message_blocks = re.split(r"_{10,}|={10,}|\*{5,}", body_text)

    if len(message_blocks) <= 1:
        # Single message page
        msg = _extract_message_from_block(body_text, page_title, filepath.name)
        if msg:
            messages.append(msg)
    else:
        prev_block = ""
        for block in message_blocks:
            block = block.strip()
            if len(block) < 50:
                prev_block = block
                continue
            msg = _extract_message_from_block(
                block, page_title, filepath.name, prev_block=prev_block,
            )
            if msg:
                messages.append(msg)
            prev_block = block

    return messages


def _extract_from_name(from_raw: str) -> str:
    """Extract display name from a From: field, falling back to email."""
    # Strip [mailto:...] notation
    name = re.sub(r"\s*\[mailto:[^\]]+\]", "", from_raw).strip()
    # Strip <email> bracket notation
    name = re.sub(r"\s*<[^>]+>", "", name).strip()
    # Strip bare email addresses from display name
    name = re.sub(r"\s*\S+@\S+", "", name).strip()
    # Strip leading "Subject:" leaked from shifted headers
    name = re.sub(r"^Subject:\s*", "", name).strip()
    # Strip trailing mbox timestamp: "Thu Oct 19 17:05:14 1995"
    name = re.sub(
        r"\s+\w{3}\s+\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\d{4}\s*$",
        "", name,
    ).strip()
    # Strip surrounding quotes
    name = name.strip('"').strip()
    # Fallback: if no display name remains, use the email address itself
    if not name and from_raw:
        email_match = re.search(r"[\w.\-+]+@[\w.\-]+", from_raw)
        name = email_match.group(0) if email_match else from_raw
    return name


def _extract_message_from_block(
    block: str, page_title: str, filename: str, prev_block: str = "",
) -> dict | None:
    """Extract a single message dict from a text block."""
    # Match From: with value on same line, or on the next line if current line is empty
    from_match = re.search(r"From:\s*(\S.+?)(?:\n|$)", block)
    if not from_match:
        # Multiline: "From:\n<name on next line>"
        from_match = re.search(r"From:\s*\n(.+?)(?:\n|$)", block)
    subject_match = re.search(r"Subject:\s*(.+?)(?:\n|$)", block)
    date_match = re.search(r"Date:\s*(.+?)(?:\n|$)", block)
    # Fallback: Outlook "Sent:" header
    if not date_match:
        date_match = re.search(r"Sent:\s*(.+?)(?:\n|$)", block)
    # Fallback: Date: may be at the tail of the previous block (split landed
    # between Date: and From:). Use the LAST match (closest to the separator).
    if not date_match and prev_block:
        all_dates = list(re.finditer(r"Date:\s*(.+?)(?:\n|$)", prev_block))
        if all_dates:
            date_match = all_dates[-1]

    # Fallback: Unix mbox "From user@host Day Mon DD HH:MM:SS YYYY" (no colon)
    mbox_match = re.search(
        r"^From\s+(\S+@\S+)\s+\w{3}\s+(\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\d{4})",
        block, re.MULTILINE,
    )
    if not from_match and mbox_match:
        from_match = mbox_match
    if not date_match and mbox_match:
        # Synthesize a date_match-like object from the mbox timestamp
        date_match = re.search(
            r"(\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\d{4})",
            mbox_match.group(2),
        )
    # Fallback: extract timestamp embedded in From: line
    # e.g. "From: SAM LIAO <email> Thu Oct 19 17:05:14 1995"
    if not date_match and from_match:
        embedded_ts = re.search(
            r"\w{3}\s+(\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\d{4})",
            from_match.group(1),
        )
        if embedded_ts:
            date_match = embedded_ts

    # Fallback: "From Name" without colon (e.g., "From Nick Holford")
    if not from_match:
        from_match = re.search(r"^From\s+([A-Z][a-z]+ [A-Z][a-z]+.*)$", block, re.MULTILINE)

    # Require a From match — blocks with only Date are almost always
    # quoted content, search results, or code fragments, not real messages.
    if not from_match:
        return None

    from_name = _extract_from_name(from_match.group(1).strip() if from_match else "")

    subject = subject_match.group(1).strip() if subject_match else page_title
    date_raw = date_match.group(1).strip() if date_match else ""

    # Detect swapped Subject/Date fields: if date_raw looks like a subject
    # (starts with Re:/FW:) and subject looks like a date, swap them.
    if date_raw and subject:
        date_looks_like_subject = bool(re.match(r"(?:Re|FW|Fwd):", date_raw, re.IGNORECASE))
        subject_looks_like_date = bool(re.match(r"\w{3},?\s+\d", subject))
        if date_looks_like_subject and subject_looks_like_date:
            date_raw, subject = subject, date_raw

    date = parse_date_flexible(date_raw) if date_raw else None

    # Fallback: if date still None, look for a date in other header fields
    # or in a second Subject: line (handles shifted headers).
    if date is None:
        # Check if subject looks like a date
        candidate = parse_date_flexible(subject) if subject else None
        if candidate:
            date = candidate
            date_raw = subject
        else:
            # Look for a second Subject: line which may contain the date
            all_subjects = re.findall(r"Subject:\s*(.+?)(?:\n|$)", block)
            for s in all_subjects[1:]:  # skip first (already captured)
                candidate = parse_date_flexible(s.strip())
                if candidate:
                    date = candidate
                    date_raw = s.strip()
                    break

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

def _extract_hypermail_body(soup: BeautifulSoup) -> str:
    """Extract message body from hypermail-generated pages.

    Looks for the <div class="mail"> between body="start" and body="end"
    comments, falling back to <pre> tags or full page text.
    """
    # Try the hypermail mail div first
    mail_div = soup.find("div", class_="mail")
    if mail_div:
        # Remove the header <address> block and navigation elements
        for tag in mail_div.find_all(["address", "map", "dfn"]):
            tag.decompose()
        # Remove the "Received on ..." span
        received = mail_div.find("span", id="received")
        if received:
            received.decompose()
        return mail_div.get_text(separator="\n").strip()

    # Fallback: <pre> tags
    pre_parts = [pre.get_text() for pre in soup.find_all("pre")]
    if pre_parts:
        return "\n".join(pre_parts)

    # Last resort: full page text
    return soup.get_text(separator="\n")


def parse_pipermail_page(filepath: Path) -> dict | None:
    """Parse a cognigen hypermail page (one message per page).

    Uses structured metadata (meta tags, HTML comments, span elements)
    rather than regex on body text.
    """
    try:
        html = read_html(filepath)
    except OSError:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # --- Subject: <title> tag (already clean) ---
    title_el = soup.find("title")
    subject = title_el.get_text(strip=True) if title_el else ""

    # --- Author: <meta name="author"> or <!-- name="..." --> or <span id="from"> ---
    from_name = ""
    meta_author = soup.find("meta", attrs={"name": "author"})
    if meta_author and meta_author.get("content", "").strip():
        from_name = meta_author["content"].strip()
    if not from_name:
        # Try HTML comment: <!-- name="Nick Holford" -->
        name_comment = re.search(r'<!--\s*name="([^"]+)"', html)
        if name_comment:
            from_name = name_comment.group(1).strip()
    if not from_name:
        # Try <span id="from"> text
        from_span = soup.find("span", id="from")
        if from_span:
            from_text = from_span.get_text()
            from_text = re.sub(r"^From:\s*", "", from_text).strip()
            from_name = _extract_from_name(from_text)

    # --- Date: <meta name="created"> or <!-- sent="..." --> or <span id="date"> ---
    date = None
    date_raw = ""
    meta_created = soup.find("meta", attrs={"name": "created"})
    if meta_created and meta_created.get("content", "").strip():
        date_raw = meta_created["content"].strip()
        date = parse_date_flexible(date_raw)
    if date is None:
        # Try HTML comment: <!-- sent="Fri, 01 Dec 2006 22:59:17 +1300" -->
        sent_comment = re.search(r'<!--\s*sent="([^"]+)"', html)
        if sent_comment:
            date_raw = sent_comment.group(1).strip()
            date = parse_date_flexible(date_raw)
    if date is None:
        # Try <span id="date"> text
        date_span = soup.find("span", id="date")
        if date_span:
            date_text = date_span.get_text()
            date_text = re.sub(r"^Date:\s*", "", date_text).strip()
            date_raw = date_text
            date = parse_date_flexible(date_raw)

    # --- Body ---
    body_raw = _extract_hypermail_body(soup)
    body_clean = strip_disclaimers(body_raw)

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
        "body_clean": body_clean,
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
