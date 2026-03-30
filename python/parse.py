"""
Parse raw HTML message files into a structured Parquet dataset.

Extracts metadata (subject, author, date, message-id, threading)
and message body. Strips corporate disclaimers, classifies content
type, and hashes email addresses for privacy.

Usage:
    python python/parse.py                              # Parse all
    python python/parse.py --input data/raw --output data/messages.parquet
"""

import argparse
import hashlib
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

from parse_cognigen import read_html

import polars as pl
from bs4 import BeautifulSoup, Comment, NavigableString, Tag

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("parse")

# Patterns for stripping corporate disclaimers
DISCLAIMER_PATTERNS = [
    re.compile(r"ICON plc made the following annotations\..*$", re.DOTALL),
    re.compile(r"This e-?mail (transmission )?may contain confidential.*$", re.DOTALL),
    re.compile(r"NOTICE: The information contained in this electronic.*$", re.DOTALL),
    re.compile(r"De inhoud van dit bericht is vertrouwelijk.*$", re.DOTALL),
    re.compile(r"Merck Serono does not accept liability.*$", re.DOTALL),
    re.compile(r"-{10,}\s*$"),
]

# Subject-based content classification
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


def hash_email(email: str) -> str | None:
    """SHA-256 hash of a lowercased email address."""
    if not email:
        return None
    return hashlib.sha256(email.lower().strip().encode()).hexdigest()


def strip_disclaimers(body: str) -> str:
    """Remove common corporate email footers."""
    for pattern in DISCLAIMER_PATTERNS:
        body = pattern.sub("", body)
    return body.strip()


def classify_subject(subject: str) -> str:
    """Classify a message as technical, job, workshop, or announcement."""
    for category, pattern in CATEGORY_PATTERNS.items():
        if pattern.search(subject):
            return category
    return "technical"


def parse_date(date_string: str) -> datetime | None:
    """Parse mail-archive.com date format to UTC datetime."""
    # Remove day-of-week prefix: "Thu, 19 Mar 2026 10:20:38 -0700"
    cleaned = re.sub(r"^\w+,\s*", "", date_string.strip())
    try:
        return datetime.strptime(cleaned, "%d %b %Y %H:%M:%S %z").astimezone(timezone.utc)
    except ValueError:
        return None


def _normalize_mail_archive_text(text: str) -> str:
    """Normalize extracted mail-archive text blocks."""
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\xa0", " ")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _join_mail_archive_flow(lines: list[str]) -> str:
    """Join flowed mail-archive <tt> lines back into a paragraph."""
    parts = []
    for line in lines:
        normalized = re.sub(r"\s+", " ", line.replace("\xa0", " ").strip())
        if normalized:
            parts.append(normalized)
    return " ".join(parts)


def _quote_mail_archive_block(text: str) -> str:
    """Represent blockquoted HTML as plain-text quote lines."""
    lines = text.split("\n")
    return "\n".join("> " + line if line else ">" for line in lines).strip()


def _extract_mail_archive_fragment(node: Tag) -> str:
    """Extract message text while preserving paragraphs and preformatted blocks."""
    parts: list[str] = []
    flowed_lines: list[str] = []

    def flush_flowed() -> None:
        if not flowed_lines:
            return
        paragraph = _join_mail_archive_flow(flowed_lines)
        flowed_lines.clear()
        if paragraph:
            parts.append(paragraph)

    for child in node.children:
        if isinstance(child, Comment):
            continue
        if isinstance(child, NavigableString):
            text = str(child)
            if text.strip():
                flowed_lines.append(text)
            continue
        if not isinstance(child, Tag):
            continue

        if child.name == "tt":
            flowed_lines.append(child.get_text(" ", strip=False))
            continue

        flush_flowed()

        if child.name == "pre":
            block = _normalize_mail_archive_text(child.get_text())
            if block:
                parts.append(block)
            continue

        if child.name == "blockquote":
            block = _extract_mail_archive_fragment(child)
            if block:
                parts.append(_quote_mail_archive_block(block))
            continue

        block = _normalize_mail_archive_text(child.get_text(" ", strip=False))
        if block:
            parts.append(block)

    flush_flowed()
    return "\n\n".join(part for part in parts if part)


def _extract_thread_parent(soup: BeautifulSoup) -> int | None:
    """Extract the parent message number from the thread tree.

    The tSliceList contains a nested <ul>/<li> tree showing the thread.
    The current message is <li class="tSliceCur">. Its parent <li>
    (one nesting level up) contains the message this one replies to.

    Structure example:
        <li class="icons-email">          ← parent message
          <span class="subject"><a href="msg08350.html">...</a></span>
          <li><ul>
            <li class="tSliceCur">        ← current message (no <a>)
            ...
          </ul></li>
        </li>
    """
    current = soup.select_one("li.tSliceCur")
    if current is None:
        return None

    # Walk up: tSliceCur <li> → parent <ul> → wrapper <li> → parent <ul> → parent <li>
    # The nesting is: parent <li> > <ul> > wrapper <li> > <ul> > tSliceCur <li>
    # But the wrapper <li> may not have a link (it just groups children).
    # We need to find the nearest ancestor <li> that has a span.subject > a.
    # Walk up from tSliceCur to find the parent message.
    # The thread tree nests replies inside wrapper <li> elements.
    # The parent message is either:
    #   a) an ancestor <li> with a direct span.subject > a, or
    #   b) the previous sibling <li> of a wrapper ancestor
    node = current
    while node:
        parent_ul = node.parent
        if parent_ul is None or parent_ul.name != "ul":
            break
        parent_li = parent_ul.parent
        if parent_li is None or parent_li.name != "li":
            break
        # Check if this <li> directly contains a subject link
        span = parent_li.find("span", class_="subject", recursive=False)
        link = span.find("a") if span else None
        if link:
            href = link.get("href", "")
            match = re.search(r"msg(\d+)", href)
            return int(match.group(1)) if match else None
        # No link — check the previous sibling <li> (parent message is often a sibling)
        prev = parent_li.find_previous_sibling("li")
        if prev:
            span = prev.find("span", class_="subject", recursive=False)
            link = span.find("a") if span else None
            if link:
                href = link.get("href", "")
                match = re.search(r"msg(\d+)", href)
                return int(match.group(1)) if match else None
        # Keep walking up
        node = parent_li

    return None


def parse_message(filepath: Path) -> dict | None:
    """Parse a single HTML file into a flat dict."""
    try:
        html = read_html(filepath)
    except OSError as exc:
        log.warning(f"Cannot read {filepath.name}: {exc}")
        return None

    soup = BeautifulSoup(html, "html.parser")

    # Message number from filename
    message_number = int(filepath.stem.replace("msg", ""))

    # Subject
    subject_el = soup.select_one("h1 span.subject span[itemprop='name']")
    subject = subject_el.get_text(strip=True) if subject_el else ""

    # Author name
    author_el = soup.select_one(".msgHead span.sender span[itemprop='name']")
    from_name = author_el.get_text(strip=True) if author_el else ""

    # Date
    date_el = soup.select_one(".msgHead span.date a")
    date_raw = date_el.get_text(strip=True) if date_el else ""
    date = parse_date(date_raw) if date_raw else None

    # Body: mail-archive mixes prose across many <tt> tags with <pre> blocks
    # for explicitly preformatted text. Reconstruct flowed prose paragraphs
    # instead of turning every HTML node boundary into a hard line break.
    msg_body = soup.select_one("div.msgBody")
    body_raw = _extract_mail_archive_fragment(msg_body) if msg_body else ""
    # Strip the "X-Body-of-Message" marker that mail-archive injects
    body_raw = body_raw.replace("X-Body-of-Message", "").strip()
    body_clean = strip_disclaimers(body_raw)

    # Message-ID from hidden form field
    msgid_el = soup.select_one("input[name='msgid']")
    message_id = msgid_el["value"] if msgid_el else None

    # Threading: extract parent from the tSliceList tree.
    # The current message is marked with li.tSliceCur. Its parent in the
    # nested <ul>/<li> structure is the message it replies to.
    # NOTE: link rel="prev" is chronological navigation, NOT threading.
    in_reply_to_number = _extract_thread_parent(soup)

    # Content classification
    category = classify_subject(subject)

    return {
        "message_number": message_number,
        "message_id": message_id,
        "date": date,
        "date_raw": date_raw,
        "from_name": from_name,
        "subject": subject,
        "category": category,
        "body_raw": body_raw,
        "body_clean": body_clean,
        "in_reply_to_number": in_reply_to_number,
    }


def reconstruct_threads(df: pl.DataFrame) -> pl.DataFrame:
    """Add thread_id column by following in_reply_to chains to the root."""
    reply_map: dict[int, int] = {}
    for row in df.select("message_number", "in_reply_to_number").iter_rows(named=True):
        if row["in_reply_to_number"] is not None:
            reply_map[row["message_number"]] = row["in_reply_to_number"]

    def find_root(msg_number: int) -> int:
        visited: set[int] = set()
        current = msg_number
        while current in reply_map:
            if current in visited:
                break
            visited.add(current)
            current = reply_map[current]
        return current

    all_numbers = df["message_number"].to_list()
    thread_ids = [find_root(n) for n in all_numbers]

    return df.with_columns(pl.Series("thread_id", thread_ids))


def parse_all(input_dir: Path) -> pl.DataFrame:
    """Parse all HTML files in a directory into a Polars DataFrame."""
    html_files = sorted(input_dir.glob("msg*.html"), key=lambda p: int(p.stem[3:]))

    if not html_files:
        raise FileNotFoundError(f"No msg*.html files found in {input_dir}")

    log.info(f"Parsing {len(html_files)} messages...")

    records = []
    failed = 0
    for filepath in html_files:
        result = parse_message(filepath)
        if result:
            records.append(result)
        else:
            failed += 1

    log.info(f"Parsed {len(records)} messages ({failed} failed)")

    df = pl.DataFrame(records)
    df = reconstruct_threads(df)
    return df


def main():
    parser = argparse.ArgumentParser(description="Parse NMusers HTML to Parquet")
    parser.add_argument("--input", type=str, default="data/raw", help="Directory with HTML files")
    parser.add_argument("--output", type=str, default="data/messages.parquet", help="Output Parquet path")
    args = parser.parse_args()

    input_dir = Path(args.input)
    output_path = Path(args.output)

    df = parse_all(input_dir)

    # Summary stats
    categories = df.group_by("category").len().sort("len", descending=True)
    log.info("Content breakdown:")
    for row in categories.iter_rows(named=True):
        log.info(f"  {row['category']:15s} {row['len']:>6d}")

    thread_count = df["thread_id"].n_unique()
    date_range = f"{df['date'].min()} – {df['date'].max()}"
    log.info(f"Threads: {thread_count}")
    log.info(f"Date range: {date_range}")

    # Write Parquet
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)
    size_mb = output_path.stat().st_size / 1024**2
    log.info(f"Wrote {output_path} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
