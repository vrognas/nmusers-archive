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
import quopri
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
        "%d %B %y",
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

def _extract_old_format_text(soup: BeautifulSoup) -> str:
    """Extract text from old Cognigen pages without fake newlines between inline nodes."""
    root = soup.body if soup.body else soup
    html = str(root)
    # fm2html pages often include literal source newlines around <BR> tags.
    # Trim that wrapper whitespace first so one intended line break does not
    # turn into two when <BR> is converted below.
    html = re.sub(r"(?is)[ \t\r\n]*(<br\s*/?>)[ \t\r\n]*", r"\1", html)
    html = re.sub(r"(?i)<br\s*/?>\s*", "\n", html)
    html = re.sub(r"(?is)</p>\s*<p[^>]*>", "\n\n", html)
    html = re.sub(r"(?i)</?p[^>]*>", "", html)

    working = BeautifulSoup(html, "html.parser")
    text = working.get_text()
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\xa0", " ")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


def _normalize_old_format_block(block: str, page_title: str) -> str:
    """Drop leaked page titles and compact the inline header prelude."""
    lines = block.replace("\r\n", "\n").replace("\r", "\n").split("\n")

    while lines and not lines[0].strip():
        lines.pop(0)

    if page_title and lines and lines[0].strip() == page_title.strip():
        nonblank_after = next((line.strip() for line in lines[1:] if line.strip()), "")
        if nonblank_after.startswith(("From:", "Date:")):
            lines.pop(0)
            while lines and not lines[0].strip():
                lines.pop(0)

    header_labels = ("From:", "Subject:", "Date:", "Sent:")
    first_header_idx = next(
        (
            idx for idx, line in enumerate(lines[:8])
            if line.strip().startswith(header_labels) or re.match(r"^\s*From\s+\S", line)
        ),
        None,
    )
    if first_header_idx and first_header_idx > 0:
        lines = lines[first_header_idx:]

    header_mode = True
    normalized: list[str] = []

    for line in lines:
        stripped = line.strip()
        if header_mode and stripped.startswith(header_labels):
            normalized.append(stripped)
            continue
        if header_mode and not stripped:
            continue
        header_mode = False
        normalized.append(line.rstrip())

    text = "\n".join(normalized).strip()
    return re.sub(r"\n{3,}", "\n\n", text)


def _looks_like_old_message_start(lines: list[str], start_idx: int) -> bool:
    """Detect whether lines after a separator begin a new message header block."""
    found_headers: set[str] = set()
    seen_nonblank = 0

    for line in lines[start_idx:]:
        stripped = line.strip()
        if not stripped or re.fullmatch(r"[_-]{1,3}", stripped):
            continue

        seen_nonblank += 1
        header_match = re.match(r"^(From:|Subject:|Date:|Sent:)", stripped, re.IGNORECASE)
        if header_match:
            found_headers.add(header_match.group(1).rstrip(":").lower())
        elif re.match(r"^From\s+\S", stripped, re.IGNORECASE):
            found_headers.add("from")

        if seen_nonblank >= 6:
            break

    return len(found_headers) >= 2 and ("from" in found_headers or "subject" in found_headers)


def _split_old_format_blocks(body_text: str) -> list[str]:
    """Split old Cognigen thread pages into message-like blocks.

    These pages use visual separators inconsistently:
      - old Adobe/FrameMaker pages often use `****`
      - 2002 digest pages use dashed lines inside `<pre>`
      - posted code can also contain long separator lines

    Only treat a separator as a message boundary when the following lines
    actually look like a new header block.
    """
    lines = body_text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    blocks: list[str] = []
    current: list[str] = []

    idx = 0
    while idx < len(lines):
        if re.fullmatch(r"\s*(?:_{7,}|={7,}|\*{4,}|-{7,})\s*", lines[idx]):
            next_idx = idx + 1
            while next_idx < len(lines):
                stripped = lines[next_idx].strip()
                if not stripped or re.fullmatch(r"[_-]{1,3}", stripped):
                    next_idx += 1
                    continue
                break

            if _looks_like_old_message_start(lines, next_idx):
                block = "\n".join(current).strip()
                if block:
                    blocks.append(block)
                current = []
                idx = next_idx
                continue

        current.append(lines[idx])
        idx += 1

    block = "\n".join(current).strip()
    if block:
        blocks.append(block)

    return blocks

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

    body_text = _extract_old_format_text(soup)

    messages = []

    # Split only when a separator is followed by the start of another message.
    # Old Cognigen pages often include long separator lines inside posted code
    # or signatures, and some thread pages start the next message with Subject
    # or Date rather than From.
    message_blocks = _split_old_format_blocks(body_text)

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
    block = _normalize_old_format_block(block, page_title)
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

def _extract_html_fragment_text(fragment) -> str:
    """Extract text while treating HTML line-break tags explicitly."""
    html = str(fragment)
    html = re.sub(r"(?is)[ \t\r\n]*(<br\s*/?>)[ \t\r\n]*", r"\1", html)
    html = re.sub(r"(?i)<br\s*/?>", "\n", html)
    html = re.sub(r"(?is)</p>\s*<p[^>]*>", "\n\n", html)
    html = re.sub(r"(?i)</?p[^>]*>", "", html)
    working = BeautifulSoup(html, "html.parser")
    return working.get_text()


def _normalize_pipermail_body(text: str, raw_html: str = "") -> str:
    """Normalize pipermail body text and decode quoted-printable artifacts."""
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\xa0", " ")

    soft_break_count = len(re.findall(r"=\n", text))
    hex_escape_count = len(re.findall(r"=[0-9A-F]{2}", text))
    looks_quoted_printable = (
        bool(re.search(r"=\s*\n\s*<br\s*/?>", raw_html, flags=re.IGNORECASE))
        or soft_break_count >= 2
        or (soft_break_count >= 1 and hex_escape_count >= 1)
        or hex_escape_count >= 3
    )
    if looks_quoted_printable:
        text = quopri.decodestring(text.encode("utf-8")).decode("utf-8", errors="replace")
        text = text.replace("\r\n", "\n").replace("\r", "\n")

    lines = [line.rstrip() for line in text.split("\n")]
    text = "\n".join("" if not line.strip() else line for line in lines)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _extract_hypermail_body(soup: BeautifulSoup) -> tuple[str, str]:
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
        raw_html = str(mail_div)
        return _extract_html_fragment_text(mail_div).strip(), raw_html

    # Fallback: <pre> tags
    pre_parts = [_extract_html_fragment_text(pre) for pre in soup.find_all("pre")]
    if pre_parts:
        return "\n".join(pre_parts).strip(), "\n".join(str(pre) for pre in soup.find_all("pre"))

    # Last resort: full page text
    return _extract_html_fragment_text(soup).strip(), str(soup)


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
    body_raw, body_html = _extract_hypermail_body(soup)
    body_raw = _normalize_pipermail_body(body_raw, body_html)
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


_PHOR_NMO_MESSAGE_PREFIXES = (
    "Question started by",
    "Topic started by",
    "Topic originated by",
    "Reply by",
    "Response from",
    "Response by",
    "Memo from",
    "Provided by",
)

_PHOR_NMO_DATE_OVERRIDES = {
    ("topic001.html", "question started by"): "21 Nov 1993",
    ("topic012.html", "topic started by"): "15 Mar 1994",
    ("topic012.html", "response from"): "15 Mar 1994",
}

_PHOR_NMO_TOPIC_DATE_OVERRIDES = {
    "topic002.html": "6 Apr 1994",
}

_PHOR_NMO_TOPIC_SEQUENCE_DATE_OVERRIDES = {
    # topic007 has a dated opener followed by ordered undated replies.
    # Keep the source order and impute increasing dates so the thread
    # renders chronologically instead of spilling into /undated/.
    "topic007.html": [
        "17 Feb 1994",
        "18 Feb 1994",
        "19 Feb 1994",
        "20 Feb 1994",
        "21 Feb 1994",
        "22 Feb 1994",
    ],
}


def _extract_phor_nmo_text(soup: BeautifulSoup) -> str:
    """Extract normalized text from pre-1995 phor topic pages."""
    root = soup.body if soup.body else soup
    for br in root.find_all("br"):
        br.replace_with("\n")
    text = root.get_text("\n")
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\xa0", " ")
    return re.sub(r"\n{3,}", "\n\n", text)


def _looks_like_phor_nmo_heading(line: str) -> bool:
    normalized = " ".join(line.strip().split())
    if not normalized:
        return False
    lowered = normalized.lower()
    return lowered.startswith(tuple(prefix.lower() for prefix in _PHOR_NMO_MESSAGE_PREFIXES)) or lowered.startswith("end of topic")


def _split_phor_nmo_author_and_date(text: str) -> tuple[str, str]:
    """Split heading payload into author and optional date."""
    cleaned = " ".join(text.strip().split())
    if not cleaned:
        return "", ""

    if " - " in cleaned:
        author, maybe_date = cleaned.rsplit(" - ", 1)
        if parse_date_flexible(maybe_date):
            return author.strip(), maybe_date.strip()

    parts = cleaned.split()
    for width in range(min(5, len(parts)), 2, -1):
        candidate = " ".join(parts[-width:])
        if parse_date_flexible(candidate):
            author = " ".join(parts[:-width]).strip(" -")
            return author, candidate

    return cleaned, ""


def _clean_phor_nmo_author(text: str) -> str:
    raw = (text or "").strip()
    cleaned = re.sub(r"\b\S+@\S+\b", "", raw)
    cleaned = re.sub(r"\(\s*\)", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -:")
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = cleaned[1:-1].strip()
    if cleaned:
        return cleaned
    email_match = re.search(r"\b\S+@\S+\b", raw)
    return email_match.group(0) if email_match else "Unknown"


def parse_phor_nmo_page(filepath: Path) -> list[dict]:
    """Parse a pre-1995 phor topic page into per-message records."""
    try:
        html = read_html(filepath)
    except OSError:
        return []

    soup = BeautifulSoup(html, "html.parser")
    subject_el = soup.find("h1")
    subject = subject_el.get_text(" ", strip=True) if subject_el else filepath.stem
    body_text = _extract_phor_nmo_text(soup)
    lines = body_text.split("\n")

    sections: list[tuple[str, list[str]]] = []
    current_heading: str | None = None
    current_body: list[str] = []

    idx = 0
    while idx < len(lines):
        line = lines[idx].rstrip()
        stripped = line.strip()
        normalized = " ".join(stripped.split())

        if _looks_like_phor_nmo_heading(normalized):
            if idx + 1 < len(lines):
                next_normalized = " ".join(lines[idx + 1].strip().split())
                if next_normalized.startswith("- "):
                    normalized = f"{normalized} {next_normalized}"
                    idx += 1

            if normalized.lower().startswith("end of topic"):
                break

            if current_heading is not None:
                sections.append((current_heading, current_body))
            current_heading = normalized
            current_body = []
            idx += 1
            continue

        if current_heading is not None:
            current_body.append(line)
        idx += 1

    if current_heading is not None:
        sections.append((current_heading, current_body))

    records = []
    heading_re = re.compile(
        r"^(?P<prefix>" + "|".join(re.escape(prefix) for prefix in _PHOR_NMO_MESSAGE_PREFIXES) + r")\s*:?\s*(?P<rest>.+)$",
        re.IGNORECASE,
    )

    sequence_overrides = _PHOR_NMO_TOPIC_SEQUENCE_DATE_OVERRIDES.get(filepath.name, [])

    for section_idx, (heading, raw_body_lines) in enumerate(sections):
        match = heading_re.match(heading)
        if not match:
            continue

        prefix = match.group("prefix").strip().lower()
        author_raw, date_raw = _split_phor_nmo_author_and_date(match.group("rest"))
        if not date_raw:
            if section_idx < len(sequence_overrides):
                date_raw = sequence_overrides[section_idx]
        if not date_raw:
            date_raw = _PHOR_NMO_TOPIC_DATE_OVERRIDES.get(filepath.name, "")
        if not date_raw:
            date_raw = _PHOR_NMO_DATE_OVERRIDES.get((filepath.name, prefix), "")
        body_raw = "\n".join(raw_body_lines).strip()
        if not body_raw:
            continue

        records.append(
            {
                "source": "phor",
                "source_file": filepath.name,
                "source_url": f"https://web.archive.org/web/*/http://www.phor.com/nonmem/nmo/{filepath.name}",
                "date": parse_date_flexible(date_raw) if date_raw else None,
                "date_raw": date_raw,
                "from_name": _clean_phor_nmo_author(author_raw),
                "subject": subject,
                "category": classify_subject(subject),
                "body_raw": body_raw,
                "body_clean": strip_disclaimers(body_raw),
            }
        )

    return records


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


def parse_all_phor(input_dir: Path, nmo_input_dir: Path) -> pl.DataFrame:
    frames = []

    if input_dir.exists() and any(input_dir.glob("*.html")):
        frames.append(parse_all_old(input_dir))

    nmo_files = sorted(nmo_input_dir.glob("*.html")) if nmo_input_dir.exists() else []
    if nmo_files:
        log.info(f"Parsing {len(nmo_files)} phor pre-1995 topic pages...")
        nmo_messages = []
        for filepath in nmo_files:
            nmo_messages.extend(parse_phor_nmo_page(filepath))
        log.info(f"Extracted {len(nmo_messages)} messages from {len(nmo_files)} phor topic pages")
        frames.append(pl.DataFrame(nmo_messages))

    if not frames:
        raise FileNotFoundError(f"No HTML files in {input_dir} or {nmo_input_dir}")

    return pl.concat(frames, how="diagonal_relaxed")


def main():
    parser = argparse.ArgumentParser(description="Parse Cognigen archives")
    parser.add_argument(
        "--source",
        choices=["old", "pipermail", "phor", "all"],
        default="all",
    )
    args = parser.parse_args()

    sources = ["old", "pipermail", "phor"] if args.source == "all" else [args.source]

    for source in sources:
        config = {
            "old": ("data/raw_cognigencorp", "data/cognigencorp_messages.parquet"),
            "pipermail": ("data/raw_cognigen_pipermail", "data/cognigen_pipermail_messages.parquet"),
            "phor": ("data/raw_phor", "data/phor_messages.parquet"),
        }
        input_dir, output_path = config[source]
        input_path = Path(input_dir)
        out = Path(output_path)
        phor_nmo_path = Path("data/raw_phor_nmo")

        if not input_path.exists():
            log.warning(f"{input_path} not found — run wayback_recover.py first")
            continue

        if source == "old":
            df = parse_all_old(input_path)
        elif source == "phor":
            df = parse_all_phor(input_path, phor_nmo_path)
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
