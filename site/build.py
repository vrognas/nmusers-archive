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
import json
import logging
import re
import shutil
import subprocess
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
OVERRIDES_PATH = Path("data/author_overrides.json")
MONTH_NAMES = {i: calendar.month_name[i] for i in range(1, 13)}

# Name particles that should stay lowercase (except at start of name)
_NAME_PARTICLES = {"van", "von", "de", "del", "der", "den", "di", "du", "la", "le", "ter", "het"}


def _load_author_overrides() -> dict[str, str]:
    """Load manual author name overrides from JSON."""
    if OVERRIDES_PATH.exists():
        return json.loads(OVERRIDES_PATH.read_text(encoding="utf-8"))
    return {}


_AUTHOR_OVERRIDES = _load_author_overrides()


def _decode_mime(name: str) -> str:
    """Decode MIME-encoded name headers like =?utf-8?B?...?= or =?iso-8859-1?Q?...?=."""
    import base64
    import quopri
    match = re.match(r"=\?([^?]+)\?([BQ])\?([^?]+)\?=", name, re.IGNORECASE)
    if not match:
        return name
    charset, encoding, payload = match.group(1), match.group(2).upper(), match.group(3)
    try:
        raw = base64.b64decode(payload) if encoding == "B" else quopri.decodestring(payload.encode())
        return raw.decode(charset, errors="replace")
    except Exception:
        return name


def normalize_author(name: str) -> str:
    """Normalize author name: apply overrides, strip suffixes, flip Last/First, title-case."""
    if not name or name == "Unknown":
        return name
    # Manual overrides first (exact match on raw name)
    if name in _AUTHOR_OVERRIDES:
        return _AUTHOR_OVERRIDES[name]
    n = name
    # Strip wrapping parentheses: "(Jeff Koup)" → "Jeff Koup"
    if n.startswith("(") and n.endswith(")"):
        n = n[1:-1].strip()
    # Decode MIME-encoded names
    if n.startswith("=?"):
        n = _decode_mime(n)
    # Strip "Re: [NMusers]..." or "Subject:" (leaked subject lines, not names)
    if re.match(r"^(Re:\s*|Fwd?:\s*|\[NMusers\]|Subject:)", n, re.IGNORECASE):
        return "Unknown"
    # Strip "From: " prefix leaked from headers
    n = re.sub(r'^From:\s*"?', "", n).strip()
    # Strip _at_ email addresses (name_at_domain.com -> name part only)
    n = re.sub(r"_at_\S+", "", n).strip()
    # Strip titles and honorifics
    n = re.sub(r"\b(Dr|PhD|Ph\.D|Pharm\.?D|M\.?D|M\.?Sc|M\.?S|B\.?Sc|B\.?Ch|M\.?b|Mieee|Prof)\.?\b[,.]?\s*", "", n, flags=re.IGNORECASE).strip()
    # Strip trailing '" -' artifacts
    n = re.sub(r'\s*"\s*-\s*$', "", n).strip()
    # Strip phone/fax numbers
    n = re.sub(r"\s*\(?\d{3}\)?\s*[-.]?\s*\d{3}\s*[-.]?\s*\d{4}.*$", "", n).strip()
    n = re.sub(r"\s*\+\d[\d\s/]+$", "", n).strip()
    n = re.sub(r"\s*FAX\b.*$", "", n, flags=re.IGNORECASE).strip()
    # Strip {PDBS~Basel} style org tags
    n = re.sub(r"\s*\{[^}]*\}\s*", " ", n).strip()
    # Strip /org suffixes (HMR/US, /FR, /Formulation/VKH, /FAES)
    n = re.sub(r"\s*/\w[\w/]*\s*$", "", n).strip()
    # Strip escaped parens \(...\)
    n = re.sub(r"\s*\\?\([^)]*\\?\)\s*", " ", n).strip()
    # Strip stray backslashes and asterisks
    n = re.sub(r"[\\*]", "", n).strip()
    # Strip suffixes in (...) and [...]
    for _ in range(2):
        n = re.sub(r"\s*\([^)]*\)\s*$", "", n).strip()
        n = re.sub(r"\s*\[[^\]]*\]\s*$", "", n).strip()
    # Strip U+FFFD replacement characters
    n = n.replace("\ufffd", "").strip()
    # Strip trailing commas, hyphens, periods from cleanup artifacts
    n = n.strip(",-. ")
    # If nothing meaningful remains, return Unknown
    if not n or len(n) < 2:
        return "Unknown"
    # Replace dots between long words with spaces (Silke.Retlich -> Silke Retlich)
    # but keep dots after single letters (A. Tahami stays A. Tahami)
    n = re.sub(r"(?<=[a-zA-Z]{2})\.(?=[a-zA-Z]{2})", " ", n)
    # Handle "Last, First" -> "First Last"
    if "," in n and "@" not in n:
        parts = [p.strip() for p in n.split(",", 1)]
        if len(parts) == 2 and parts[1] and len(parts[0].split()) <= 3 and len(parts[1].split()) <= 3:
            n = f"{parts[1]} {parts[0]}"
    # Title-case, preserving particles
    words = n.split()
    result = []
    for i, w in enumerate(words):
        if w.lower() in _NAME_PARTICLES and i > 0:
            result.append(w.lower())
        elif w.isupper() and len(w) > 2:
            result.append(w.capitalize())
        elif w.islower() and len(w) > 2 and w not in _NAME_PARTICLES:
            result.append(w.capitalize())
        else:
            result.append(w)
    n = " ".join(result)
    # Check overrides again after normalization
    if n in _AUTHOR_OVERRIDES:
        return _AUTHOR_OVERRIDES[n]
    return n


def commafy(value: int) -> str:
    return f"{value:,}"


def clean_body(text: str) -> Markup:
    """Clean HTML remnants and auto-link URLs in message bodies."""
    # Replace U+FFFD replacement characters with best-guess substitution
    text = text.replace("\ufffd", "")
    # Normalize CRLF/CR line endings before any newline-sensitive cleanup
    text = text.replace("\r\n", "\n").replace("\r", "\n")
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
    # Split concatenated URLs (e.g. "...docs.htmlhttps://..." → "...docs.html https://...")
    text = re.sub(r"(?<=\S)(https?://)", r" \1", text)
    # Collapse excessive blank lines (3+ → 2)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = text.strip()

    # HTML-escape the content (so user content is safe)
    text = html.escape(text)

    # Auto-link URLs (after escaping, so the <a> tags we insert are preserved)
    # Note: &amp; is allowed (query params), but &lt; &gt; &quot; are not
    text = re.sub(
        r'(https?://(?:[^\s<>)\]"\'&]|&amp;)+)',
        r'<a href="\1">\1</a>',
        text,
    )
    # Auto-link www. URLs without protocol
    text = re.sub(
        r'(?<!/)(www\.(?:[^\s<>)\]"\'&]|&amp;)+)',
        r'<a href="http://\1">\1</a>',
        text,
    )
    return Markup(text)


def normalize_subject(subject: str) -> str:
    """Strip Re:/FW: prefixes and [NMusers] tag for thread grouping."""
    cleaned = (subject or "").strip().replace("[NMusers]", "").strip()
    while True:
        new = cleaned
        for prefix in ["Re:", "RE:", "Fwd:", "FW:", "Fw:", "re:"]:
            if new.startswith(prefix):
                new = new[len(prefix) :].strip()
        if new == cleaned:
            break
        cleaned = new
    s = cleaned.lower().strip()
    if s in {"", "no subject", "(no subject)"}:
        return "(no subject)"
    # Normalize common spelling variants for better thread grouping
    s = s.replace("modelling", "modeling").replace("behaviour", "behavior")
    s = s.replace("minimisation", "minimization").replace("optimisation", "optimization")
    s = s.replace("parametrisation", "parametrization").replace("characterisation", "characterization")
    return s


def display_subject(subject: str | None) -> str:
    """Return a subject suitable for display, preserving reply prefixes."""
    cleaned = (subject or "").strip().replace("[NMusers]", "").strip()
    if cleaned.lower() in {"", "no subject", "(no subject)"}:
        return "(no subject)"
    return cleaned


def display_thread_subject(subject: str) -> str:
    """Strip common reply prefixes for display without lowercasing."""
    cleaned = (subject or "").strip().replace("[NMusers]", "").strip()
    while True:
        new = cleaned
        for prefix in ["Re:", "RE:", "Fwd:", "FW:", "Fw:", "re:"]:
            if new.startswith(prefix):
                new = new[len(prefix) :].strip()
        if new == cleaned:
            break
        cleaned = new
    if cleaned.lower() in {"", "no subject", "(no subject)"}:
        return "(no subject)"
    return cleaned


_REPLY_PREFIX_RE = re.compile(r"^(?:(?:re|fw|fwd|aw)\s*:\s*)+", re.IGNORECASE)


def is_reply_subject(subject: str | None) -> bool:
    cleaned = (subject or "").strip().replace("[NMusers]", "").strip()
    return bool(_REPLY_PREFIX_RE.match(cleaned))


def order_thread_messages(messages: list[dict]) -> list[dict]:
    """Prefer the earliest non-reply subject as thread starter, then chronological order."""
    if not messages:
        return []
    ordered = sorted(
        messages,
        key=lambda m: (
            m.get("date") is None,
            m.get("date"),
            m.get("msg_seq", 0),
        ),
    )
    roots = [m for m in ordered if not is_reply_subject(m.get("subject"))]
    if not roots:
        return ordered
    root = roots[0]
    return [root] + [m for m in ordered if m is not root]


_TOP_POST_REPLY_PATTERNS = [
    re.compile(r"\n(?:>\s*)?On [^\n]+(?:\n(?:>\s*)?[^\n]*){0,8}\bwrote:\s*\n", re.IGNORECASE),
    re.compile(
        r"\n(?:>\s*)?Op [^\n]+(?:\n(?:>\s*)?[^\n]*){0,8}\bhet volgende geschreven:\s*\n",
        re.IGNORECASE,
    ),
    re.compile(r"\n(?:>\s*)?Op [^\n]+(?:\n(?:>\s*)?[^\n]*){0,8}\bschreef:\s*\n", re.IGNORECASE),
    re.compile(r"\n(?:>\s*)?El [^\n]+(?:\n(?:>\s*)?[^\n]*){0,8}\bescribió:\s*\n", re.IGNORECASE),
    re.compile(r"\n(?:>\s*)?-+\s*Original Message\s*-+\s*\n", re.IGNORECASE),
    # Outlook-style quoted chains often start with a long underscore divider
    # before wrapped From/Sent/To/Subject headers.
    re.compile(r"\n_{8,}\s*\n(?=(?:From:|보낸 사람:)\s)", re.IGNORECASE),
    re.compile(r"\nFrom:\s+.+\n(?:Sent|Date):\s+.+\nTo:\s+.+\nSubject:\s+.+\n", re.IGNORECASE),
    re.compile(r"\nVan:\s+.+\nVerzonden:\s+.+\nAan:\s+.+\nOnderwerp:\s+.+\n", re.IGNORECASE),
]

_HEADER_START_LABELS = ("From:", "Van:", "보낸 사람:", "De:")
_HEADER_SENT_LABELS = ("Sent:", "Date:", "Verzonden:", "보냄:", "Envoyé:", "Envoye:")
_HEADER_TO_LABELS = ("To:", "Aan:", "받는 사람:", "À:", "A:")
_HEADER_CC_LABELS = ("Cc:", "Kopie:", "참조:")
_HEADER_SUBJECT_LABELS = ("Subject:", "Onderwerp:", "제목:", "Objet:")


def _line_has_header_label(line: str, labels: tuple[str, ...]) -> bool:
    """Return True when a line starts with one of the localized mail header labels."""
    stripped = line.lstrip()
    while stripped.startswith(">"):
        stripped = stripped[1:].lstrip()
    normalized = re.sub(r"\s*:\s*", ":", stripped, count=1).casefold()
    return any(normalized.startswith(label.casefold()) for label in labels)


def _find_wrapped_header_block(text: str) -> int | None:
    """Detect Outlook-style quoted header blocks with localized labels and wrapped lines."""
    lines = text.splitlines(keepends=True)
    if not lines:
        return None

    offsets = []
    position = 0
    for line in lines:
        offsets.append(position)
        position += len(line)

    for i, line in enumerate(lines):
        if not _line_has_header_label(line, _HEADER_START_LABELS):
            continue

        seen_sent = False
        seen_to = False
        seen_subject = False
        for j in range(i + 1, min(len(lines), i + 20)):
            current = lines[j]
            if _line_has_header_label(current, _HEADER_SENT_LABELS):
                seen_sent = True
            elif _line_has_header_label(current, _HEADER_TO_LABELS):
                seen_to = True
            elif _line_has_header_label(current, _HEADER_CC_LABELS):
                continue
            elif _line_has_header_label(current, _HEADER_SUBJECT_LABELS):
                seen_subject = True

            if seen_sent and seen_to and seen_subject:
                return offsets[i]

    return None


_DATED_ATTRIBUTION_RE = re.compile(r"^\s*\d{4}[/-]\d{1,2}[/-]\d{1,2}\s+\S")


def _find_dated_quoted_block(text: str) -> int | None:
    """Detect Gmail-style attribution lines followed by a >-quoted original message."""
    lines = text.splitlines(keepends=True)
    if not lines:
        return None

    offsets = []
    position = 0
    for line in lines:
        offsets.append(position)
        position += len(line)

    for i, line in enumerate(lines):
        if not _DATED_ATTRIBUTION_RE.match(line):
            continue

        quote_count = 0
        prelude_lines = 0
        for j in range(i + 1, min(len(lines), i + 20)):
            stripped = lines[j].strip()
            if not stripped:
                continue

            if stripped.startswith(">"):
                quote_count += 1
                if quote_count >= 3:
                    return offsets[i]
                continue

            if quote_count > 0:
                break

            # Allow wrapped attribution fragments before the quoted block starts.
            if (
                "@" in stripped
                or "email" in stripped.casefold()
                or "mailto:" in stripped.casefold()
                or stripped.endswith("<")
                or stripped.endswith(">")
                or stripped in {"<", ">"}
            ):
                prelude_lines += 1
                if prelude_lines > 4:
                    break
                continue

            break

    return None


def split_reply_history(text: str, source: str | None = None) -> tuple[str, str | None]:
    """Split an email body into the new content and quoted reply history."""
    body = (text or "").strip()
    if not body:
        return "", None

    patterns = _TOP_POST_REPLY_PATTERNS
    if source == "cognigencorp":
        # This source often starts with forwarded headers or quoted context
        # before the new reply, so only split on explicit original-message blocks.
        patterns = [re.compile(r"\n-+\s*Original Message\s*-+\s*\n", re.IGNORECASE)]

    cut_points = []
    for pattern in patterns:
        match = pattern.search(body)
        if match and match.start() > 0:
            cut_points.append(match.start())

    if source != "cognigencorp":
        header_cut = _find_wrapped_header_block(body)
        if header_cut is not None and header_cut > 0:
            cut_points.append(header_cut)
        dated_quote_cut = _find_dated_quoted_block(body)
        if dated_quote_cut is not None and dated_quote_cut > 0:
            cut_points.append(dated_quote_cut)

    if not cut_points:
        return body, None

    cut = min(cut_points)
    main = body[:cut].rstrip()
    quoted = body[cut:].lstrip()

    if not main or not quoted:
        return body, None

    return main, quoted


def thread_page_url(thread_messages: list[dict]) -> str:
    """Build a stable URL for a full-thread page."""
    first = thread_messages[0]
    identifier = first.get("thread_id") or first.get("message_number")
    if not identifier:
        if first.get("year") is not None and first.get("month") is not None:
            identifier = f"{first['year']}-{first['month']:02d}-{first['msg_seq']}"
        elif first.get("source_file"):
            identifier = Path(first["source_file"]).stem
        else:
            identifier = f"undated-{first['msg_seq']}"
    subject_slug = slugify(display_thread_subject(first["subject"]))[:80] or "thread"
    return f"/threads/{subject_slug}-{identifier}/"


# Improved category patterns (applied at build time to override source data)
# Order matters: event is checked before job so conference/course signals win
_CATEGORY_PATTERNS = {
    "admin": re.compile(
        r"\bunsubscri|\bsubscri|remove me|sign.?off\b|"
        r"\bvirus\b|\bspam\b|do not open|phishing|"
        r"^test\b|test message|please ignore|do not respond|linkedin|"
        r"^To:|email\s*protected",
        re.IGNORECASE,
    ),
    "event": re.compile(
        r"workshop|courses?\b|training|registration|diploma|PAGE\s?\d{4}|"
        r"PAGANZ|webinar|symposium|conference|congress|summer school|"
        r"\bmeeting\b.*\d{4}|\d{4}.*\bmeeting\b|"
        r"\bat PAGE\b|ACoP\d*\b|\bWCoP\b|\bISOP\b.*(?:webinar|session)|"
        r"\bASCPT\b|save the date|mark your calendar|register for|tutorial|PDx-Pop\s+workshop|"
        r"call for.*program|call for.*abstract|call for.*paper|"
        r"\bAPN\b|\bPKUK\b|\bQSPC\b|\bUPSS\b",
        re.IGNORECASE,
    ),
    "job": re.compile(
        r"hiring|position[s]?\b|opportunit|career|recruit|"
        r"job\b|talent|now hiring|"
        r"director\b.*(?:role|search|quantitative|pharmacol|pharmacomet)|"
        r"scientists?\b|researchers?\b|looking for\b.*(?:scientist|modeler|pharmacomet|analyst|candidate|talent|person)|fellowship|postdoc|"
        r"post.?doc|vacancy|vacanc|openings?\b|"
        r"associate director|senior.*(?:scientist|manager)|"
        r"pharmacometrician|apply\b.*(?:role|position)|"
        r"we are seeking|join our|join the|"
        r"head of|vice president|\bVP\b.*pharm|wanted|"
        r"\bintern\b|\binternship|faculty|professor|tenure|"
        r"modelers?\b.*(?:for|at|in)|laboratory|studentship|"
        r"expert\b.*(?:in|at|for)|(?:pharmacomet|PK.?PD|MIDD).*(?:in\s+\w+,|germany|usa|uk|france)|"
        r"call for applications|PhD program|lecturer\b|collaborator\w*\s+sought|sought\b|"
        r"call for.*members?\b|calling all\b|(?:pharmaco|PK.?PD|clinical|remote|senior)\w*\s+role|role\b.*(?:pharmaco|PK|clinical|remote|senior)|"
        r"leverage your skills",
        re.IGNORECASE,
    ),
    "news": re.compile(
        r"\breleased?\b|available from\b|now available|version \d|new member|"
        r"Wings for NONMEM|\bWFN\b|sad news|passing of|passed away|funeral|in memoriam|obituary|"
        r"\bR package\b|\bpython package\b|an? \w+ package for|"
        r"^new software$|^new tool\b|software update|"
        r"distribution of NONMEM|update.*available|"
        r"^\w+ (?:design )?software\b|bug list|change\s*log|patch\b|"
        r"discussion group\b|user group\b|citations.*archive|"
        r"^new journal$",
        re.IGNORECASE,
    ),
}


def classify_message(subject: str | None, body: str | None) -> str:
    """Classify a message, primarily from its subject line."""
    subject = subject or ""
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

    # Fix swapped from_name/subject fields (parsing artifact from cognigencorp)
    swapped = df["from_name"].str.contains(r"^(?:Re:|RE:|Fwd?:|FW:|\[NMusers\])")
    name_in_subject = ~df["subject"].str.contains(r"^(?:Re:|RE:|Fwd?:|FW:|\[NMusers\])") & swapped
    df = df.with_columns(
        pl.when(name_in_subject).then(pl.col("subject")).otherwise(pl.col("from_name")).alias("from_name"),
        pl.when(name_in_subject).then(pl.col("from_name")).otherwise(pl.col("subject")).alias("subject"),
    )

    # Reclassify with improved patterns
    df = df.with_columns(
        pl.struct(["subject", "body_clean"])
        .map_elements(
            lambda row: classify_message(row["subject"], row["body_clean"]),
            return_dtype=pl.Utf8,
        )
        .alias("category"),
        pl.col("from_name")
        .map_elements(clean_from_name, return_dtype=pl.Utf8)
        .alias("from_name"),
    )

    # Normalize display subjects so blank subjects never render empty.
    df = df.with_columns(
        pl.col("subject")
        .map_elements(display_subject, return_dtype=pl.Utf8)
        .alias("subject"),
    )

    # Normalize author names: overrides, strip suffixes, flip Last/First, title-case
    df = df.with_columns(
        pl.col("from_name")
        .map_elements(normalize_author, return_dtype=pl.Utf8)
        .alias("from_name"),
    )

    df = df.with_columns(
        pl.col("date").dt.year().alias("year"),
        pl.col("date").dt.month().alias("month"),
        pl.col("from_name")
        .map_elements(lambda x: slugify(x) if x else "unknown", return_dtype=pl.Utf8)
        .alias("author_slug"),
        pl.col("subject")
        .map_elements(normalize_subject, return_dtype=pl.Utf8)
        .alias("_subject_key"),
    )

    # Use source-native thread metadata when available, otherwise fall back to subject.
    # mail-archive has explicit thread ids; old Cognigen pages are multi-message thread pages.
    if "thread_id" in df.columns:
        df = df.with_columns(
            pl.when(pl.col("thread_id").is_not_null())
            .then(pl.lit("ma:") + pl.col("thread_id").cast(pl.Utf8))
            .when(pl.col("source").is_in(["cognigencorp", "phor"]) & pl.col("source_url").is_not_null())
            .then(
                pl.when(pl.col("source") == "cognigencorp")
                .then(pl.lit("cg:"))
                .otherwise(pl.lit("ph:"))
                + pl.col("source_url")
            )
            .otherwise(pl.col("_subject_key"))
            .alias("thread_key"),
        )
    else:
        df = df.with_columns(pl.col("_subject_key").alias("thread_key"))
    df = df.drop("_subject_key")

    # Sort by date for consistent ordering
    df = df.sort("date")

    # Split threads with large time gaps (>30 days between consecutive messages)
    # This prevents unrelated discussions with the same subject from being grouped
    thread_epoch = {}
    thread_keys = df["thread_key"].to_list()
    dates = df["date"].to_list()
    new_keys = []
    for i, (tk, dt) in enumerate(zip(thread_keys, dates)):
        if dt is None or tk.startswith(("ma:", "cg:", "ph:")):
            # Skip time-splitting for source-native threads that are already grouped.
            new_keys.append(tk)
            continue
        if tk not in thread_epoch:
            thread_epoch[tk] = {"last_date": dt, "seq": 0}
            new_keys.append(tk)
        else:
            gap = (dt - thread_epoch[tk]["last_date"]).days
            if gap > 60:
                thread_epoch[tk]["seq"] += 1
            thread_epoch[tk]["last_date"] = dt
            seq = thread_epoch[tk]["seq"]
            new_keys.append(f"{tk}#{seq}" if seq > 0 else tk)
    df = df.with_columns(pl.Series("thread_key", new_keys))

    # Assign sequential IDs within each year-month group. Use a non-null
    # column for the cumulative count so undated rows do not all collapse
    # to msg_seq=0.
    df = df.with_columns(
        pl.cum_count("subject").over("year", "month").alias("msg_seq"),
    )

    log.info(f"Loaded {len(df)} messages")
    return df


def msg_url(row: dict) -> str:
    """Generate URL path for a message."""
    if row["year"] is None or row["month"] is None:
        return f"/undated/{row['msg_seq']}.html"
    return f"/{row['year']}/{row['month']:02d}/{row['msg_seq']}.html"


def msg_date_short(row: dict) -> str:
    """Format date as 'Mar 15, 2006'."""
    if row["date"] is None:
        return "?"
    return row["date"].strftime("%b %d, %Y")


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
    years_df = df.filter(pl.col("year").is_not_null())
    years_data = (
        years_df.group_by("year")
        .len()
        .sort("year")
        .rename({"len": "count"})
        .to_dicts()
    )
    # Category breakdown per year for stacked chart
    year_cats = (
        years_df.group_by("year", "category")
        .len()
        .sort("year")
        .rename({"len": "count"})
    )
    year_cat_map = {}
    for r in year_cats.iter_rows(named=True):
        yr = r["year"]
        if yr not in year_cat_map:
            year_cat_map[yr] = {}
        year_cat_map[yr][r["category"]] = r["count"]
    for yd in years_data:
        cats = year_cat_map.get(yd["year"], {})
        yd["technical"] = cats.get("technical", 0)
        yd["job"] = cats.get("job", 0)
        yd["event"] = cats.get("event", 0)
        yd["news"] = cats.get("news", 0)
        yd["admin"] = cats.get("admin", 0)
    recent = [r for r in reversed(rows) if r["date"] is not None]
    initial_recent = recent[:30]
    recent_years = sorted({r["year"] for r in recent}, reverse=True)
    home_messages = [
        {
            "subject": r["subject"],
            "url": r["url"],
            "from_name": r["from_name"],
            "author_slug": r["author_slug"],
            "date_short": r["date_short"],
            "category": r["category"],
            "year": r["year"],
            "thread_key": r["thread_key"],
        }
        for r in recent
    ]

    source_counts = df.group_by("source").len().to_dicts()
    source_map = {r["source"]: r["len"] for r in source_counts}

    year_min = df["year"].min()
    year_max = df["year"].max()

    home_html = env.get_template("home.html").render(
        total_messages=len(rows),
        total_authors=df["from_name"].n_unique(),
        total_technical=df.filter(pl.col("category") == "technical").height,
        year_min=year_min,
        year_max=year_max,
        years=years_data,
        recent_messages=initial_recent,
        recent_years=recent_years,
    )
    (output_dir / "index.html").write_text(home_html, encoding="utf-8")
    (output_dir / "home-messages.json").write_text(
        json.dumps(home_messages, separators=(",", ":"), ensure_ascii=False),
        encoding="utf-8",
    )

    # --- Search page ---
    log.info("Generating search page...")
    search_html = env.get_template("search.html").render(
        total_messages=len(rows),
        year_max=year_max,
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

        # Group by thread_key, order threads by most recent message
        thread_buckets: dict[str, list[dict]] = {}
        for m in msgs:
            tk = m["thread_key"]
            if tk not in thread_buckets:
                thread_buckets[tk] = []
            thread_buckets[tk].append(m)
        for tk, thread in thread_buckets.items():
            thread_buckets[tk] = order_thread_messages(thread)
        # Sort threads by latest message (descending), messages within thread with the starter first
        sorted_threads = sorted(
            thread_buckets.values(),
            key=lambda t: t[-1]["date"] if t[-1]["date"] else t[0]["date"],
            reverse=True,
        )
        # Mark is_reply: first message in each thread is not a reply
        thread_sorted_msgs = []
        for thread in sorted_threads:
            for i, m in enumerate(thread):
                m["is_reply"] = i > 0 and len(thread) > 1
            thread_sorted_msgs.extend(thread)

        month_html = env.get_template("month.html").render(
            year=year,
            month_name=MONTH_NAMES[month],
            messages=thread_sorted_msgs,
            date_sorted=msgs,
        )
        (month_dir / "index.html").write_text(month_html, encoding="utf-8")

    # --- Message pages ---
    log.info("Generating message pages...")
    # Build display-oriented thread groups/metadata for message pages and full-thread pages.
    # This intentionally follows subject continuity plus time gaps rather than raw source
    # threading, because archive users typically want the topic on one page even when a
    # reply changed the subject line mid-thread.
    display_thread_groups: dict[str, list[dict]] = {}
    display_thread_epoch: dict[str, dict] = {}
    for r in rows:
        norm_subject = normalize_subject(r["subject"])
        if r.get("source") in {"cognigencorp", "phor"} and r.get("source_url"):
            base_key = r.get("thread_key") or f"src:{r['source_url']}"
        elif norm_subject == "(no subject)":
            base_key = r.get("thread_key") or f"msg-{r['message_number'] or r['msg_seq']}"
        else:
            base_key = norm_subject or f"msg-{r['message_number'] or r['msg_seq']}"

        if r["date"] is None:
            r["display_thread_key"] = base_key
            display_thread_groups.setdefault(base_key, []).append(r)
            continue

        epoch = display_thread_epoch.get(base_key)
        if epoch is None:
            epoch = {"last_date": r["date"], "seq": 0}
            display_thread_epoch[base_key] = epoch
        else:
            gap = (r["date"] - epoch["last_date"]).days
            if gap > 60:
                epoch["seq"] += 1
            epoch["last_date"] = r["date"]
        display_key = f"{base_key}#{epoch['seq']}" if epoch["seq"] > 0 else base_key
        r["display_thread_key"] = display_key
        display_thread_groups.setdefault(display_key, []).append(r)

    thread_meta: dict[str, dict] = {}
    for tk, msgs in display_thread_groups.items():
        thread_msgs = order_thread_messages(msgs)
        if not thread_msgs:
            continue
        subject = display_thread_subject(thread_msgs[0]["subject"])
        last_dated = next((m for m in reversed(thread_msgs) if m["date"] is not None), thread_msgs[-1])
        thread_meta[tk] = {
            "subject": subject,
            "subject_lower": subject.lower(),
            "count": len(thread_msgs),
            "participants": len(set(m["from_name"] for m in thread_msgs)),
            "url": thread_page_url(thread_msgs),
            "messages": thread_msgs,
            "last_date_short": last_dated["date_short"],
            "last_date_sort": last_dated["date"].strftime("%Y-%m-%d") if last_dated["date"] else "",
        }

    # Build message_number → row lookup for computing reply depth
    msg_by_number: dict[int, dict] = {}
    for r in rows:
        mn = r.get("message_number")
        if mn is not None:
            msg_by_number[mn] = r

    def compute_depth(msg: dict, allowed_numbers: set[int] | None = None) -> int:
        """Follow in_reply_to chain to compute nesting depth."""
        depth = 0
        current = msg.get("in_reply_to_number")
        seen = set()
        while current is not None and current not in seen:
            seen.add(current)
            if allowed_numbers is not None and current not in allowed_numbers:
                break
            depth += 1
            parent = msg_by_number.get(current)
            if parent is None:
                break
            current = parent.get("in_reply_to_number")
        return depth

    # Build prev/next index across all generated message pages.
    page_rows = rows
    msg_template = env.get_template("message.html")
    thread_page_template = env.get_template("thread_page.html")
    for i, row in enumerate(page_rows):
        # Thread context with nesting depth
        tk = row.get("display_thread_key")
        thread_info = thread_meta.get(tk)
        thread_msgs = thread_info["messages"] if thread_info else []
        thread_context = None
        if len(thread_msgs) > 1:
            thread_msg_numbers = {
                t["message_number"] for t in thread_msgs if t.get("message_number") is not None
            }
            thread_context = [
                {
                    "date_short": t["date_short"],
                    "from_name": t["from_name"],
                    "subject": t["subject"],
                    "url": t["url"],
                    "depth": compute_depth(t, thread_msg_numbers),
                    "is_current": t["msg_seq"] == row["msg_seq"]
                    and t["year"] == row["year"]
                    and t["month"] == row["month"],
                }
                for t in thread_msgs
            ]

        prev_url = page_rows[i - 1]["url"] if i > 0 else None
        next_url = page_rows[i + 1]["url"] if i < len(page_rows) - 1 else None
        body_main, quoted_history = split_reply_history(row["body_clean"], row["source"])

        msg_html = msg_template.render(
            subject=row["subject"],
            from_name=row["from_name"],
            author_slug=row["author_slug"],
            date_long=row["date_long"],
            date_sort=row["date"].strftime("%Y-%m-%d") if row["date"] else "",
            category=row["category"],
            year=row["year"],
            month_num=f"{row['month']:02d}" if row["month"] is not None else None,
            month_name=MONTH_NAMES[row["month"]] if row["month"] is not None else None,
            body=clean_body(body_main),
            quoted_body=clean_body(quoted_history) if quoted_history else None,
            source=row["source"],
            source_url=row.get("source_url", ""),
            thread_messages=thread_context,
            thread_url=thread_info["url"] if thread_info and len(thread_msgs) > 1 else None,
            thread_count=thread_info["count"] if thread_info else 1,
            prev_url=prev_url,
            next_url=next_url,
        )

        msg_path = output_dir / row["url"].strip("/")
        msg_path.parent.mkdir(parents=True, exist_ok=True)
        msg_path.write_text(msg_html, encoding="utf-8")

    log.info(f"Generated {len(page_rows)} message pages")

    # --- Full thread pages ---
    log.info("Generating full thread pages...")
    thread_page_count = 0
    for meta in thread_meta.values():
        thread_messages = []
        for msg in meta["messages"]:
            body_main, quoted_history = split_reply_history(msg["body_clean"], msg["source"])
            thread_messages.append(
                {
                    "subject": msg["subject"],
                    "from_name": msg["from_name"],
                    "author_slug": msg["author_slug"],
                    "date_long": msg["date_long"],
                    "date_short": msg["date_short"],
                    "category": msg["category"],
                    "url": msg["url"],
                    "body": clean_body(body_main),
                    "quoted_body": clean_body(quoted_history) if quoted_history else None,
                }
            )

        thread_html = thread_page_template.render(
            thread_subject=meta["subject"],
            thread_count=meta["count"],
            participant_count=meta["participants"],
            last_date_short=meta["last_date_short"],
            messages=thread_messages,
        )

        thread_dir = output_dir / meta["url"].strip("/")
        thread_dir.mkdir(parents=True, exist_ok=True)
        (thread_dir / "index.html").write_text(thread_html, encoding="utf-8")
        thread_page_count += 1

    log.info(f"Generated {thread_page_count} full thread pages")

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
    author_list = []
    for slug, msgs in author_groups.items():
        dates = [m["date"] for m in msgs if m["date"]]
        first_year = min(d.year for d in dates) if dates else None
        last_year = max(d.year for d in dates) if dates else None
        author_list.append({
            "name": msgs[0]["from_name"],
            "slug": slug,
            "count": len(msgs),
            "first_year": first_year,
            "last_year": last_year,
        })
    author_list.sort(key=lambda a: a["name"].lower())
    max_count = max(a["count"] for a in author_list) if author_list else 1
    author_index_html = env.get_template("author_index.html").render(
        authors=author_list,
        max_count=max_count,
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

    # --- Threads index page ---
    log.info("Generating threads page...")
    thread_list = [
        {
            "subject": meta["subject"],
            "subject_lower": meta["subject_lower"],
            "count": meta["count"],
            "participants": meta["participants"],
            "thread_url": meta["url"],
            "last_date_short": meta["last_date_short"],
            "last_date_sort": meta["last_date_sort"],
        }
        for meta in thread_meta.values()
    ]
    thread_list.sort(key=lambda t: t["last_date_sort"], reverse=True)

    threads_dir = output_dir / "threads"
    threads_dir.mkdir(exist_ok=True)
    threads_html = env.get_template("threads.html").render(threads=thread_list)
    (threads_dir / "index.html").write_text(threads_html, encoding="utf-8")
    log.info(f"Generated threads page with {len(thread_list)} threads")

    # --- Search index data ---
    log.info("Exporting search data...")

    search_docs = []
    for r in rows:
        search_docs.append(
            {
                "subject": r["subject"],
                "from_name": r["from_name"],
                "body": r["body_clean"],
                "category": r["category"],
                "year": r["year"] if r["year"] is not None else 0,
                "date": r["date_short"],
                "url": r["url"],
            }
        )
    search_data_path = output_dir / "search-data.json"
    search_data_path.write_text(
        json.dumps(search_docs, separators=(",", ":"), ensure_ascii=False),
        encoding="utf-8",
    )
    size_mb = search_data_path.stat().st_size / 1024**2
    log.info(f"Exported {len(search_docs)} docs to {search_data_path} ({size_mb:.1f} MB)")

    log.info("Building Orama search index...")
    subprocess.run(
        ["node", str(SITE_DIR / "build-search.mjs"), "--output", str(output_dir)],
        check=True,
    )

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
