"""
Merge and deduplicate NMusers messages from all three sources.

Combines:
  1. cognigencorp.com (1995-2006)
  2. cognigen.com/nmusers (2006-2021)
  3. mail-archive.com (2007-present)

Deduplication uses normalized subject + date + author.

Usage:
    python python/merge.py
"""

import logging
import re
from pathlib import Path

import polars as pl

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("merge")

DATA_DIR = Path("data")

SOURCE_FILES = {
    "mail_archive": DATA_DIR / "messages.parquet",
    "cognigencorp": DATA_DIR / "cognigencorp_messages.parquet",
    "pipermail": DATA_DIR / "cognigen_pipermail_messages.parquet",
}

SHARED_COLUMNS = ["date", "from_name", "subject", "category", "body_clean", "source"]


def load_source(name: str, path: Path) -> pl.DataFrame | None:
    if not path.exists():
        log.warning(f"{name}: {path} not found, skipping")
        return None

    df = pl.read_parquet(path)
    log.info(f"{name}: {len(df)} rows")

    if "source" not in df.columns:
        df = df.with_columns(pl.lit(name).alias("source"))

    for col in SHARED_COLUMNS:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))

    return df.select(SHARED_COLUMNS)


def normalize_subject(subject: str | None) -> str:
    """Strip Re:/FW: prefixes and [NMusers] tag for dedup matching."""
    if subject is None:
        return ""
    cleaned = subject.strip().replace("[NMusers]", "").strip()
    while True:
        new = cleaned
        for prefix in ["Re:", "RE:", "Fwd:", "FW:", "Fw:"]:
            if new.startswith(prefix):
                new = new[len(prefix):].strip()
        if new == cleaned:
            break
        cleaned = new
    return cleaned.lower().strip()


def deduplicate(df: pl.DataFrame) -> pl.DataFrame:
    """Remove duplicates. Priority: mail_archive > pipermail > cognigencorp."""
    source_priority = {"mail_archive": 0, "pipermail": 1, "cognigencorp": 2}

    df = df.with_columns(
        pl.col("source")
        .replace_strict(source_priority, default=3)
        .alias("_priority"),
        pl.col("subject")
        .map_elements(normalize_subject, return_dtype=pl.Utf8)
        .alias("_norm_subject"),
        pl.col("date").cast(pl.Date).alias("_date_day"),
    )

    deduped = (
        df.sort("_priority")
        .unique(subset=["_norm_subject", "_date_day", "from_name"], keep="first")
        .drop("_priority", "_norm_subject", "_date_day")
    )

    removed = len(df) - len(deduped)
    log.info(f"Deduplicated: {len(df)} -> {len(deduped)} ({removed} duplicates removed)")
    return deduped.sort("date")


def main():
    frames = []
    for name, path in SOURCE_FILES.items():
        df = load_source(name, path)
        if df is not None:
            frames.append(df)

    if not frames:
        log.error("No source files found. Run scrapers/parsers first.")
        return

    combined = pl.concat(frames)
    log.info(f"Combined: {len(combined)} total rows")

    merged = deduplicate(combined)

    for label, col in [("source", "source"), ("category", "category")]:
        breakdown = merged.group_by(col).len().sort("len", descending=True)
        log.info(f"By {label}:")
        for row in breakdown.iter_rows(named=True):
            log.info(f"  {row[col]:20s} {row['len']:>6d}")

    dates = merged.filter(pl.col("date").is_not_null())["date"]
    if len(dates) > 0:
        log.info(f"Date range: {dates.min()} - {dates.max()}")

    output_path = DATA_DIR / "messages_all.parquet"
    merged.write_parquet(output_path)
    size_mb = output_path.stat().st_size / 1024**2
    log.info(f"Wrote {output_path} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
