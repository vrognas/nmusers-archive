# CLAUDE.md

## Project Overview

This project scrapes, parses, and archives the NMusers mailing list — the primary discussion forum for NONMEM (pharmacometrics software) users. It combines three data sources spanning 1995–present into a single deduplicated Parquet dataset.

The archive preserves historically significant pharmacometrics content, including contributions from NONMEM's creators (Lewis Sheiner, Stuart Beal).

## Architecture

**Python** handles scraping and parsing (one-time data acquisition). **R** handles analysis (ongoing use). **Parquet** bridges the two.

```
python/scrape.py            → Async scraper for mail-archive.com (2007–present)
python/parse.py             → Parser for mail-archive.com HTML → Parquet
python/wayback_recover.py   → Wayback Machine CDX discovery + download (two subcommands: discover, download)
python/parse_cognigen.py    → Parser for both Cognigen HTML formats → Parquet
python/merge.py             → Deduplicate and merge all sources → messages_all.parquet
R/nmusers.R                 → Read + query helpers (arrow + dplyr)
```

### Data Sources

| Source | Coverage | Dir |
|--------|----------|-----|
| mail-archive.com | 2007–present, ~9,100 msgs | `data/raw/` |
| cognigen.com pipermail (Wayback) | 2006–2021, ~4,700 msgs | `data/raw_cognigen_pipermail/` |
| cognigencorp.com old format (Wayback) | 1995–2006, ~1,100 msgs | `data/raw_cognigencorp/` |

## Tech Stack

- **Python ≥ 3.12**, managed with **uv** (not pip)
- `httpx` for async HTTP, `beautifulsoup4` for HTML parsing, `polars` for DataFrames, `pyarrow` for Parquet I/O
- **R** with `arrow`, `dplyr`, `cli` for analysis

## Commands

```bash
uv sync                                                              # Install deps
uv run python python/wayback_recover.py discover                     # Find Wayback URLs, save manifests
uv run python python/wayback_recover.py download --source old        # Download pre-2007 pages
uv run python python/wayback_recover.py download --source pipermail  # Download pipermail pages
uv run python python/parse_cognigen.py                               # Parse Cognigen HTML → Parquet
uv run python python/scrape.py                                       # Scrape mail-archive.com
uv run python python/scrape.py --start 0 --end 9                    # Scrape a small range
uv run python python/parse.py                                        # Parse mail-archive HTML → Parquet
uv run python python/merge.py                                        # Merge + deduplicate all sources
```

## Code Style

- Python: type hints, f-strings, `async`/`await` for HTTP, `logging` (not print)
- No `requirements.txt` — dependencies live in `pyproject.toml`
- Run scripts with `uv run python python/<script>.py`, not bare `python`
- Add new dependencies with `uv add <package>`

## Known Issues / TODOs

- **Thread reconstruction in `parse.py`**: The `_extract_thread_parent()` function tries to extract the parent message from the `tSliceList` thread tree in mail-archive.com HTML, but the CSS selector `":scope > span.subject a"` doesn't match correctly. Needs fixing — either debug the selector against actual HTML nesting, or fall back to subject-based threading (strip `Re:`/`RE:` prefixes, group by normalized subject).
- **Old-format parser (`parse_cognigen.py`)**: The cognigencorp.com pages bundle multiple messages per page (a full thread). The `From:`/`Subject:`/`Date:` regex splitting works but needs tuning against real downloaded pages — the HTML structure varies across years (1995–2006).
- **Deduplication in `merge.py`**: Uses subject + from_name + date (rounded to minute) for fuzzy matching. May need refinement once all sources are merged.
- **Content classification**: `classify_subject()` in both parsers uses regex on subject lines. Could be improved with body-text analysis.

## File Conventions

- Raw HTML goes in `data/raw*/` directories (gitignored, reproducible)
- Wayback URL manifests go in `data/manifests/` (gitignored)
- Parquet output goes in `data/` (committed for the final merged dataset)
- All Python scripts have `main()` entry points and `argparse` CLIs
