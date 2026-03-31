# CLAUDE.md

## Project Overview

This project scrapes, parses, and archives the NMusers mailing list, a discussion forum for NONMEM (pharmacometrics software) users.
It combines four data sources spanning 1993–present into a single deduplicated Parquet dataset, and generates a static website at nmusers.vrognas.com.

The archive preserves historically significant pharmacometrics content, including contributions from NONMEM's creators (Lewis B. Sheiner, Stuart Beal).

## Architecture

**Python** handles scraping, parsing, and site generation. **R** handles analysis (ongoing use). **Parquet** bridges the two.

```
python/scrape.py            → Async scraper for mail-archive.com (2007–present)
python/parse.py             → Parser for mail-archive.com HTML → Parquet
python/wayback_recover.py   → Wayback Machine CDX discovery + download (four sources)
python/parse_cognigen.py    → Parser for Cognigen + phor.com HTML formats → Parquet
python/merge.py             → Deduplicate and merge all sources → messages_all.parquet
site/build.py               → Static site generator (Jinja2 + Polars + D3.js)
site/build-search.mjs       → Orama full-text search index builder
R/nmusers.R                 → Read + query helpers (arrow + dplyr)
```

### Data Sources

| Source | Coverage | Messages | Dir |
|--------|----------|----------|-----|
| mail-archive.com | 2007–present | ~8,300 | `data/raw/` |
| cognigencorp.com (Wayback) | 1993–2006 | ~3,600 | `data/raw_cognigencorp/` |
| cognigen.com pipermail (Wayback) | 2006–2021 | ~1,300 | `data/raw_cognigen_pipermail/` |
| phor.com (Wayback) | 1998–2004 | ~140 | `data/raw_phor/` |

After deduplication: **~13,400 unique messages** from ~2,300 contributors.

## Tech Stack

- **Python ≥ 3.12**, managed with **uv** (not pip)
- `httpx` for async HTTP, `beautifulsoup4` for HTML parsing, `polars` for DataFrames, `pyarrow` for Parquet I/O
- `jinja2` + `python-slugify` for site generation
- **Node.js** with `@orama/orama` for full-text search index
- **D3.js** (v7) for interactive stacked bar chart
- **R** with `arrow`, `dplyr`, `cli` for analysis
- **Netlify** for hosting

## Commands

```bash
uv sync                                                              # Install deps
uv run python python/wayback_recover.py discover                     # Find Wayback URLs, save manifests
uv run python python/wayback_recover.py download --source old        # Download pre-2007 pages
uv run python python/wayback_recover.py download --source pipermail  # Download pipermail pages
uv run python python/wayback_recover.py download --source phor       # Download phor.com pages
uv run python python/parse_cognigen.py                               # Parse Cognigen/phor HTML → Parquet
uv run python python/scrape.py                                       # Scrape mail-archive.com
uv run python python/parse.py                                        # Parse mail-archive HTML → Parquet
uv run python python/merge.py                                        # Merge + deduplicate all sources
uv run python site/build.py                                          # Build static site + search index
uv run python site/build.py --serve                                  # Build + serve locally
```

## Code Style

- Python: type hints, f-strings, `async`/`await` for HTTP, `logging` (not print)
- No `requirements.txt` — dependencies live in `pyproject.toml`
- Run scripts with `uv run python python/<script>.py`, not bare `python`
- Add new dependencies with `uv add <package>`

## Key Design Decisions

- **Thread reconstruction**: mail-archive messages use `in_reply_to_number` from the HTML thread tree (`tSliceList`).
Non-mail-archive sources use subject-based grouping with time-aware splitting (60-day gap → new thread).
Mail-archive threads (`ma:` prefix) are never time-split.
- **Author normalization**: `data/author_overrides.json` maps raw names to canonical forms.
Overrides are checked both before and after cleanup (title stripping, Last/First flipping, etc).
- **Category classification**: regex-based on subject lines. Five categories: technical (default), job, event, news, admin.
Order matters — admin checked first, then event, job, news.
- **Body parsing**: mail-archive uses mixed `<tt>` (flowed text) and `<pre>` (preformatted) blocks.
The parser reconstructs paragraphs from `<tt>` chains and preserves `<pre>` blocks.
Old-format (cognigencorp) pages bundle multiple messages per page, split on `****` separators.
- **Deduplication**: subject + from_name + date (rounded to day) + body signature.
Same-page follow-ups (multiple messages from same author on same day) are restored after dedup.

## File Conventions

- Raw HTML goes in `data/raw*/` directories (gitignored, reproducible)
- Wayback URL manifests go in `data/manifests/` (gitignored)
- Parquet output goes in `data/` (committed for the final merged dataset)
- Author overrides in `data/author_overrides.json` (committed)
- Static site output goes in `site/output/` (gitignored, built on Netlify)
- All Python scripts have `main()` entry points and `argparse` CLIs
