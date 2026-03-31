# NMusers Mailing List Archive

A structured, searchable archive of the [NMusers mailing list](https://www.mail-archive.com/nmusers@globomaxnm.com/), a discussion forum for [NONMEM](https://www.iconplc.com/innovation/nonmem/) users in pharmacometrics.

**Live site: [nmusers.vrognas.com](https://nmusers.vrognas.com)**

Inspired by the [R Mailing List Archives](https://r-mailing-lists.thecoatlessprofessor.com/) project.

## About NMusers

The NONMEM Users Network (NMusers) is maintained by ICON Clinical Research LLC and has been active since at least 1993.
It covers NONMEM usage, pharmacokinetic/pharmacodynamic modeling, and pharmacometrics in general.

The archive includes contributions from NONMEM's creators (Lewis B. Sheiner, Stuart Beal) and generations of pharmacometricians.

## Data Sources

| Source | Coverage | Messages | Method |
|--------|----------|----------|--------|
| [mail-archive.com](https://www.mail-archive.com/nmusers@globomaxnm.com/) | 2007–present | ~8,300 | Live scraping |
| [cognigencorp.com](https://web.archive.org/web/*/cognigencorp.com/nonmem/nm/*) | 1993–2006 | ~3,600 | Wayback Machine |
| [cognigen.com/nmusers](https://web.archive.org/web/*/cognigen.com/nmusers/*) | 2006–2021 | ~1,300 | Wayback Machine |
| [phor.com/nonmem](https://web.archive.org/web/*/www.phor.com/nonmem/nm/*) | 1998–2004 | ~140 | Wayback Machine |

After deduplication the combined archive contains **~13,400 unique messages spanning 33 years** from ~2,300 contributors.

## Web App

The archive is published as a static site at [nmusers.vrognas.com](https://nmusers.vrognas.com) with:

- Full-text search (Orama) with typo tolerance
- Interactive D3.js stacked bar chart (filterable by category and year)
- Thread reconstruction using mail-archive reply chains and subject-based grouping
- Five message categories: Technical, Job, Event, News, Admin
- Dark/light theme with Gruvbox-inspired color palette
- Keyboard shortcuts (j/k/o/f/s/?)
- Responsive design with mobile card layout

Built with Python (Jinja2 + Polars), hosted on Netlify.

## Quick Start

```bash
uv sync
```

### Step 1: Recover historical archives (Wayback Machine)

```bash
# Discover archived URLs (all sources)
uv run python python/wayback_recover.py discover

# Download each source
uv run python python/wayback_recover.py download --source old --workers 3
uv run python python/wayback_recover.py download --source pipermail --workers 3
uv run python python/wayback_recover.py download --source phor --workers 3

# Parse all into Parquet
uv run python python/parse_cognigen.py
```

### Step 2: Scrape current archive (mail-archive.com)

```bash
uv run python python/scrape.py
uv run python python/parse.py
```

### Step 3: Merge and deduplicate

```bash
uv run python python/merge.py
# Output: data/messages_all.parquet
```

### Step 4: Build the website

```bash
uv run python site/build.py
# Includes search index build via node site/build-search.mjs
# Output: site/output/
```

### Step 5: Analyse in R

```r
source("R/nmusers.R")

messages <- nmusers_read("data/messages_all.parquet")

# Technical Q&A only (no job ads, events)
technical <- nmusers_technical(messages)

# Longest discussion threads
nmusers_thread_summary(messages)

# Top contributors
nmusers_top_contributors(messages)

# Activity over time
nmusers_monthly_volume(messages)
```

## Architecture

```
Wayback Machine                        Live site
┌────────────────────┐                ┌──────────────────┐
│ cognigencorp.com   │                │ mail-archive.com │
│ 1993–2006 (3,600)  │                │ 2007–2026 (8,300)│
├────────────────────┤                └────────┬─────────┘
│ cognigen.com       │                         │
│ 2006–2021 (1,300)  │                    scrape.py
├────────────────────┤                    parse.py
│ phor.com           │                         │
│ 1998–2004 (140)    │                         │
└────────┬───────────┘                         │
         │                                     │
    wayback_recover.py                         │
    parse_cognigen.py                          │
         │                                     │
         ▼                                     ▼
    .parquet files ──────► merge.py ◄──── .parquet file
                              │
                              ▼
                     messages_all.parquet
                              │
                     ┌────────┴────────┐
                     │  site/build.py  │──► Static site (Netlify)
                     │  R (analysis)   │
                     └─────────────────┘
```

## Repository Structure

```
nmusers-archive/
├── python/
│   ├── scrape.py            # Async scraper for mail-archive.com
│   ├── parse.py             # Parser for mail-archive.com HTML
│   ├── wayback_recover.py   # Wayback Machine URL discovery + download
│   ├── parse_cognigen.py    # Parser for Cognigen + phor.com HTML formats
│   └── merge.py             # Deduplicate and merge all sources
├── site/
│   ├── build.py             # Static site generator (Jinja2 + Polars)
│   ├── build-search.mjs     # Orama search index builder
│   ├── templates/           # Jinja2 HTML templates
│   └── static/              # CSS, JS, favicon
├── R/
│   └── nmusers.R            # Read + query helpers (arrow + dplyr)
├── data/
│   ├── raw/                 # mail-archive.com HTML (gitignored)
│   ├── raw_cognigencorp/    # Wayback old format HTML (gitignored)
│   ├── raw_cognigen_pipermail/ # Wayback pipermail HTML (gitignored)
│   ├── raw_phor/            # Wayback phor.com HTML (gitignored)
│   ├── manifests/           # CDX URL lists (gitignored)
│   ├── author_overrides.json # Manual author name corrections
│   └── messages_all.parquet # Final merged output
├── netlify.toml             # Netlify build + deploy config
├── pyproject.toml
├── package.json             # Orama search dependencies
└── README.md
```

## Privacy

Email addresses are not stored.
Display names are retained as they appear in the public archive, with manual corrections for consistency.

## License

Code: MIT.
The mailing list content is publicly archived and belongs to its respective authors.
