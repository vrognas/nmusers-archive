# NMusers Mailing List Archive

A structured, searchable archive of the [NMusers mailing list](https://www.mail-archive.com/nmusers@globomaxnm.com/) — the primary discussion forum for [NONMEM](https://www.iconplc.com/innovation/nonmem/) users in pharmacometrics.

Inspired by the [R Mailing List Archives](https://r-mailing-lists.thecoatlessprofessor.com/) project.

## About NMusers

The NONMEM Users Network (NMusers) is maintained by ICON Clinical Research LLC and has been active since at least 1995. It covers NONMEM usage, pharmacokinetic/pharmacodynamic modeling, and pharmacometrics in general.

The archive includes contributions from NONMEM's creators (Lewis Sheiner, Stuart Beal) and generations of pharmacometricians.

## Data Sources

| Source | Coverage | Messages | Method |
|--------|----------|----------|--------|
| [mail-archive.com](https://www.mail-archive.com/nmusers@globomaxnm.com/) | 2007–present | ~9,100+ | Live scraping |
| [cognigen.com/nmusers](https://www.cognigen.com/nmusers/) | 2006–2021 | ~4,700 | Wayback Machine |
| [cognigencorp.com/nonmem/nm](https://www.cognigencorp.com/nonmem/nm/) | **1995–2006** | ~1,100 | Wayback Machine |

After deduplication the combined archive contains an estimated **~11,000–12,000 unique messages spanning 31 years**.

## Quick Start

```bash
uv sync
```

### Step 1: Recover historical archives (Wayback Machine)

```bash
# Discover archived URLs
uv run python python/wayback_recover.py discover

# Download pre-2007 archive first (most at-risk, ~1,071 pages)
uv run python python/wayback_recover.py download --source old --workers 3

# Then the pipermail archive (~4,690 pages)
uv run python python/wayback_recover.py download --source pipermail --workers 3

# Parse both into Parquet
uv run python python/parse_cognigen.py
```

### Step 2: Scrape current archive (mail-archive.com)

```bash
# ~30 min with 5 workers
uv run python python/scrape.py
uv run python python/parse.py
```

### Step 3: Merge and deduplicate

```bash
uv run python python/merge.py
# Output: data/messages_all.parquet
```

### Step 4: Analyse in R

```r
source("R/nmusers.R")

messages <- nmusers_read("data/messages_all.parquet")

# Technical Q&A only (no job ads, workshops)
technical <- nmusers_technical(messages)

# Longest discussion threads
nmusers_thread_summary(messages)

# Top contributors across 31 years
nmusers_top_contributors(messages)

# Activity over time
nmusers_monthly_volume(messages)
```

## Architecture

```
Wayback Machine                        Live site
┌────────────────────┐                ┌──────────────────┐
│ cognigencorp.com   │                │ mail-archive.com │
│ 1995–2006 (1,100)  │                │ 2007–2026 (9,100)│
├────────────────────┤                └────────┬─────────┘
│ cognigen.com       │                         │
│ 2006–2021 (4,700)  │                         │
└────────┬───────────┘                         │
         │                                     │
    wayback_recover.py                    scrape.py
    parse_cognigen.py                     parse.py
         │                                     │
         ▼                                     ▼
    .parquet files ──────► merge.py ◄──── .parquet file
                              │
                              ▼
                     messages_all.parquet
                              │
                     ┌────────┴────────┐
                     │  R (analysis)   │
                     │  Python (RAG)   │
                     └─────────────────┘
```

## Repository Structure

```
nmusers-archive/
├── python/
│   ├── scrape.py            # Async scraper for mail-archive.com
│   ├── parse.py             # Parser for mail-archive.com HTML
│   ├── wayback_recover.py   # Wayback Machine URL discovery + download
│   ├── parse_cognigen.py    # Parser for both Cognigen HTML formats
│   └── merge.py             # Deduplicate and merge all sources
├── R/
│   └── nmusers.R            # Read + query helpers (arrow + dplyr)
├── data/
│   ├── raw/                 # mail-archive.com HTML (gitignored)
│   ├── raw_cognigencorp/    # Wayback old format HTML (gitignored)
│   ├── raw_cognigen_pipermail/ # Wayback pipermail HTML (gitignored)
│   ├── manifests/           # CDX URL lists (gitignored)
│   └── messages_all.parquet # Final merged output
├── pyproject.toml
├── .gitignore
├── LICENSE
└── README.md
```

## Privacy

Email addresses are not stored. Display names are retained as they appear in the public archive.

## License

Code: MIT. The mailing list content is publicly archived and belongs to its respective authors.
