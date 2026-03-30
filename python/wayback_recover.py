"""
Recover NMusers messages from the Wayback Machine.

Downloads archived Cognigen pages in both formats:
  1. Old format: cognigencorp.com/nonmem/nm/*.html (1995–2006)
  2. Pipermail format: cognigen.com/nmusers/YYYY-Month/NNN.html (2006–2021)

Uses the Wayback Machine CDX API to discover URLs, then fetches
the archived snapshots.

Usage:
    python python/wayback_recover.py discover                           # Find all URLs
    python python/wayback_recover.py discover --source old              # Only pre-2007
    python python/wayback_recover.py download --source old --workers 3  # Download pre-2007
    python python/wayback_recover.py download                           # Download all
"""

import argparse
import asyncio
import logging
import re
from pathlib import Path
from urllib.parse import urljoin

import httpx

WAYBACK_CDX = "https://web.archive.org/cdx/search/cdx"
WAYBACK_RAW = "https://web.archive.org/web/{timestamp}id_/{url}"
USER_AGENT = "nmusers-archive/0.1 (https://github.com/vrognas/nmusers-archive)"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("wayback")

# CDX queries for each source
CDX_QUERIES = {
    "old": {
        "urls": ["cognigencorp.com/nonmem/nm/*"],
        "filter": re.compile(r"/nonmem/nm/\d{2}\w{3}\d+\.html$"),
        "output_dir": "data/raw_cognigencorp",
    },
    "pipermail": {
        "urls": ["cognigen.com/nmusers/*"],
        "filter": re.compile(r"/nmusers/\d{4}-\w+/\d+\.html$"),
        "output_dir": "data/raw_cognigen_pipermail",
    },
    "phor": {
        "urls": [
            "www.phor.com/nonmem/nm/*",
            "phor.com/nonmem/nm/*",
        ],
        "filter": re.compile(r"/nonmem/nm/\d{2}\w{3}\d+\.html$"),
        "output_dir": "data/raw_phor",
        "index_pages": [
            {
                "wayback_url": "https://web.archive.org/web/20071015145237/http://phor.com/nonmem/nm/archpage.html",
                "base_url": "http://www.phor.com/nonmem/nm/",
                "link_pattern": re.compile(r'href="([0-9]{2}[a-z]{3}\d{1,2}\d{4}\.html)"', re.IGNORECASE),
            },
        ],
    },
    "phor_nmo": {
        "urls": [
            "www.phor.com/nonmem/nmo/*",
            "phor.com/nonmem/nmo/*",
        ],
        "filter": re.compile(r"/nonmem/nmo/topic\d{3}\.html$", re.IGNORECASE),
        "output_dir": "data/raw_phor_nmo",
        "index_pages": [
            {
                "wayback_url": "https://web.archive.org/web/20070418102623/http://www.phor.com/nonmem/nmo/index.html",
                "base_url": "http://www.phor.com/nonmem/nmo/",
                "link_pattern": re.compile(r'href="(topic\d{3}\.html)"', re.IGNORECASE),
            },
        ],
    },
}


def _query_cdx(url_pattern: str, filter_pattern: re.Pattern[str]) -> list[dict]:
    """Query the Wayback CDX API for one URL pattern."""
    log.info(f"Querying CDX API for {url_pattern}...")

    params = {
        "url": url_pattern,
        "output": "text",
        "fl": "timestamp,original",
        "collapse": "urlkey",  # One snapshot per unique URL
        "limit": "10000",
    }

    with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=120) as client:
        response = client.get(WAYBACK_CDX, params=params)
        response.raise_for_status()

    results = []
    for line in response.text.strip().split("\n"):
        if not line.strip():
            continue
        parts = line.split(" ", 1)
        if len(parts) != 2:
            continue
        timestamp, original_url = parts
        if filter_pattern.search(original_url):
            results.append({"timestamp": timestamp, "url": original_url})

    return results


def _extract_wayback_timestamp(html: str, fallback: str) -> str:
    """Prefer the actual replayed capture timestamp if Wayback redirected us."""
    match = re.search(r'__wm\.wombat\(".*?","(\d+)"', html)
    return match.group(1) if match else fallback


def _harvest_index_links(index_config: dict) -> list[dict]:
    """Fetch one archived index page and turn its relative links into manifest entries."""
    wayback_url = index_config["wayback_url"]
    requested_timestamp_match = re.search(r"/web/(\d+)/", wayback_url)
    requested_timestamp = requested_timestamp_match.group(1) if requested_timestamp_match else ""

    log.info(f"Harvesting links from {wayback_url}...")
    with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=120, follow_redirects=True) as client:
        response = client.get(wayback_url)
        response.raise_for_status()

    replay_timestamp = _extract_wayback_timestamp(response.text, requested_timestamp)
    entries = []
    seen_urls: set[str] = set()
    for href in index_config["link_pattern"].findall(response.text):
        original_url = urljoin(index_config["base_url"], href)
        if original_url in seen_urls:
            continue
        seen_urls.add(original_url)
        entries.append({"timestamp": replay_timestamp, "url": original_url})

    log.info(f"Harvested {len(entries)} links from {wayback_url}")
    return entries


def _dedupe_entries(entries: list[dict]) -> list[dict]:
    """Keep one entry per original URL, preferring the most recent timestamp."""
    by_url: dict[str, dict] = {}
    for entry in entries:
        existing = by_url.get(entry["url"])
        if existing is None or entry["timestamp"] > existing["timestamp"]:
            by_url[entry["url"]] = entry
    return [by_url[url] for url in sorted(by_url)]


def discover_urls(source: str) -> list[dict]:
    """Query the Wayback CDX API and archived indexes to find message URLs."""
    config = CDX_QUERIES[source]
    entries: list[dict] = []

    for url_pattern in config.get("urls", []):
        entries.extend(_query_cdx(url_pattern, config["filter"]))

    for index_config in config.get("index_pages", []):
        entries.extend(_harvest_index_links(index_config))

    deduped = _dedupe_entries(
        [entry for entry in entries if config["filter"].search(entry["url"])]
    )
    by_filename = _dedupe_entries_by_filename(deduped, source)
    log.info(
        f"Found {len(by_filename)} message pages in {source} archive"
        + (f" ({len(deduped) - len(by_filename)} host/timestamp variants collapsed)" if len(by_filename) != len(deduped) else "")
    )
    return by_filename


def url_to_filename(url: str, source: str) -> str:
    """Convert a URL to a safe local filename."""
    if source in ("old", "phor"):
        # cognigencorp.com/nonmem/nm/99apr242002.html → 99apr242002.html
        match = re.search(r"(\d{2}\w{3}\d+\.html)$", url)
        return match.group(1) if match else url.split("/")[-1]
    elif source == "phor_nmo":
        match = re.search(r"(topic\d{3}\.html)$", url, re.IGNORECASE)
        return match.group(1) if match else url.split("/")[-1]
    else:
        # cognigen.com/nmusers/2006-December/0015.html → 2006-December_0015.html
        match = re.search(r"(\d{4}-\w+)/(\d+\.html)$", url)
        if match:
            return f"{match.group(1)}_{match.group(2)}"
        return url.split("/")[-1]


def _dedupe_entries_by_filename(entries: list[dict], source: str) -> list[dict]:
    """Keep only the newest snapshot for each local target filename."""
    by_filename: dict[str, dict] = {}
    for entry in entries:
        filename = url_to_filename(entry["url"], source)
        current = by_filename.get(filename)
        if current is None or entry["timestamp"] > current["timestamp"]:
            by_filename[filename] = entry
    return [by_filename[name] for name in sorted(by_filename)]


async def download_snapshot(
    client: httpx.AsyncClient,
    entry: dict,
    output_dir: Path,
    source: str,
    semaphore: asyncio.Semaphore,
    max_retries: int = 5,
) -> dict:
    """Download a single Wayback Machine snapshot with retry on 429."""
    filename = url_to_filename(entry["url"], source)
    filepath = output_dir / filename

    if filepath.exists() and filepath.stat().st_size > 0:
        return {"url": entry["url"], "status": "cached"}

    # Use id_ prefix to get the original page without Wayback toolbar
    wayback_url = f"https://web.archive.org/web/{entry['timestamp']}id_/{entry['url']}"

    async with semaphore:
        for attempt in range(max_retries):
            try:
                response = await client.get(wayback_url)
            except httpx.HTTPError as exc:
                log.warning(f"{filename} FAILED: {exc}")
                return {"url": entry["url"], "status": "error"}

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 0))
                backoff = max(retry_after, 2 ** (attempt + 1))
                if attempt < max_retries - 1:
                    log.debug(f"{filename} 429, retry in {backoff}s (attempt {attempt + 1})")
                    await asyncio.sleep(backoff)
                    continue
                else:
                    log.warning(f"{filename} 429 after {max_retries} retries")
                    return {"url": entry["url"], "status": "error"}

            if response.status_code != 200:
                log.warning(f"{filename} HTTP {response.status_code}")
                return {"url": entry["url"], "status": "error"}

            filepath.write_bytes(response.content)
            await asyncio.sleep(1.5)  # Be polite to the Wayback Machine
            return {"url": entry["url"], "status": "downloaded"}

    return {"url": entry["url"], "status": "error"}


async def download_all(source: str, max_workers: int = 2) -> list[dict]:
    """Download all archived pages for a source from saved manifest."""
    config = CDX_QUERIES[source]
    output_dir = Path(config["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = Path(f"data/manifests/{source}.json")
    if not manifest_path.exists():
        log.error(f"Manifest not found: {manifest_path}. Run 'discover' first.")
        return []

    import json
    entries = _dedupe_entries_by_filename(json.loads(manifest_path.read_text()), source)

    if not entries:
        log.warning(f"No URLs in manifest for {source}")
        return []

    semaphore = asyncio.Semaphore(max_workers)
    total = len(entries)

    log.info(f"Downloading {total} pages to {output_dir} ({max_workers} workers)")

    results: list[dict] = []
    downloaded = 0
    cached = 0
    failed = 0

    async with httpx.AsyncClient(
        headers={"User-Agent": USER_AGENT},
        timeout=30,
        follow_redirects=True,
    ) as client:
        # Process in batches to avoid overwhelming the server
        batch_size = 50
        for i in range(0, total, batch_size):
            batch = entries[i : i + batch_size]
            tasks = [
                download_snapshot(client, entry, output_dir, source, semaphore)
                for entry in batch
            ]
            batch_results = await asyncio.gather(*tasks)
            results.extend(batch_results)

            batch_dl = sum(1 for r in batch_results if r["status"] == "downloaded")
            batch_cached = sum(1 for r in batch_results if r["status"] == "cached")
            batch_failed = sum(1 for r in batch_results if r["status"] == "error")
            downloaded += batch_dl
            cached += batch_cached
            failed += batch_failed

            done = i + len(batch)
            log.info(
                f"Progress: {done}/{total} "
                f"({downloaded} new, {cached} cached, {failed} failed)"
            )

    log.info(f"Done: {downloaded} downloaded, {cached} cached, {failed} failed")
    return results


def cmd_discover(args):
    """Query CDX API and save URL manifests."""
    import json

    manifest_dir = Path("data/manifests")
    manifest_dir.mkdir(parents=True, exist_ok=True)

    sources = ["old", "pipermail", "phor", "phor_nmo"] if args.source == "all" else [args.source]

    for source in sources:
        entries = discover_urls(source)
        manifest_path = manifest_dir / f"{source}.json"
        manifest_path.write_text(json.dumps(entries, indent=2))
        log.info(f"Saved manifest: {manifest_path} ({len(entries)} entries)")


def cmd_download(args):
    """Download pages from saved manifests."""
    sources = ["old", "pipermail", "phor", "phor_nmo"] if args.source == "all" else [args.source]

    for source in sources:
        asyncio.run(download_all(source, args.workers))


def main():
    parser = argparse.ArgumentParser(description="Recover NMusers from Wayback Machine")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # discover subcommand
    discover_parser = subparsers.add_parser(
        "discover", help="Query CDX API and save URL manifests"
    )
    discover_parser.add_argument(
        "--source", choices=["old", "pipermail", "phor", "phor_nmo", "all"], default="all",
    )

    # download subcommand
    download_parser = subparsers.add_parser(
        "download", help="Download pages from saved manifests"
    )
    download_parser.add_argument(
        "--source", choices=["old", "pipermail", "phor", "phor_nmo", "all"], default="all",
    )
    download_parser.add_argument(
        "--workers", type=int, default=2, help="Max concurrent requests",
    )

    args = parser.parse_args()

    if args.command == "discover":
        cmd_discover(args)
    elif args.command == "download":
        cmd_download(args)


if __name__ == "__main__":
    main()
