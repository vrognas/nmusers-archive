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
        "url": "cognigencorp.com/nonmem/nm/*",
        "filter": re.compile(r"/nonmem/nm/\d{2}\w{3}\d+\.html$"),
        "output_dir": "data/raw_cognigencorp",
    },
    "pipermail": {
        "url": "cognigen.com/nmusers/*",
        "filter": re.compile(r"/nmusers/\d{4}-\w+/\d+\.html$"),
        "output_dir": "data/raw_cognigen_pipermail",
    },
}


def discover_urls(source: str) -> list[dict]:
    """Query the Wayback CDX API to find all archived message URLs."""
    config = CDX_QUERIES[source]
    log.info(f"Querying CDX API for {source} archive...")

    params = {
        "url": config["url"],
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
        if config["filter"].search(original_url):
            results.append({"timestamp": timestamp, "url": original_url})

    log.info(f"Found {len(results)} message pages in {source} archive")
    return results


def url_to_filename(url: str, source: str) -> str:
    """Convert a URL to a safe local filename."""
    if source == "old":
        # cognigencorp.com/nonmem/nm/99apr242002.html → 99apr242002.html
        match = re.search(r"(\d{2}\w{3}\d+\.html)$", url)
        return match.group(1) if match else url.split("/")[-1]
    else:
        # cognigen.com/nmusers/2006-December/0015.html → 2006-December_0015.html
        match = re.search(r"(\d{4}-\w+)/(\d+\.html)$", url)
        if match:
            return f"{match.group(1)}_{match.group(2)}"
        return url.split("/")[-1]


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
    entries = json.loads(manifest_path.read_text())

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

    sources = ["old", "pipermail"] if args.source == "all" else [args.source]

    for source in sources:
        entries = discover_urls(source)
        manifest_path = manifest_dir / f"{source}.json"
        manifest_path.write_text(json.dumps(entries, indent=2))
        log.info(f"Saved manifest: {manifest_path} ({len(entries)} entries)")


def cmd_download(args):
    """Download pages from saved manifests."""
    sources = ["old", "pipermail"] if args.source == "all" else [args.source]

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
        "--source", choices=["old", "pipermail", "all"], default="all",
    )

    # download subcommand
    download_parser = subparsers.add_parser(
        "download", help="Download pages from saved manifests"
    )
    download_parser.add_argument(
        "--source", choices=["old", "pipermail", "all"], default="all",
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
