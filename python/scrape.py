"""
Scrape NMusers message pages from mail-archive.com.

Downloads raw HTML files with polite async concurrency.
Resumes gracefully — skips already-downloaded files.

By default the scraper starts at the message number immediately after
the highest one already in data/messages.parquet, so daily runs only
fetch the few new messages. Pass --start 0 for a full re-scrape.

Usage:
    python python/scrape.py                    # Incremental from last parquet
    python python/scrape.py --start 0          # Full archive
    python python/scrape.py --start 0 --end 9  # First 10 messages
    python python/scrape.py --workers 3         # 3 concurrent requests
"""

import argparse
import asyncio
import logging
from pathlib import Path

import httpx
import polars as pl
from bs4 import BeautifulSoup

BASE_URL = "https://www.mail-archive.com/nmusers@globomaxnm.com"
USER_AGENT = "nmusers-archive/0.1 (https://github.com/vrognas/nmusers-archive)"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scrape")


def discover_max_message_id() -> int:
    """Fetch the date-sorted index and find the highest message number."""
    log.info("Discovering latest message ID...")
    url = f"{BASE_URL}/maillist.html"

    with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=30) as client:
        response = client.get(url)
        response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    message_ids = []
    for anchor in soup.select("a[href^='msg']"):
        href = anchor.get("href", "")
        if href.startswith("msg") and href.endswith(".html"):
            try:
                message_ids.append(int(href[3:-5]))
            except ValueError:
                continue

    max_id = max(message_ids)
    log.info(f"Latest message: msg{max_id:05d}")
    return max_id


async def download_message(
    client: httpx.AsyncClient,
    message_number: int,
    output_dir: Path,
    semaphore: asyncio.Semaphore,
    max_retries: int = 5,
) -> dict:
    """Download a single message page, respecting concurrency limits."""
    filename = f"msg{message_number:05d}.html"
    filepath = output_dir / filename

    if filepath.exists():
        return {"id": message_number, "status": "cached"}

    async with semaphore:
        url = f"{BASE_URL}/{filename}"
        for attempt in range(max_retries):
            try:
                response = await client.get(url)
            except httpx.HTTPError as exc:
                log.warning(f"msg{message_number:05d} FAILED: {exc}")
                return {"id": message_number, "status": "error"}

            if response.status_code == 404:
                return {"id": message_number, "status": "not_found"}

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 0))
                backoff = max(retry_after, 2 ** (attempt + 1))
                if attempt < max_retries - 1:
                    log.debug(f"msg{message_number:05d} 429, retry in {backoff}s")
                    await asyncio.sleep(backoff)
                    continue
                else:
                    log.warning(f"msg{message_number:05d} 429 after {max_retries} retries")
                    return {"id": message_number, "status": "error"}

            if response.status_code != 200:
                log.warning(f"msg{message_number:05d} HTTP {response.status_code}")
                return {"id": message_number, "status": "error"}

            filepath.write_bytes(response.content)
            await asyncio.sleep(0.5)
            return {"id": message_number, "status": "downloaded"}

    return {"id": message_number, "status": "error"}


async def scrape(
    start_id: int,
    end_id: int,
    output_dir: Path,
    max_workers: int = 5,
) -> list[dict]:
    """Download a range of messages with bounded concurrency."""
    output_dir.mkdir(parents=True, exist_ok=True)
    semaphore = asyncio.Semaphore(max_workers)
    total = end_id - start_id + 1

    log.info(f"Scraping msg{start_id:05d}–msg{end_id:05d} ({total} messages, {max_workers} workers)")

    results: list[dict] = []
    downloaded = 0
    cached = 0
    failed = 0

    async with httpx.AsyncClient(
        headers={"User-Agent": USER_AGENT},
        timeout=30,
        follow_redirects=True,
    ) as client:
        all_ids = list(range(start_id, end_id + 1))
        batch_size = 100
        for i in range(0, len(all_ids), batch_size):
            batch_ids = all_ids[i : i + batch_size]
            tasks = [
                download_message(client, msg_id, output_dir, semaphore)
                for msg_id in batch_ids
            ]
            batch_results = await asyncio.gather(*tasks)
            results.extend(batch_results)

            batch_dl = sum(1 for r in batch_results if r["status"] == "downloaded")
            batch_cached = sum(1 for r in batch_results if r["status"] == "cached")
            batch_failed = sum(1 for r in batch_results if r["status"] in ("error", "not_found"))
            downloaded += batch_dl
            cached += batch_cached
            failed += batch_failed

            done = i + len(batch_ids)
            log.info(
                f"Progress: {done}/{total} "
                f"({downloaded} new, {cached} cached, {failed} failed)"
            )

    log.info(f"Done: {downloaded} downloaded, {cached} cached, {failed} failed")
    return results


def resolve_start_id(parquet_path: Path) -> int:
    """Return the next message ID to scrape based on the existing parquet.

    Falls back to 0 (full archive) when no parquet is available.
    """
    if not parquet_path.exists():
        log.info(f"No existing parquet at {parquet_path}, starting from msg00000")
        return 0
    max_num = pl.read_parquet(parquet_path, columns=["message_number"])["message_number"].max()
    if max_num is None:
        log.info(f"{parquet_path} is empty, starting from msg00000")
        return 0
    next_id = int(max_num) + 1
    log.info(f"Existing parquet ends at msg{int(max_num):05d}, resuming at msg{next_id:05d}")
    return next_id


def main():
    parser = argparse.ArgumentParser(description="Scrape NMusers archive")
    parser.add_argument(
        "--start",
        type=int,
        default=None,
        help="First message ID (default: derived from --parquet, or 0 if absent)",
    )
    parser.add_argument("--end", type=int, default=None, help="Last message ID (auto-discovered if omitted)")
    parser.add_argument("--output", type=str, default="data/raw", help="Output directory for HTML files")
    parser.add_argument(
        "--parquet",
        type=str,
        default="data/messages.parquet",
        help="Existing parquet used to derive --start when not given",
    )
    parser.add_argument("--workers", type=int, default=5, help="Max concurrent requests")
    args = parser.parse_args()

    start_id = args.start if args.start is not None else resolve_start_id(Path(args.parquet))
    end_id = args.end if args.end is not None else discover_max_message_id()

    if start_id > end_id:
        log.info(f"Nothing to scrape: start={start_id} > end={end_id}")
        return

    asyncio.run(scrape(start_id, end_id, Path(args.output), args.workers))


if __name__ == "__main__":
    main()
