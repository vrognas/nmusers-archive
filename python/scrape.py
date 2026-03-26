"""
Scrape NMusers message pages from mail-archive.com.

Downloads raw HTML files with polite async concurrency.
Resumes gracefully — skips already-downloaded files.

Usage:
    python python/scrape.py                    # Full archive
    python python/scrape.py --start 0 --end 9  # First 10 messages
    python python/scrape.py --workers 3         # 3 concurrent requests
"""

import argparse
import asyncio
import logging
from pathlib import Path

import httpx
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
) -> dict:
    """Download a single message page, respecting concurrency limits."""
    filename = f"msg{message_number:05d}.html"
    filepath = output_dir / filename

    if filepath.exists():
        return {"id": message_number, "status": "cached"}

    async with semaphore:
        url = f"{BASE_URL}/{filename}"
        try:
            response = await client.get(url)
        except httpx.HTTPError as exc:
            log.warning(f"msg{message_number:05d} FAILED: {exc}")
            return {"id": message_number, "status": "error"}

        if response.status_code == 404:
            return {"id": message_number, "status": "not_found"}

        if response.status_code != 200:
            log.warning(f"msg{message_number:05d} HTTP {response.status_code}")
            return {"id": message_number, "status": "error"}

        filepath.write_text(response.text, encoding="utf-8")

        # Polite delay after each actual download
        await asyncio.sleep(0.5)

        return {"id": message_number, "status": "downloaded"}


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

    async with httpx.AsyncClient(
        headers={"User-Agent": USER_AGENT},
        timeout=30,
        follow_redirects=True,
    ) as client:
        tasks = [
            download_message(client, msg_id, output_dir, semaphore)
            for msg_id in range(start_id, end_id + 1)
        ]
        results = await asyncio.gather(*tasks)

    downloaded = sum(1 for r in results if r["status"] == "downloaded")
    cached = sum(1 for r in results if r["status"] == "cached")
    failed = sum(1 for r in results if r["status"] in ("error", "not_found"))

    log.info(f"Done: {downloaded} downloaded, {cached} cached, {failed} failed")
    return results


def main():
    parser = argparse.ArgumentParser(description="Scrape NMusers archive")
    parser.add_argument("--start", type=int, default=0, help="First message ID")
    parser.add_argument("--end", type=int, default=None, help="Last message ID (auto-discovered if omitted)")
    parser.add_argument("--output", type=str, default="data/raw", help="Output directory for HTML files")
    parser.add_argument("--workers", type=int, default=5, help="Max concurrent requests")
    args = parser.parse_args()

    end_id = args.end if args.end is not None else discover_max_message_id()
    asyncio.run(scrape(args.start, end_id, Path(args.output), args.workers))


if __name__ == "__main__":
    main()
