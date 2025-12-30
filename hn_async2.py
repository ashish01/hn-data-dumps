#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "fastparquet>=2023.4.0",
#   "pandas>=2.0.0",
#   "requests>=2.31.0",
# ]
# ///
"""
Hacker News Data Dumper - downloads and stores all Hacker News items.
"""
import json
import logging
import queue
import re
import signal
import shutil
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import fastparquet
import pandas as pd
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants
DB_PATH = Path("data/hn.parquet")
API_BASE_URL = "https://hacker-news.firebaseio.com/v0"
WORKER_COUNT = 100
REQUEST_TIMEOUT = 10  # seconds
RETRY_ATTEMPTS = 3
RETRY_DELAY = 1  # seconds
QUEUE_SIZE = 10000
WRITE_QUEUE_SIZE = WORKER_COUNT * 2
BATCH_SIZE = 1000
WRITE_FLUSH_SECONDS = 0.5
REPORT_INTERVAL = 1.0

COLOR_RESET = "\x1b[0m"
COLOR_DIM = "\x1b[2m"
COLOR_RED = "\x1b[31m"
COLOR_GREEN = "\x1b[32m"
COLOR_YELLOW = "\x1b[33m"
COLOR_CYAN = "\x1b[36m"
COLOR_BOLD = "\x1b[1m"

ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")


@dataclass
class Stats:
    start_time: float = field(default_factory=time.monotonic)
    fetched: int = 0
    persisted: int = 0
    skipped: int = 0
    fetch_errors: int = 0
    write_errors: int = 0
    retries: int = 0


@dataclass
class FetchResult:
    item_json: Optional[str]
    retries: int
    not_found: bool = False


def add_stats(stats: Stats, lock: threading.Lock, **updates: int) -> None:
    with lock:
        for key, value in updates.items():
            setattr(stats, key, getattr(stats, key) + value)


def snapshot_stats(stats: Stats, lock: threading.Lock) -> Stats:
    with lock:
        return Stats(
            start_time=stats.start_time,
            fetched=stats.fetched,
            persisted=stats.persisted,
            skipped=stats.skipped,
            fetch_errors=stats.fetch_errors,
            write_errors=stats.write_errors,
            retries=stats.retries,
        )


def format_duration(seconds: float) -> str:
    total = int(seconds)
    mins, secs = divmod(total, 60)
    hours, mins = divmod(mins, 60)
    if hours > 0:
        return f"{hours:02d}:{mins:02d}:{secs:02d}"
    return f"{mins:02d}:{secs:02d}"


def format_rate(count: int, elapsed: float) -> str:
    if elapsed <= 0:
        return "0.0/s"
    return f"{count / elapsed:.1f}/s"


def format_int(value: int) -> str:
    return f"{value:,}"


def truncate_ansi_line(line: str, max_width: int) -> str:
    if max_width <= 0:
        return ""
    visible = 0
    i = 0
    out: list[str] = []
    while i < len(line) and visible < max_width:
        if line[i] == "\x1b":
            match = ANSI_ESCAPE_RE.match(line, i)
            if match:
                out.append(match.group(0))
                i = match.end()
                continue
        out.append(line[i])
        visible += 1
        i += 1
    if i < len(line):
        out.append(COLOR_RESET)
    return "".join(out)


def render_dashboard(
    snapshot: Stats,
    total_target: int,
    item_queue: queue.Queue,
    write_queue: queue.Queue,
    stop_event: threading.Event,
) -> list[str]:
    elapsed = time.monotonic() - snapshot.start_time
    percent = (snapshot.persisted / total_target * 100) if total_target else 100.0
    bar_width = 28
    filled = int(bar_width * min(max(percent, 0.0), 100.0) / 100.0)
    bar = (
        f"{COLOR_GREEN}{'=' * filled}"
        f"{COLOR_DIM}{'.' * (bar_width - filled)}{COLOR_RESET}"
    )

    header = (
        f"{COLOR_BOLD}HN dump{COLOR_RESET} "
        f"{COLOR_CYAN}{percent:5.1f}%{COLOR_RESET} "
        f"[{bar}] "
        f"{format_int(snapshot.persisted)}/{format_int(total_target)}"
    )

    stats_line = (
        f"{COLOR_GREEN}persisted {format_int(snapshot.persisted)}{COLOR_RESET} | "
        f"{COLOR_CYAN}fetched {format_int(snapshot.fetched)}{COLOR_RESET} | "
        f"{COLOR_YELLOW}skipped {format_int(snapshot.skipped)}{COLOR_RESET} | "
        f"{COLOR_RED}fetch_err {format_int(snapshot.fetch_errors)}{COLOR_RESET} | "
        f"{COLOR_RED}write_err {format_int(snapshot.write_errors)}{COLOR_RESET}"
    )

    status = (
        f"rate {format_rate(snapshot.persisted, elapsed)} | "
        f"retries {format_int(snapshot.retries)} | "
        f"queues ids {item_queue.qsize():>5} write {write_queue.qsize():>4} | "
        f"elapsed {format_duration(elapsed)}"
    )
    if stop_event.is_set():
        status += f" | {COLOR_YELLOW}stopping...{COLOR_RESET}"

    return [header, stats_line, status]


def scan_existing_ids(db_path: Path) -> tuple[int, Optional[bytearray]]:
    if not db_path.exists():
        logger.info("No existing parquet file found")
        return 0, None

    try:
        parquet = fastparquet.ParquetFile(str(db_path))
    except Exception as e:
        logger.error(f"Error opening parquet file {db_path}: {e}")
        return 0, None

    seen = bytearray(1)
    max_id = 0

    try:
        for batch in parquet.iter_row_groups(columns=["item_id"]):
            if batch.empty:
                continue
            ids = batch["item_id"].tolist()
            if not ids:
                continue
            row_max = max(ids)
            if row_max >= len(seen):
                new_size = max(row_max + 1, len(seen) * 2)
                seen.extend(b"\x00" * (new_size - len(seen)))
            for item_id in ids:
                if item_id < 0:
                    continue
                if item_id >= len(seen):
                    seen.extend(b"\x00" * (item_id + 1 - len(seen)))
                seen[item_id] = 1
            if row_max > max_id:
                max_id = row_max
    except Exception as e:
        logger.error(f"Error scanning parquet file {db_path}: {e}")
        return 0, None

    if max_id + 1 < len(seen):
        seen = seen[: max_id + 1]

    return max_id, seen


def find_missing_ids(seen: Optional[bytearray], max_id: int) -> list[int]:
    if seen is None or max_id <= 0:
        return []
    limit = min(max_id, len(seen) - 1)
    return [item_id for item_id in range(1, limit + 1) if seen[item_id] == 0]


def get_max_id(session: requests.Session) -> int:
    max_id_url = f"{API_BASE_URL}/maxitem.json"

    for attempt in range(RETRY_ATTEMPTS):
        try:
            response = session.get(max_id_url, timeout=REQUEST_TIMEOUT)
            if response.status_code != 200:
                logger.warning(
                    "Got status %s from API, retrying...",
                    response.status_code,
                )
                time.sleep(RETRY_DELAY * (attempt + 1))
                continue

            max_id = json.loads(response.text)
            if not isinstance(max_id, int) or max_id <= 0:
                raise ValueError(f"Invalid max ID returned from API: {max_id}")

            return max_id
        except (requests.RequestException, ValueError) as e:
            logger.warning("Error getting max ID (attempt %s): %s", attempt + 1, e)
        time.sleep(RETRY_DELAY * (attempt + 1))

    raise RuntimeError("Failed to get max ID after multiple attempts")


def fetch_item(session: requests.Session, item_id: int) -> FetchResult:
    url = f"{API_BASE_URL}/item/{item_id}.json"

    for attempt in range(RETRY_ATTEMPTS):
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT)
            if response.status_code == 404:
                return FetchResult("null", attempt, True)
            if response.status_code != 200:
                logger.debug(
                    "Got status %s for item %s, retrying...",
                    response.status_code,
                    item_id,
                )
                time.sleep(RETRY_DELAY * (attempt + 1))
                continue

            return FetchResult(response.text, attempt, False)
        except requests.RequestException as e:
            logger.debug(
                "Error fetching item %s (attempt %s): %s",
                item_id,
                attempt + 1,
                e,
            )
        time.sleep(RETRY_DELAY * (attempt + 1))

    logger.warning("Failed to fetch item %s after %s attempts", item_id, RETRY_ATTEMPTS)
    return FetchResult(None, RETRY_ATTEMPTS, False)


def write_batch(db_path: Path, item_ids: list[int], item_jsons: list[str]) -> None:
    if not item_ids:
        return
    frame = pd.DataFrame({"item_id": item_ids, "item_json": item_jsons})
    fastparquet.write(str(db_path), frame, compression="ZSTD", append=db_path.exists())


def writer_loop(
    db_path: Path,
    write_queue: queue.Queue,
    stats: Stats,
    stats_lock: threading.Lock,
) -> None:
    batch_ids: list[int] = []
    batch_jsons: list[str] = []

    def flush_batch() -> None:
        nonlocal batch_ids, batch_jsons
        if not batch_ids:
            return
        try:
            write_batch(db_path, batch_ids, batch_jsons)
            add_stats(stats, stats_lock, persisted=len(batch_ids))
        except Exception as e:
            add_stats(stats, stats_lock, write_errors=len(batch_ids))
            logger.error("Error writing batch ending at %s: %s", batch_ids[-1], e)
        batch_ids = []
        batch_jsons = []

    while True:
        try:
            item = write_queue.get(timeout=WRITE_FLUSH_SECONDS)
        except queue.Empty:
            flush_batch()
            continue

        if item is None:
            write_queue.task_done()
            break

        item_id, item_json = item
        batch_ids.append(item_id)
        batch_jsons.append(item_json)
        write_queue.task_done()

        if len(batch_ids) >= BATCH_SIZE:
            flush_batch()

    flush_batch()


def worker_loop(
    worker_id: int,
    item_queue: queue.Queue,
    write_queue: queue.Queue,
    stats: Stats,
    stats_lock: threading.Lock,
    stop_event: threading.Event,
) -> None:
    session = requests.Session()
    while True:
        try:
            item_id = item_queue.get(timeout=0.5)
        except queue.Empty:
            if stop_event.is_set():
                break
            continue

        if item_id is None:
            item_queue.task_done()
            break

        if stop_event.is_set():
            item_queue.task_done()
            break

        result = fetch_item(session, item_id)
        add_stats(stats, stats_lock, retries=result.retries)

        if result.item_json is None:
            add_stats(stats, stats_lock, fetch_errors=1)
        else:
            if result.not_found:
                add_stats(stats, stats_lock, skipped=1)
            add_stats(stats, stats_lock, fetched=1)
            write_queue.put((item_id, result.item_json))

        item_queue.task_done()

        if stop_event.is_set():
            break

    session.close()
    logger.debug("Worker %s exiting", worker_id)


def enqueue_ids(
    ids: list[int],
    item_queue: queue.Queue,
    stop_event: threading.Event,
) -> bool:
    for item_id in ids:
        while True:
            if stop_event.is_set():
                return False
            try:
                item_queue.put(item_id, timeout=0.5)
                break
            except queue.Full:
                continue
    return True


def enqueue_range(
    start_id: int,
    end_id: int,
    item_queue: queue.Queue,
    stop_event: threading.Event,
) -> bool:
    for item_id in range(start_id, end_id + 1):
        while True:
            if stop_event.is_set():
                return False
            try:
                item_queue.put(item_id, timeout=0.5)
                break
            except queue.Full:
                continue
    return True


def reporter_loop(
    stats: Stats,
    stats_lock: threading.Lock,
    item_queue: queue.Queue,
    write_queue: queue.Queue,
    total_target: int,
    stop_event: threading.Event,
    done_event: threading.Event,
) -> None:
    if not sys.stdout.isatty():
        while not done_event.is_set():
            snapshot = snapshot_stats(stats, stats_lock)
            elapsed = time.monotonic() - snapshot.start_time
            rate = snapshot.persisted / elapsed if elapsed > 0 else 0.0
            logger.info(
                "progress persisted=%s/%s fetched=%s skipped=%s fetch_errors=%s "
                "write_errors=%s retries=%s rate=%.1f/s queues ids=%s write=%s",
                snapshot.persisted,
                total_target,
                snapshot.fetched,
                snapshot.skipped,
                snapshot.fetch_errors,
                snapshot.write_errors,
                snapshot.retries,
                rate,
                item_queue.qsize(),
                write_queue.qsize(),
            )
            time.sleep(REPORT_INTERVAL)
        return

    line_count = 3
    cursor_up = max(line_count - 1, 0)
    rendered = False
    sys.stdout.write("\x1b[?25l")
    sys.stdout.flush()

    try:
        while not done_event.is_set():
            snapshot = snapshot_stats(stats, stats_lock)
            lines = render_dashboard(
                snapshot, total_target, item_queue, write_queue, stop_event
            )
            terminal_width = shutil.get_terminal_size((100, 20)).columns
            max_width = max(1, terminal_width - 1)
            lines = [truncate_ansi_line(line, max_width) for line in lines]

            if rendered and cursor_up:
                sys.stdout.write(f"\x1b[{cursor_up}A")
            for idx, line in enumerate(lines):
                sys.stdout.write("\r\x1b[2K" + line)
                if idx < len(lines) - 1:
                    sys.stdout.write("\n")
            sys.stdout.flush()
            rendered = True
            time.sleep(REPORT_INTERVAL)
    finally:
        sys.stdout.write(COLOR_RESET)
        sys.stdout.write("\x1b[?25h")
        sys.stdout.write("\n")
        sys.stdout.flush()


def main() -> None:
    start_time = time.time()
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    stop_event = threading.Event()
    report_done = threading.Event()

    def handle_stop(signum: int, frame) -> None:
        if not stop_event.is_set():
            logger.info("Shutdown requested, stopping intake...")
            stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, handle_stop)
        except ValueError:
            pass

    stats = Stats()
    stats_lock = threading.Lock()

    last_id, seen = scan_existing_ids(DB_PATH)

    with requests.Session() as session:
        max_id = get_max_id(session)

    scan_limit = min(last_id, max_id)
    missing_ids = find_missing_ids(seen, scan_limit)
    missing_count = len(missing_ids)
    items_to_download = max(0, max_id - last_id) + missing_count

    if last_id > 0:
        logger.info("Highest local ID: %s", f"{last_id:,}")
        logger.info("Max item ID: %s", f"{max_id:,}")
        if missing_count:
            logger.info("Missing IDs to backfill: %s", f"{missing_count:,}")
        logger.info("Items to download: %s", f"{items_to_download:,}")
    else:
        logger.info("Starting fresh download of %s items", f"{max_id:,}")

    if items_to_download <= 0:
        logger.info("No new items to download")
        return

    item_queue: queue.Queue = queue.Queue(maxsize=QUEUE_SIZE)
    write_queue: queue.Queue = queue.Queue(maxsize=WRITE_QUEUE_SIZE)

    writer_thread = threading.Thread(
        target=writer_loop,
        args=(DB_PATH, write_queue, stats, stats_lock),
        name="writer",
    )
    writer_thread.start()

    workers = []
    for worker_id in range(WORKER_COUNT):
        thread = threading.Thread(
            target=worker_loop,
            args=(
                worker_id,
                item_queue,
                write_queue,
                stats,
                stats_lock,
                stop_event,
            ),
            name=f"worker-{worker_id}",
        )
        thread.start()
        workers.append(thread)

    reporter_thread = threading.Thread(
        target=reporter_loop,
        args=(
            stats,
            stats_lock,
            item_queue,
            write_queue,
            items_to_download,
            stop_event,
            report_done,
        ),
        name="reporter",
        daemon=True,
    )
    reporter_thread.start()

    try:
        if enqueue_ids(missing_ids, item_queue, stop_event):
            enqueue_range(last_id + 1, max_id, item_queue, stop_event)
    finally:
        if stop_event.is_set():
            dropped = 0
            while True:
                try:
                    item_id = item_queue.get_nowait()
                except queue.Empty:
                    break
                else:
                    if item_id is not None:
                        dropped += 1
                    item_queue.task_done()
            if dropped:
                logger.info("Dropped %s queued IDs due to shutdown", dropped)

        for _ in range(WORKER_COUNT):
            item_queue.put(None)

        for thread in workers:
            thread.join()

        write_queue.put(None)
        writer_thread.join()

        report_done.set()

    elapsed = time.time() - start_time
    final_stats = snapshot_stats(stats, stats_lock)
    if items_to_download > 0:
        logger.info(
            "Downloaded %s/%s items in %.2f seconds",
            f"{final_stats.persisted:,}",
            f"{items_to_download:,}",
            elapsed,
        )
        if elapsed > 0:
            logger.info(
                "Average rate: %.2f items/second",
                final_stats.persisted / elapsed,
            )
    else:
        logger.info("No new items to download")


if __name__ == "__main__":
    try:
        main()
        logger.info("Finished successfully")
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.critical("Fatal error: %s", e, exc_info=True)
