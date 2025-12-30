#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "fastparquet>=2023.4.0",
#   "pandas>=2.0.0",
#   "requests>=2.31.0",
#   "rich>=13.7.0",
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
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import fastparquet
import pandas as pd
import requests
from rich import box
from rich.console import Console, Group
from rich.live import Live
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants
DATA_DIR = Path("data")
SHARD_SIZE = 100000
SHARD_DIGITS = 9
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
SHARD_NAME_RE = re.compile(r"^hn-(\d+)-(\d+)\.parquet$")


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


def shard_bounds(item_id: int) -> tuple[int, int]:
    shard_index = (item_id - 1) // SHARD_SIZE
    start_id = shard_index * SHARD_SIZE + 1
    end_id = start_id + SHARD_SIZE - 1
    return start_id, end_id


def shard_name(start_id: int, end_id: int) -> str:
    return f"hn-{start_id:0{SHARD_DIGITS}d}-{end_id:0{SHARD_DIGITS}d}.parquet"


def shard_path_for_id(data_dir: Path, item_id: int) -> Path:
    start_id, end_id = shard_bounds(item_id)
    return data_dir / shard_name(start_id, end_id)


def list_shard_files(data_dir: Path) -> list[tuple[int, int, Path]]:
    if not data_dir.exists():
        return []
    shards: list[tuple[int, int, Path]] = []
    for path in sorted(data_dir.glob("hn-*.parquet")):
        match = SHARD_NAME_RE.match(path.name)
        if not match:
            logger.warning("Ignoring unexpected parquet file: %s", path)
            continue
        start_id = int(match.group(1))
        end_id = int(match.group(2))
        if start_id <= 0 or end_id < start_id:
            logger.warning("Ignoring shard with invalid range: %s", path)
            continue
        shards.append((start_id, end_id, path))
    shards.sort(key=lambda entry: entry[0])
    return shards


def normalize_ranges(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    if not ranges:
        return []
    ranges = sorted(ranges, key=lambda entry: entry[0])
    merged = [ranges[0]]
    for start_id, end_id in ranges[1:]:
        last_start, last_end = merged[-1]
        if start_id <= last_end + 1:
            merged[-1] = (last_start, max(last_end, end_id))
        else:
            merged.append((start_id, end_id))
    return merged


def scan_existing_ids(
    data_dir: Path,
) -> tuple[int, Optional[bytearray], list[tuple[int, int]], list[Path]]:
    shards = list_shard_files(data_dir)
    legacy_path = data_dir / "hn.parquet"
    if not shards:
        if legacy_path.exists():
            logger.warning("Legacy file found (not used): %s", legacy_path)
        logger.info("No existing shard files found")
        return 0, None, [], []

    seen = bytearray(1)
    max_id = 0
    corrupted_ranges: list[tuple[int, int]] = []
    corrupted_paths: list[Path] = []

    for start_id, end_id, path in shards:
        try:
            parquet = fastparquet.ParquetFile(str(path))
        except Exception as e:
            logger.error("Error opening shard %s: %s", path, e)
            corrupted_ranges.append((start_id, end_id))
            corrupted_paths.append(path)
            continue

        local_ids: list[int] = []
        local_max = 0
        try:
            for batch in parquet.iter_row_groups(columns=["item_id"]):
                if batch.empty:
                    continue
                ids = batch["item_id"].tolist()
                if not ids:
                    continue
                local_ids.extend(ids)
                row_max = max(ids)
                if row_max > local_max:
                    local_max = row_max
        except Exception as e:
            logger.error("Error scanning shard %s: %s", path, e)
            corrupted_ranges.append((start_id, end_id))
            corrupted_paths.append(path)
            continue

        if local_ids:
            if local_max >= len(seen):
                new_size = max(local_max + 1, len(seen) * 2)
                seen.extend(b"\x00" * (new_size - len(seen)))
            for item_id in local_ids:
                if item_id < 0:
                    continue
                if item_id >= len(seen):
                    seen.extend(b"\x00" * (item_id + 1 - len(seen)))
                seen[item_id] = 1
            if local_max > max_id:
                max_id = local_max

    if max_id + 1 < len(seen):
        seen = seen[: max_id + 1]

    return max_id, seen, normalize_ranges(corrupted_ranges), corrupted_paths


def find_missing_ids(
    seen: Optional[bytearray],
    max_id: int,
    skip_ranges: list[tuple[int, int]],
) -> list[int]:
    if seen is None or max_id <= 0:
        return []
    limit = min(max_id, len(seen) - 1)
    skip_ranges = normalize_ranges(skip_ranges)
    missing: list[int] = []
    range_index = 0
    current_range = skip_ranges[0] if skip_ranges else None
    for item_id in range(1, limit + 1):
        while current_range and item_id > current_range[1]:
            range_index += 1
            current_range = (
                skip_ranges[range_index] if range_index < len(skip_ranges) else None
            )
        if current_range and current_range[0] <= item_id <= current_range[1]:
            continue
        if seen[item_id] == 0:
            missing.append(item_id)
    return missing


def count_range_excluding(
    start_id: int,
    end_id: int,
    skip_ranges: list[tuple[int, int]],
) -> int:
    if start_id > end_id:
        return 0
    total = end_id - start_id + 1
    for range_start, range_end in normalize_ranges(skip_ranges):
        overlap_start = max(start_id, range_start)
        overlap_end = min(end_id, range_end)
        if overlap_start <= overlap_end:
            total -= overlap_end - overlap_start + 1
    return max(0, total)


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
    data_dir: Path,
    write_queue: queue.Queue,
    stats: Stats,
    stats_lock: threading.Lock,
) -> None:
    shard_buffers: dict[Path, tuple[list[int], list[str]]] = {}

    def flush_shard(shard_path: Path, item_ids: list[int], item_jsons: list[str]) -> None:
        if not item_ids:
            return
        try:
            write_batch(shard_path, item_ids, item_jsons)
            add_stats(stats, stats_lock, persisted=len(item_ids))
        except Exception as e:
            add_stats(stats, stats_lock, write_errors=len(item_ids))
            logger.error("Error writing shard %s: %s", shard_path, e)

    def flush_all() -> None:
        nonlocal shard_buffers
        for shard_path, (item_ids, item_jsons) in list(shard_buffers.items()):
            if not item_ids:
                continue
            flush_shard(shard_path, item_ids, item_jsons)
            shard_buffers[shard_path] = ([], [])

    while True:
        try:
            item = write_queue.get(timeout=WRITE_FLUSH_SECONDS)
        except queue.Empty:
            flush_all()
            continue

        if item is None:
            write_queue.task_done()
            break

        item_id, item_json = item
        shard_path = shard_path_for_id(data_dir, item_id)
        if shard_path not in shard_buffers:
            shard_buffers[shard_path] = ([], [])
        shard_ids, shard_jsons = shard_buffers[shard_path]
        shard_ids.append(item_id)
        shard_jsons.append(item_json)
        write_queue.task_done()

        if len(shard_ids) >= BATCH_SIZE:
            flush_shard(shard_path, shard_ids, shard_jsons)
            shard_buffers[shard_path] = ([], [])

    flush_all()


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


def enqueue_range_excluding(
    start_id: int,
    end_id: int,
    item_queue: queue.Queue,
    stop_event: threading.Event,
    skip_ranges: list[tuple[int, int]],
) -> bool:
    if start_id > end_id:
        return True
    skip_ranges = normalize_ranges(skip_ranges)
    if not skip_ranges:
        return enqueue_range(start_id, end_id, item_queue, stop_event)
    current = start_id
    for range_start, range_end in skip_ranges:
        if range_end < current:
            continue
        if range_start > end_id:
            break
        if range_start > current:
            if not enqueue_range(
                current,
                min(range_start - 1, end_id),
                item_queue,
                stop_event,
            ):
                return False
        current = max(current, range_end + 1)
        if current > end_id:
            break
    if current <= end_id:
        return enqueue_range(current, end_id, item_queue, stop_event)
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

    console = Console()
    progress = Progress(
        SpinnerColumn(style="cyan"),
        TextColumn("[bold cyan]{task.description}"),
        BarColumn(bar_width=None, complete_style="green"),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TextColumn(" | "),
        TimeElapsedColumn(),
        TextColumn("eta"),
        TimeRemainingColumn(),
        console=console,
    )
    task_id = progress.add_task("Downloading", total=total_target)

    snapshot = snapshot_stats(stats, stats_lock)
    elapsed = time.monotonic() - snapshot.start_time
    rate = snapshot.persisted / elapsed if elapsed > 0 else 0.0
    progress.update(
        task_id,
        completed=snapshot.persisted,
        description="Stopping" if stop_event.is_set() else "Downloading",
    )
    status_table = build_status_table(
        snapshot,
        rate,
        item_queue,
        write_queue,
        stop_event.is_set(),
    )
    with Live(
        Group(progress, status_table),
        console=console,
        refresh_per_second=10,
    ) as live:
        try:
            while not done_event.is_set():
                snapshot = snapshot_stats(stats, stats_lock)
                elapsed = time.monotonic() - snapshot.start_time
                rate = snapshot.persisted / elapsed if elapsed > 0 else 0.0
                progress.update(
                    task_id,
                    completed=snapshot.persisted,
                    description="Stopping"
                    if stop_event.is_set()
                    else "Downloading",
                )
                status_table = build_status_table(
                    snapshot,
                    rate,
                    item_queue,
                    write_queue,
                    stop_event.is_set(),
                )
                live.update(Group(progress, status_table), refresh=True)
                time.sleep(REPORT_INTERVAL)
        finally:
            snapshot = snapshot_stats(stats, stats_lock)
            elapsed = time.monotonic() - snapshot.start_time
            rate = snapshot.persisted / elapsed if elapsed > 0 else 0.0
            progress.update(
                task_id,
                completed=snapshot.persisted,
                description="Done",
            )
            status_table = build_status_table(
                snapshot,
                rate,
                item_queue,
                write_queue,
                stop_event.is_set(),
            )
            live.update(Group(progress, status_table), refresh=True)


def build_status_table(
    snapshot: Stats,
    rate: float,
    item_queue: queue.Queue,
    write_queue: queue.Queue,
    stopping: bool,
) -> Table:
    status = "[yellow]stopping[/]" if stopping else "[green]running[/]"
    fetch_err = (
        f"[red]{snapshot.fetch_errors:,}[/]" if snapshot.fetch_errors else "0"
    )
    write_err = (
        f"[red]{snapshot.write_errors:,}[/]" if snapshot.write_errors else "0"
    )
    table = Table(box=box.SIMPLE, show_header=False, pad_edge=False)
    table.add_column("metric", style="dim", no_wrap=True)
    table.add_column("value", justify="right")
    table.add_column("metric", style="dim", no_wrap=True)
    table.add_column("value", justify="right")
    table.add_row("status", status, "rate", f"{rate:,.1f}/s")
    table.add_row(
        "persisted",
        f"{snapshot.persisted:,}",
        "fetched",
        f"{snapshot.fetched:,}",
    )
    table.add_row(
        "skipped",
        f"{snapshot.skipped:,}",
        "retries",
        f"{snapshot.retries:,}",
    )
    table.add_row("fetch err", fetch_err, "write err", write_err)
    table.add_row(
        "id queue",
        f"{item_queue.qsize():,}",
        "write queue",
        f"{write_queue.qsize():,}",
    )
    return table


def main() -> None:
    start_time = time.time()
    DATA_DIR.mkdir(parents=True, exist_ok=True)

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

    last_id, seen, corrupted_ranges, corrupted_paths = scan_existing_ids(DATA_DIR)

    with requests.Session() as session:
        max_id = get_max_id(session)

    scan_limit = min(last_id, max_id)
    missing_ids = find_missing_ids(seen, scan_limit, corrupted_ranges)
    missing_count = len(missing_ids)
    new_range_start = last_id + 1
    new_count = count_range_excluding(new_range_start, max_id, corrupted_ranges)
    items_to_download = missing_count + new_count

    if last_id > 0:
        logger.info("Highest local ID: %s", f"{last_id:,}")
        logger.info("Max item ID: %s", f"{max_id:,}")
        if corrupted_paths:
            logger.error(
                "Corrupted shard(s) detected (delete to rebuild): %s",
                ", ".join(str(path) for path in corrupted_paths),
            )
        if missing_count:
            logger.info("Missing IDs to backfill: %s", f"{missing_count:,}")
        logger.info("Items to download: %s", f"{items_to_download:,}")
    else:
        logger.info("Starting fresh download of %s items", f"{max_id:,}")
        if corrupted_paths:
            logger.error(
                "Corrupted shard(s) detected (delete to rebuild): %s",
                ", ".join(str(path) for path in corrupted_paths),
            )

    if items_to_download <= 0:
        logger.info("No new items to download")
        return

    item_queue: queue.Queue = queue.Queue(maxsize=QUEUE_SIZE)
    write_queue: queue.Queue = queue.Queue(maxsize=WRITE_QUEUE_SIZE)

    writer_thread = threading.Thread(
        target=writer_loop,
        args=(DATA_DIR, write_queue, stats, stats_lock),
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
            enqueue_range_excluding(
                last_id + 1,
                max_id,
                item_queue,
                stop_event,
                corrupted_ranges,
            )
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
