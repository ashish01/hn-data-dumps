#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "aiohttp>=3.8.0",
#   "pyarrow>=12.0.0",
#   "fastparquet>=2023.4.0",
#   "pandas>=2.0.0",
# ]
# ///
"""
Hacker News Data Dumper - Efficiently downloads and stores all Hacker News items.
"""
import asyncio
import json
import logging
import os
import re
import shutil
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import aiohttp
import pyarrow.parquet as pq

from buffered_writer import BucketedBufferedParquetWriter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants
DB_NAME = "data/hn"
API_BASE_URL = "https://hacker-news.firebaseio.com/v0"
WORKER_COUNT = 100
REQUEST_TIMEOUT = 10  # seconds
RETRY_ATTEMPTS = 3
RETRY_DELAY = 1  # seconds
QUEUE_SIZE = 10000
WRITE_QUEUE_SIZE = WORKER_COUNT * 2
UI_REFRESH_SECONDS = 0.15
PERSIST_FLASH_SECONDS = 0.6
ERROR_FLASH_SECONDS = 1.0
UI_WORKERS_PER_ROW = 10

STATE_IDLE = "idle"
STATE_FETCHING = "fetch"
STATE_QUEUED = "queued"
STATE_SKIPPED = "skipped"
STATE_ERROR = "error"

COLOR_RESET = "\x1b[0m"
COLOR_DIM = "\x1b[2m"
COLOR_RED = "\x1b[31m"
COLOR_GREEN = "\x1b[32m"
COLOR_YELLOW = "\x1b[33m"
COLOR_CYAN = "\x1b[36m"
COLOR_BOLD = "\x1b[1m"

ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")

STATE_COLORS = {
    STATE_IDLE: COLOR_DIM,
    STATE_FETCHING: COLOR_YELLOW,
    STATE_QUEUED: COLOR_CYAN,
    STATE_SKIPPED: COLOR_YELLOW,
    STATE_ERROR: COLOR_RED,
}
STATE_SHORT_LABELS = {
    STATE_IDLE: "i",
    STATE_FETCHING: "f",
    STATE_QUEUED: "q",
    STATE_SKIPPED: "s",
    STATE_ERROR: "e",
}


@dataclass
class WorkerStatus:
    state: str = STATE_IDLE
    item_id: Optional[int] = None
    retries: int = 0
    last_latency_ms: float = 0.0
    last_persisted_id: Optional[int] = None
    last_persisted_at: float = 0.0
    last_error_at: float = 0.0
    downloaded: int = 0


@dataclass
class Metrics:
    start_time: float = field(default_factory=time.monotonic)
    completed: int = 0
    persisted: int = 0
    skipped: int = 0
    errors: int = 0
    retries: int = 0


def get_last_id(db_name: str) -> int:
    """
    Find the highest item ID that has been stored in parquet files.
    
    Args:
        db_name: Base name of the database files
        
    Returns:
        The highest item ID found, or 0 if no files exist
    """
    max_id = 0
    file_pattern = f"{db_name}*.parquet"
    
    try:
        files = list(Path().glob(file_pattern))
        if not files:
            logger.info(f"No existing files found with pattern {file_pattern}")
            return 0
            
        logger.info(f"Found {len(files)} database files")
        
        for filename in files:
            try:
                logger.debug(f"Reading metadata from {filename}")
                metadata = pq.read_metadata(str(filename))
                for row_grp in range(metadata.num_row_groups):
                    stats = metadata.row_group(row_grp).column(0).statistics
                    if stats is None or stats.max is None:
                        continue
                    if stats.max > max_id:
                        max_id = stats.max
            except Exception as e:
                logger.error(f"Error reading metadata from {filename}: {e}")
    except Exception as e:
        logger.error(f"Error finding last ID: {e}")
        
    return max_id


async def get_max_id(session: aiohttp.ClientSession) -> int:
    """
    Get the current maximum item ID from the Hacker News API.
    
    Args:
        session: The aiohttp ClientSession to use for the request
        
    Returns:
        The current maximum item ID
        
    Raises:
        ValueError: If the API response is invalid
    """
    max_id_url = f"{API_BASE_URL}/maxitem.json"
    
    for attempt in range(RETRY_ATTEMPTS):
        try:
            async with session.get(max_id_url, timeout=REQUEST_TIMEOUT) as response:
                if response.status != 200:
                    logger.warning(f"Got status {response.status} from API, retrying...")
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                    
                text = await response.text()
                max_id = json.loads(text)
                
                if not isinstance(max_id, int) or max_id <= 0:
                    raise ValueError(f"Invalid max ID returned from API: {max_id}")
                    
                return max_id
                
        except asyncio.TimeoutError:
            logger.warning(f"Timeout getting max ID (attempt {attempt+1})")
        except Exception as e:
            logger.warning(f"Error getting max ID (attempt {attempt+1}): {e}")
            
        # Wait before retrying
        await asyncio.sleep(RETRY_DELAY * (attempt + 1))
    
    # If we get here, all attempts failed
    raise RuntimeError("Failed to get max ID after multiple attempts")


async def db_writer_worker(
    db_name: str,
    input_queue: asyncio.Queue,
    buffer_size: int = 1000
) -> None:
    """
    Async worker that writes items to parquet files using a background thread.
    
    Args:
        db_name: Base name for the database files
        input_queue: Queue receiving (item_id, item_json, ack_future) tuples
        buffer_size: Number of items to buffer before writing
    """
    logger.info(f"Starting database writer with buffer size {buffer_size}")
    db = BucketedBufferedParquetWriter(db_name, buffer_size=buffer_size)
    try:
        while True:
            data = await input_queue.get()
            try:
                # None is the signal to exit
                if data is None:
                    logger.info("Received exit signal, closing database")
                    break

                item_id, item_json, ack = data
                ok = True
                try:
                    await asyncio.to_thread(db.add, item_id, item_json)
                except Exception as e:
                    ok = False
                    logger.error(f"Error writing item {item_id}: {e}")
                if not ack.done():
                    ack.set_result(ok)
            except Exception as e:
                logger.error(f"Error in database writer: {e}")
            finally:
                input_queue.task_done()
    except Exception as e:
        logger.critical(f"Fatal error in database writer: {e}")
    finally:
        await asyncio.to_thread(db.close)


async def fetch_item(
    session: aiohttp.ClientSession, 
    item_id: int,
    retry_attempts: int = RETRY_ATTEMPTS
) -> tuple[str, int]:
    """
    Fetch a single item from the Hacker News API.
    
    Args:
        session: The aiohttp ClientSession to use
        item_id: The ID of the item to fetch
        retry_attempts: Number of retry attempts for failed requests
        
    Returns:
        Tuple of JSON string (or "null") and retry count
    """
    url = f"{API_BASE_URL}/item/{item_id}.json"
    
    for attempt in range(retry_attempts):
        try:
            async with session.get(url, timeout=REQUEST_TIMEOUT) as response:
                if response.status == 404:
                    # Item doesn't exist
                    return "null", attempt
                elif response.status != 200:
                    logger.debug(f"Got status {response.status} for item {item_id}, retrying...")
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                
                return await response.text(), attempt
                
        except asyncio.TimeoutError:
            logger.debug(f"Timeout fetching item {item_id} (attempt {attempt+1})")
        except Exception as e:
            logger.debug(f"Error fetching item {item_id} (attempt {attempt+1}): {e}")
        
        # Wait before retrying
        await asyncio.sleep(RETRY_DELAY * (attempt + 1))
    
    # If all retries failed, return null to skip this item
    logger.warning(f"Failed to fetch item {item_id} after {retry_attempts} attempts")
    return "null", retry_attempts


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


def format_worker_line(index: int, status: WorkerStatus, now: float) -> str:
    flash_persist = now - status.last_persisted_at < PERSIST_FLASH_SECONDS
    flash_error = now - status.last_error_at < ERROR_FLASH_SECONDS

    if flash_error:
        color = COLOR_RED
        state_label = "error"
        item_id = status.item_id
    elif flash_persist:
        color = COLOR_GREEN
        state_label = "persisted"
        item_id = status.last_persisted_id
    else:
        color = STATE_COLORS.get(status.state, COLOR_RESET)
        state_label = status.state
        item_id = status.item_id

    item_str = f"{item_id}" if item_id is not None else "-"
    latency = f"{status.last_latency_ms:4.0f}ms" if status.last_latency_ms else "  --"
    return (
        f"{color}W{index:02d} {state_label:<9} "
        f"id={item_str:<9} r={status.retries:<2} {latency:>6}"
        f"{COLOR_RESET}"
    )


def format_worker_token(index: int, status: WorkerStatus, now: float) -> str:
    flash_persist = now - status.last_persisted_at < PERSIST_FLASH_SECONDS
    flash_error = now - status.last_error_at < ERROR_FLASH_SECONDS

    if flash_error:
        color = COLOR_RED
        state_label = "e"
        item_id = status.item_id
    elif flash_persist:
        color = COLOR_GREEN
        state_label = "p"
        item_id = status.last_persisted_id
    else:
        color = STATE_COLORS.get(status.state, COLOR_RESET)
        state_label = STATE_SHORT_LABELS.get(status.state, "?")
        item_id = status.item_id

    item_str = f"{item_id}" if item_id is not None else "-"
    return f"{color}{index:02d}{state_label}:{item_str:>9}{COLOR_RESET}"


def format_worker_group_summary(group: list[WorkerStatus]) -> str:
    counts = {
        STATE_IDLE: 0,
        STATE_FETCHING: 0,
        STATE_QUEUED: 0,
        STATE_SKIPPED: 0,
        STATE_ERROR: 0,
    }
    for status in group:
        counts[status.state] = counts.get(status.state, 0) + 1

    downloaded = sum(status.downloaded for status in group)
    return " ".join(
        [
            f"{COLOR_DIM}idle={counts[STATE_IDLE]:02d}{COLOR_RESET}",
            f"{COLOR_YELLOW}fetch={counts[STATE_FETCHING]:02d}{COLOR_RESET}",
            f"{COLOR_CYAN}queued={counts[STATE_QUEUED]:02d}{COLOR_RESET}",
            f"{COLOR_YELLOW}skipped={counts[STATE_SKIPPED]:02d}{COLOR_RESET}",
            f"{COLOR_RED}error={counts[STATE_ERROR]:02d}{COLOR_RESET}",
            f"{COLOR_GREEN}downloaded={format_int(downloaded)}{COLOR_RESET}",
        ]
    )


def get_worker_groups(
    statuses: list[WorkerStatus],
    group_size: int
) -> list[tuple[int, list[WorkerStatus]]]:
    total = len(statuses)
    if total == 0 or group_size <= 0:
        return []
    groups = []
    for start in range(0, total, group_size):
        end = min(start + group_size, total)
        groups.append((start, statuses[start:end]))
    return groups


def render_dashboard(
    statuses: list[WorkerStatus],
    metrics: Metrics,
    item_queue: asyncio.Queue,
    write_queue: asyncio.Queue,
    items_total: int,
    max_id: int
) -> list[str]:
    now = time.monotonic()
    elapsed = now - metrics.start_time
    completed = metrics.completed
    percent = (completed / items_total * 100) if items_total else 100.0
    busy = sum(1 for s in statuses if s.state != STATE_IDLE)

    header = (
        f"{COLOR_BOLD}HN dump{COLOR_RESET} "
        f"{COLOR_CYAN}{percent:5.1f}%{COLOR_RESET} "
        f"{format_int(completed)}/{format_int(items_total)} | "
        f"{COLOR_GREEN}persisted {format_int(metrics.persisted)}{COLOR_RESET} | "
        f"{COLOR_YELLOW}skipped {format_int(metrics.skipped)}{COLOR_RESET} | "
        f"{COLOR_RED}errors {format_int(metrics.errors)}{COLOR_RESET} | "
        f"{COLOR_YELLOW}retries {format_int(metrics.retries)}{COLOR_RESET} | "
        f"{COLOR_CYAN}rate {format_rate(completed, elapsed)}{COLOR_RESET} | "
        f"{COLOR_DIM}elapsed {format_duration(elapsed)}{COLOR_RESET}"
    )
    queues = (
        f"queues: ids {item_queue.qsize():>5} write {write_queue.qsize():>3} | "
        f"workers busy {busy}/{len(statuses)} | max_id {format_int(max_id)}"
    )

    lines: list[str] = []
    groups = get_worker_groups(statuses, UI_WORKERS_PER_ROW)
    for start, group in groups:
        end = start + len(group) - 1
        label = f"W{start:02d}-{end:02d}"
        summary = format_worker_group_summary(group)
        lines.append(f"{COLOR_BOLD}{label}{COLOR_RESET} {summary}")
    lines.append("")
    lines.append(header)
    lines.append(queues)
    return lines


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


async def ui_loop(
    statuses: list[WorkerStatus],
    metrics: Metrics,
    item_queue: asyncio.Queue,
    write_queue: asyncio.Queue,
    items_total: int,
    max_id: int,
    stop_event: asyncio.Event
) -> None:
    if not sys.stdout.isatty():
        return

    line_count = 3 + len(get_worker_groups(statuses, UI_WORKERS_PER_ROW))
    cursor_up = max(line_count - 1, 0)
    rendered = False
    sys.stdout.write("\x1b[?25l")
    sys.stdout.flush()

    try:
        while not stop_event.is_set():
            lines = render_dashboard(
                statuses,
                metrics,
                item_queue,
                write_queue,
                items_total,
                max_id
            )
            terminal_width = shutil.get_terminal_size((80, 20)).columns
            max_width = max(1, terminal_width - 1)
            lines = [truncate_ansi_line(line, max_width) for line in lines]
            if rendered:
                if cursor_up:
                    sys.stdout.write(f"\x1b[{cursor_up}A")
            for idx, line in enumerate(lines):
                sys.stdout.write("\r\x1b[2K" + line)
                if idx < len(lines) - 1:
                    sys.stdout.write("\n")
            sys.stdout.flush()
            rendered = True
            await asyncio.sleep(UI_REFRESH_SECONDS)
    finally:
        sys.stdout.write(COLOR_RESET)
        sys.stdout.write("\x1b[?25h")
        sys.stdout.write("\n")
        sys.stdout.flush()


async def worker_loop(
    worker_id: int,
    session: aiohttp.ClientSession,
    item_queue: asyncio.Queue,
    write_queue: asyncio.Queue,
    status: WorkerStatus,
    metrics: Metrics
) -> None:
    while True:
        item_id = await item_queue.get()
        try:
            if item_id is None:
                status.state = STATE_IDLE
                break

            status.state = STATE_FETCHING
            status.item_id = item_id
            status.retries = 0
            status.last_latency_ms = 0.0

            start = time.monotonic()
            item_json, retries = await fetch_item(session, item_id)
            status.last_latency_ms = (time.monotonic() - start) * 1000.0
            status.retries = retries
            metrics.retries += retries

            if item_json == "null":
                status.state = STATE_SKIPPED
                metrics.skipped += 1
                metrics.completed += 1
                continue

            status.downloaded += 1
            status.state = STATE_QUEUED
            ack = asyncio.get_running_loop().create_future()
            await write_queue.put((item_id, item_json, ack))
            ok = await ack

            if ok:
                status.last_persisted_id = item_id
                status.last_persisted_at = time.monotonic()
                metrics.persisted += 1
            else:
                status.state = STATE_ERROR
                status.last_error_at = time.monotonic()
                metrics.errors += 1

            metrics.completed += 1
            status.state = STATE_IDLE
        except Exception as e:
            status.state = STATE_ERROR
            status.last_error_at = time.monotonic()
            metrics.errors += 1
            logger.error(f"Worker {worker_id} failed on item {item_id}: {e}")
        finally:
            item_queue.task_done()


async def run() -> None:
    """
    Main function to run the data dumper.
    """
    start_time = time.time()
    max_id = 0
    last_id = 0

    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(DB_NAME) if os.path.dirname(DB_NAME) else ".", exist_ok=True)

    try:
        # Find the last ID we've processed
        last_id = get_last_id(DB_NAME)

        # Create TCP connector with keepalive to reuse connections
        connector = aiohttp.TCPConnector(
            limit=WORKER_COUNT,
            ttl_dns_cache=300,
            keepalive_timeout=60
        )

        # Create client session with retry options
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            raise_for_status=False  # We'll handle status codes ourselves
        ) as session:
            # Get the current maximum item ID
            max_id = await get_max_id(session)

            items_to_download = max_id - last_id
            if last_id > 0:
                logger.info(f"Resuming from ID {last_id:,}")
                logger.info(f"Max item ID: {max_id:,}")
                logger.info(f"Items to download: {items_to_download:,}")
            else:
                logger.info(f"Starting fresh download of {max_id:,} items")

            if items_to_download <= 0:
                return

            item_queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_SIZE)
            write_queue: asyncio.Queue = asyncio.Queue(maxsize=WRITE_QUEUE_SIZE)

            statuses = [WorkerStatus() for _ in range(WORKER_COUNT)]
            metrics = Metrics()
            stop_ui = asyncio.Event()

            writer_task = asyncio.create_task(
                db_writer_worker(DB_NAME, write_queue)
            )
            ui_task = asyncio.create_task(
                ui_loop(
                    statuses,
                    metrics,
                    item_queue,
                    write_queue,
                    items_to_download,
                    max_id,
                    stop_ui
                )
            )
            workers = [
                asyncio.create_task(
                    worker_loop(
                        worker_id,
                        session,
                        item_queue,
                        write_queue,
                        statuses[worker_id],
                        metrics
                    )
                )
                for worker_id in range(WORKER_COUNT)
            ]

            try:
                for item_id in range(last_id + 1, max_id + 1):
                    await item_queue.put(item_id)
                for _ in range(WORKER_COUNT):
                    await item_queue.put(None)

                await asyncio.gather(*workers)
            finally:
                await write_queue.put(None)
                await writer_task
                stop_ui.set()
                await ui_task
    finally:
        elapsed = time.time() - start_time
        items_processed = max_id - last_id

        if items_processed > 0:
            logger.info(f"Downloaded {items_processed:,} items in {elapsed:.2f} seconds")
            logger.info(f"Average rate: {items_processed / elapsed:.2f} items/second")
        else:
            logger.info("No new items to download")


def main() -> None:
    """Main entry point for the script."""
    try:
        # Run the main async function
        asyncio.run(run())

        logger.info("Finished successfully")
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)


if __name__ == "__main__":
    main()
