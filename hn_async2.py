#!/usr/bin/env python3
"""
Hacker News Data Dumper - Efficiently downloads and stores all Hacker News items.
"""
import asyncio
import aiohttp
import json
import queue
import threading
import logging
import time
import os
from pathlib import Path
from typing import Optional, Tuple, List, Set, Dict, Any
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from tqdm.asyncio import tqdm_asyncio
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
CONCURRENT_REQUESTS = 100
REQUEST_TIMEOUT = 10  # seconds
RETRY_ATTEMPTS = 3
RETRY_DELAY = 1  # seconds
QUEUE_SIZE = 10000


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
                    col_max = metadata.row_group(row_grp).column(0).statistics.max
                    if col_max > max_id:
                        max_id = col_max
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


def db_writer_worker(db_name: str, input_queue: queue.Queue, buffer_size: int = 1000) -> None:
    """
    Worker thread that writes items to parquet files.
    
    Args:
        db_name: Base name for the database files
        input_queue: Queue to receive items from
        buffer_size: Number of items to buffer before writing
    """
    logger.info(f"Starting database writer thread with buffer size {buffer_size}")
    
    try:
        with BucketedBufferedParquetWriter(db_name, buffer_size=buffer_size) as db:
            while True:
                try:
                    data = input_queue.get()
                    
                    # None is the signal to exit
                    if data is None:
                        logger.info("Received exit signal, closing database")
                        break
                        
                    item_id, item_json = data
                    db.add(item_id, item_json)
                    
                except Exception as e:
                    logger.error(f"Error in database writer: {e}")
                finally:
                    input_queue.task_done()
    except Exception as e:
        logger.critical(f"Fatal error in database writer thread: {e}")
        # In a production environment, we might want to signal the main thread
        # that a critical error occurred, so it can shut down gracefully


async def fetch_item(
    session: aiohttp.ClientSession, 
    item_id: int,
    retry_attempts: int = RETRY_ATTEMPTS
) -> Optional[str]:
    """
    Fetch a single item from the Hacker News API.
    
    Args:
        session: The aiohttp ClientSession to use
        item_id: The ID of the item to fetch
        retry_attempts: Number of retry attempts for failed requests
        
    Returns:
        The JSON string representing the item, or None if the item doesn't exist
    """
    url = f"{API_BASE_URL}/item/{item_id}.json"
    
    for attempt in range(retry_attempts):
        try:
            async with session.get(url, timeout=REQUEST_TIMEOUT) as response:
                if response.status == 404:
                    # Item doesn't exist
                    return "null"
                elif response.status != 200:
                    logger.debug(f"Got status {response.status} for item {item_id}, retrying...")
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                
                return await response.text()
                
        except asyncio.TimeoutError:
            logger.debug(f"Timeout fetching item {item_id} (attempt {attempt+1})")
        except Exception as e:
            logger.debug(f"Error fetching item {item_id} (attempt {attempt+1}): {e}")
        
        # Wait before retrying
        await asyncio.sleep(RETRY_DELAY * (attempt + 1))
    
    # If all retries failed, return null to skip this item
    logger.warning(f"Failed to fetch item {item_id} after {retry_attempts} attempts")
    return "null"


async def fetch_and_enqueue(
    session: aiohttp.ClientSession,
    db_queue: queue.Queue,
    item_id: int,
    semaphore: asyncio.Semaphore
) -> None:
    """
    Fetch an item and add it to the processing queue.
    
    Args:
        session: The aiohttp ClientSession to use
        db_queue: Queue to add the item to
        item_id: The ID of the item to fetch
        semaphore: Semaphore to limit concurrent requests
    """
    try:
        async with semaphore:
            item_json = await fetch_item(session, item_id)
            
            # Skip if item doesn't exist or we couldn't fetch it
            if item_json is None or item_json == "null":
                return
                
            # Wait if queue is getting too full to avoid memory issues
            while db_queue.qsize() > QUEUE_SIZE:
                await asyncio.sleep(0.1)
                
            db_queue.put((item_id, item_json))
            
    except Exception as e:
        logger.error(f"Error processing item {item_id}: {e}")


async def run(db_queue: queue.Queue) -> None:
    """
    Main function to run the data dumper.
    
    Args:
        db_queue: Queue to send items to for database writing
    """
    start_time = time.time()
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(DB_NAME) if os.path.dirname(DB_NAME) else ".", exist_ok=True)
    
    # Find the last ID we've processed
    last_id = get_last_id(DB_NAME)
    
    # Create TCP connector with keepalive to reuse connections
    connector = aiohttp.TCPConnector(
        ssl=False,
        limit=CONCURRENT_REQUESTS,
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
        
        # Create semaphore to limit concurrent requests
        sem = asyncio.Semaphore(CONCURRENT_REQUESTS)
        
        if last_id > 0:
            logger.info(f"Resuming from ID {last_id:,}")
            logger.info(f"Max item ID: {max_id:,}")
            logger.info(f"Items to download: {max_id-last_id:,}")
        else:
            logger.info(f"Starting fresh download of {max_id:,} items")
        
        # Create and schedule tasks for all items
        tasks = []
        for item_id in range(last_id + 1, max_id + 1):
            task = asyncio.create_task(
                fetch_and_enqueue(session, db_queue, item_id, sem)
            )
            tasks.append(task)
        
        # Show progress bar while waiting for tasks to complete
        for task in tqdm_asyncio(
            asyncio.as_completed(tasks),
            total=len(tasks),
            desc="Downloading items",
            unit="items"
        ):
            await task
        
        # Wait for any remaining tasks (should be none at this point)
        if tasks:
            await asyncio.gather(*tasks)
    
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
        # Create thread-safe queue for communication between asyncio and the writer thread
        db_queue = queue.Queue()
        
        # Start database writer thread
        db_thread = threading.Thread(
            target=db_writer_worker,
            args=(DB_NAME, db_queue),
            daemon=True  # Allow the program to exit if this thread is still running
        )
        db_thread.start()
        
        # Run the main async function
        asyncio.run(run(db_queue))
        
        # Signal the database writer to exit and wait for it to finish
        logger.info("Waiting for database writer to finish...")
        db_queue.put(None)
        db_thread.join()
        
        logger.info("Finished successfully")
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)


if __name__ == "__main__":
    main()