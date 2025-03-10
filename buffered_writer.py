from fastparquet import write
import pandas as pd
from pathlib import Path
import logging
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class BufferedParquetWriter:
    """Writes data to a parquet file with buffering for performance."""
    
    def __init__(self, filename: str, buffer_size: int = 1000) -> None:
        """
        Initialize a buffered parquet writer.
        
        Args:
            filename: Path to the output parquet file
            buffer_size: Number of items to buffer before writing to disk
        """
        self.keys: List[int] = []
        self.values: List[str] = []
        self.filename = filename
        self.buffer_size = buffer_size
        
        # Create parent directory if it doesn't exist
        path = Path(self.filename)
        if not path.parent.exists():
            path.parent.mkdir(parents=True, exist_ok=True)

    def add(self, key: int, value: str) -> None:
        """Add a key-value pair to the buffer."""
        self.keys.append(key)
        self.values.append(value)

        if len(self.keys) >= self.buffer_size:
            self.flush_to_file()

    def flush_to_file(self) -> None:
        """Write buffered data to the parquet file."""
        if not self.keys:
            return
            
        try:
            path = Path(self.filename)
            df = pd.DataFrame(data={"item_id": self.keys, "item_json": self.values})
            write(path, df, compression="ZSTD", append=path.exists())
            logger.debug(f"Wrote {len(self.keys)} items to {self.filename}")
            self.keys.clear()
            self.values.clear()
        except Exception as e:
            logger.error(f"Error writing to {self.filename}: {e}")
            raise

    def close(self) -> None:
        """Flush any remaining data and close the writer."""
        if self.keys:
            self.flush_to_file()


class BucketedBufferedParquetWriter:
    """Distributes data across multiple parquet files based on ID bucketing."""
    
    def __init__(self, basename: str, buffer_size: int = 1000, max_writers: int = 10) -> None:
        """
        Initialize a bucketed parquet writer.
        
        Args:
            basename: Base name for the output parquet files
            buffer_size: Number of items to buffer before writing to disk
            max_writers: Maximum number of active writer objects to keep in memory
        """
        self.basename = basename
        self.writers: Dict[str, BufferedParquetWriter] = {}
        self.buffer_size = buffer_size
        self.max_writers = max_writers
        self.access_order: List[str] = []  # LRU tracking
        
        # Ensure data directory exists
        os.makedirs(os.path.dirname(basename) if os.path.dirname(basename) else ".", exist_ok=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self) -> None:
        """Close all writers and flush pending data."""
        with ThreadPoolExecutor() as executor:
            # Close writers in parallel
            list(executor.map(lambda w: w.close(), self.writers.values()))
        self.writers.clear()
        self.access_order.clear()

    def add(self, key: int, value: str) -> None:
        """Add a key-value pair to the appropriate bucket."""
        data_bucket = self.id_to_bucket(key)

        # LRU management - move bucket to end of access list if it exists
        if data_bucket in self.access_order:
            self.access_order.remove(data_bucket)
        self.access_order.append(data_bucket)

        # Create writer if it doesn't exist
        if data_bucket not in self.writers:
            # If we have too many writers, close the least recently used one
            if len(self.writers) >= self.max_writers:
                lru_bucket = self.access_order.pop(0)
                self.writers[lru_bucket].close()
                del self.writers[lru_bucket]
                
            self.writers[data_bucket] = BufferedParquetWriter(
                f"{self.basename}-{data_bucket}.parquet", 
                buffer_size=self.buffer_size
            )

        self.writers[data_bucket].add(key, value)

    @staticmethod
    def id_to_bucket(data_id: int) -> str:
        """Convert an ID to a bucket string."""
        return f"{data_id // 1000000:04}"