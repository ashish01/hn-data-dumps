from fastparquet import write
import pandas as pd
from os import path


class BufferedParquetWriter:
    def __init__(self, filename: str, buffer_size: int = 1000) -> None:
        self.keys = []
        self.values = []
        self.filename = filename
        self.buffer_size = buffer_size

    def add(self, key: int, value: str):
        self.keys.append(key)
        self.values.append(value)

        if len(self.keys) < self.buffer_size:
            return

        self.flush_to_file()

    def flush_to_file(self):
        df = pd.DataFrame(data={"item_id": self.keys, "item_json": self.values})
        write(self.filename, df, compression="ZSTD", append=path.exists(self.filename))
        self.keys.clear()
        self.values.clear()

    def close(self):
        if self.keys:
            self.flush_to_file()


class BucketedBufferedParquetWriter:
    def __init__(self, basename: str):
        self.basename = basename
        self.writers = {}

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        for v in self.writers.values():
            v.close()

    def add(self, key: int, value: str) -> None:
        data_bucket = BucketedBufferedParquetWriter.id_to_bucket(key)

        if data_bucket not in self.writers:
            self.writers[data_bucket] = BufferedParquetWriter(
                f"{self.basename}-{data_bucket}.parquet"
            )

        self.writers[data_bucket].add(key, value)

    @staticmethod
    def id_to_bucket(data_id: int) -> str:
        return f"{data_id // 1000000:04}"
