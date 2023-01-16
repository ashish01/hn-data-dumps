import asyncio
import aiohttp
from tqdm import tqdm
import json
import queue
import threading
from buffered_writer import BucketedBufferedParquetWriter
import duckdb

DB_NAME = "data/hn"


def get_last_id(db_name):
    try:
        with duckdb.connect(database=":memory:") as con:
            return (
                con.execute(
                    f"select max(item_id) from '{db_name}-*.parquet'"
                ).fetchall()[0][0]
                + 1
            )
    except:
        return 0


async def get_max_id():
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False)
    ) as session:
        async with session.get(
            "https://hacker-news.firebaseio.com/v0/maxitem.json"
        ) as response:
            text = await response.text()
    return json.loads(text)


def db_writer_worker(db_name, input_queue):
    with BucketedBufferedParquetWriter(db_name) as db:
        while True:
            data = input_queue.get()
            if data is None:
                break
            item_id, item_json = data
            db.add(item_id, item_json)


async def fetch_and_save(session, db_queue, sem, id):
    url = f"https://hacker-news.firebaseio.com/v0/item/{id}.json"
    try:
        async with session.get(url) as response:
            text = await response.text()
            db_queue.put((id, text))
    except Exception as e:
        print(e)
    finally:
        sem.release()


async def run(db_queue):
    last_id = get_last_id(DB_NAME)
    max_id = await get_max_id()

    N = 100
    sem = asyncio.Semaphore(N)

    if last_id > 0:
        print(
            "Resuming! If this is not intended then delete the data folder and restart."
        )
        print(f"Max item id                     {max_id:,}")
        print(f"Number of items downloaded      {last_id:,}")
        print(f"Number of items remaining       {max_id-last_id:,}")

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False)
    ) as session:
        for id in tqdm(range(last_id + 1, max_id + 1)):
            await sem.acquire()
            asyncio.create_task(fetch_and_save(session, db_queue, sem, id))

        for i in range(N):
            await sem.acquire()


db_queue = queue.Queue()
db_thread = threading.Thread(target=db_writer_worker, args=(DB_NAME, db_queue))
db_thread.start()

asyncio.run(run(db_queue))

db_queue.put(None)
db_thread.join()
