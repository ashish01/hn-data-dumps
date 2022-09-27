import asyncio
import aiohttp
from tqdm import tqdm
import json
import sqlite3
import queue
import threading
import zstd


DB_NAME = "hn2.db3"


def create_db(db_name):
    with sqlite3.connect(db_name) as db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS hn_items(id int PRIMARY KEY, item_json_compressed blob)"
        )
        db.commit()


def get_last_id(db_name):
    with sqlite3.connect(db_name) as db:
        cursor = db.execute("select max(id) from hn_items")
        rows = cursor.fetchall()
        return int(rows[0][0]) if rows[0][0] else 0


async def get_max_id():
    async with aiohttp.ClientSession() as session:
        async with session.get(
            "https://hacker-news.firebaseio.com/v0/maxitem.json"
        ) as response:
            text = await response.text()
    return json.loads(text)


def db_writer_worker(db_name, input_queue):
    with sqlite3.connect(db_name, isolation_level=None) as db:
        db.execute('pragma journal_mode=wal;')
        while True:
            data = input_queue.get()
            if data is None:
                break
            item, item_json = data
            item_json_compressed = zstd.compress(item_json.encode("utf8"))
            db.execute(
                "insert into hn_items values(?, ?)", (item, item_json_compressed)
            )


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
    create_db(DB_NAME)
    last_id = get_last_id(DB_NAME)
    max_id = await get_max_id()

    N = 100
    sem = asyncio.Semaphore(N)

    async with aiohttp.ClientSession() as session:
        for id in tqdm(range(last_id + 1, max_id + 1)):
            await sem.acquire()
            asyncio.create_task(fetch_and_save(session, db_queue, sem, id))

        for i in range(N):
            await sem.acquire()


db_queue = queue.Queue()
db_thread = threading.Thread(target=db_writer_worker, args=(DB_NAME, db_queue))
db_thread.start()

asyncio.get_event_loop().run_until_complete(run(db_queue))

db_queue.put(None)
db_thread.join()
