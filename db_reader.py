import sqlite3
import zstd

DB_NAME = "hn2.db3"

with sqlite3.connect(DB_NAME) as db:
    for row in db.execute("select * from hn_items"):
        print("{}\t{}".format(row[0], zstd.decompress(row[1]).decode("utf8")))
