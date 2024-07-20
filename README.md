# hn-data-dumps

Hacker News corpus has a few very nice properties. It is small enough that it
can be analyzed on a laptop but at the same time it's big and interesting
enough to do some non trivial experiments for learning or otherwise. To do any
analysis it would be nice to have a copy of HN corpus. Looking around on the
web I did find some efforts to have such a copy but each had something missing.
[Google BigQuery HN
dataset](https://console.cloud.google.com/marketplace/product/y-combinator/hacker-news?filter=solution-type:dataset&q=hacker%20news&id=5227103e-0eb9-4744-872b-325a8df50bee)
came the closest but looks like it has not been updated in a while.

Luckily HN has a nice [FireBase API](https://github.com/HackerNews/API) which
updates in real time. So I wrote a (very) small crawler to get all the items
starting with id 1 all the way to id 25,562,625 (at the time of this writing).

Once the initial dataset has been crawled, incremental updates are quite cheap.
There is a small script which runs once a day to download everything since last
sync and then uploads a snapshot to this repo, in case anyone else finds it
useful as well.


## Running the Script

To create a `venv` and install dependencies (only needs to be done once):

```
# enter project directory
cd hn-data-dumps

# create new venv
python3 -m venv .venv

# activate venv
source .venv/bin/activate

# install dependencies
python3 -m pip install --upgrade pip
pip install -r ./requirements.txt

# exit venv
deactivate
```

To run the script going forward:

```
# enter project directory
cd hn-data-dumps

# activate venv
source .venv/bin/activate

# download data from HN
python hn_async2.py

# exit venv
deactivate
```

`hn_async2.py` will start downloading all items sequentially starting with id 1
to the current max from Firebase DB and store them locally in a SQLite database
named `hn2.db3`. Once the initial download is finished, which can take a long
time depending on your computer and network, subsequent runs only download the
new items created since the last run so they finish quickly.


## Data Schema

The schema of the DB is very simple. It only has one table - `hn_stories` which
contains integer ID of the story and its attributes stored as JSON.

```
.schema
CREATE TABLE hn_stories(id INT PRIMARY KEY, item_json TEXT);
```

Here is a sample of rows:

| id | item_json  |
| -- | ---------- |
| 20 | {"by":"pg","time":1160424038,"title":"Salaries at VC-backed companies","url":"http://avc.blogs.com/a_vc/2006/10/search_by_salar.html"}                                                        |
| 4  | {"by":"onebeerdave","time":1160419662,"title":"NYC Developer Dilemma","url":"http://avc.blogs.com/a_vc/2006/10/the_nyc_develop.html"}                                                         |
| 2  | {"by":"phyllis","time":1160418628,"title":"A Student's Guide to Startups","url":"http://www.paulgraham.com/mit.html"}                                                                         |
| 3  | {"by":"phyllis","time":1160419233,"title":"Woz Interview: the early days of Apple","url":"http://www.foundersatwork.com/stevewozniak.html"}                                                   |
| 7  | {"by":"phyllis","time":1160420455,"title":"Sevin Rosen Unfunds - why?","url":"http://featured.gigaom.com/2006/10/09/sevin-rosen-unfunds-why/"}                                                |
| 21 | {"by":"sama","time":1160443271,"title":"Best IRR ever?  YouTube 1.65B...","url":"http://www.techcrunch.com/2006/10/09/google-has-acquired-youtube/"}                                          |
| 9  | {"by":"askjigga","time":1160421542,"title":"weekendr: social network for the weekend","url":"http://www.weekendr.com/"}                                                                       |
| 1  | {"by":"pg","time":1160418111,"title":"Y Combinator","url":"http://ycombinator.com"}                                                                                                           |
| 5  | {"by":"perler","time":1160419864,"title":"Google, YouTube acquisition announcement could come tonight","url":"http://www.techcrunch.com/2006/10/09/google-youtube-sign-more-separate-deals/"} |
| 81 | {"by":"justin","time":1171869130,"title":"allfreecalls.com shut down by AT&T","url":"http://www.techcrunch.com/2007/02/16/allfreecalls-shut-down/"}                                           |

Let me know if you find this useful. Thanks!
