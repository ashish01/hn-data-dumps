# hn-data-dumps

Hacker News corpus has a few very nice properties. It is small enough that it can be analyzed on a laptop but at the same time it's big and interesting enough to do some non trivial experiments for learning or otherwise. To do any analysis it would be nice to have a copy of HN corpus. Looking around on the web I did find some efforts to have such a copy but each had something missing. [Google BigQuery HN dataset](https://console.cloud.google.com/marketplace/product/y-combinator/hacker-news?filter=solution-type:dataset&q=hacker%20news&id=5227103e-0eb9-4744-872b-325a8df50bee) came the closest but looks like it has not been updated in a while.

Luckily HN has a nice [FireBase API](https://github.com/HackerNews/API) which updates in real time. So I wrote a (very) small crawler to get all the items starting with id 1 all the way to id 25,562,625 (at the time of this writing).

Once the initial dataset has been crawled, incremental updates are quite cheap. There is a small script which runs once a day to download everything since last sync and then uploads a snapshot to this repo, in case anyone else finds it useful as well.
