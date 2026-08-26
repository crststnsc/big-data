from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from airflow.sdk import Variable, dag, task

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 15
MAX_WORKERS = 20
USER_AGENT = "Mozilla/5.0 (compatible; NewsPipelineBot/1.0; +https://airflow.apache.org)"


def fetch_feed(feed: dict) -> tuple[dict, list, str | None]:
    import feedparser

    try:
        resp = requests.get(
            feed["url"], timeout=REQUEST_TIMEOUT, headers={"User-Agent": USER_AGENT}
        )
        resp.raise_for_status()
        parsed = feedparser.parse(resp.content)
        return feed, parsed.entries, None
    except Exception as exc:  # feed sites fail in many non-Exception-specific ways
        return feed, [], str(exc)


@dag(
    dag_id="rss_export",
    description="Fetch all RSS feeds in feeds.clean.json and index entries into Elasticsearch",
    schedule="0 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["rss", "ingestion"],
)
def rss_export():
    @task
    def fetch_and_index(logical_date=None) -> dict:
        from news_es import bulk_index_rss_docs, rss_entry_to_doc

        feeds_path = Path(
            Variable.get("rss_feeds_file", default="/home/crst/dev/project/feeds.clean.json")
        )
        feeds = json.loads(feeds_path.read_text())
        batch_ts = logical_date.strftime("%Y%m%d%H%M%S")

        docs = []
        feeds_ok = 0
        feeds_failed = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = [pool.submit(fetch_feed, feed) for feed in feeds]
            for future in as_completed(futures):
                feed, entries, error = future.result()
                if error:
                    feeds_failed += 1
                    logger.warning("Feed failed name=%s url=%s error=%s", feed["name"], feed["url"], error)
                    continue
                feeds_ok += 1
                for entry in entries:
                    doc = rss_entry_to_doc(entry, feed, batch_ts)
                    if doc:
                        docs.append(doc)

        stats = bulk_index_rss_docs(docs) if docs else {"indexed": 0, "failed": 0}
        stats.update({"feeds_ok": feeds_ok, "feeds_failed": feeds_failed, "feeds_total": len(feeds)})
        logger.info("RSS batch_ts=%s complete: %s", batch_ts, stats)
        return stats

    fetch_and_index()


rss_export()
