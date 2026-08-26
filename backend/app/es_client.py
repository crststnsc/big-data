from __future__ import annotations

import os

from elasticsearch import Elasticsearch

ES_URL = os.environ.get("ES_URL", "http://localhost:9200")
INDEX_NAME = os.environ.get("NEWS_INDEX", "news_events")


def get_client() -> Elasticsearch:
    return Elasticsearch(ES_URL, request_timeout=30, retry_on_timeout=True, max_retries=3)
