from __future__ import annotations

# Dashboard scope: last 7 full days. "/d" rounds down to midnight so the
# window is day-aligned (matches the day-granularity histograms below)
# instead of cutting off partway through today's data. This also
# supersedes the old fixed 2000-01-01 floor that existed only to exclude
# one corrupted document (year 0206) -- a 7-day rolling window is always
# far more recent than that outlier, so it's excluded either way.
DATE_FLOOR = "now-7d/d"

def build_stats_query() -> dict:
    return {
        "size": 0,
        "query": {"range": {"content_date": {"gte": DATE_FLOOR}}},
        "aggs": {
            "by_type": {"terms": {"field": "type", "size": 10}},
            "volume_over_time": {
                "date_histogram": {"field": "content_date", "calendar_interval": "day"},
                "aggs": {"by_type": {"terms": {"field": "type", "size": 10}}},
            },
            "quad_class": {
                "filter": {"term": {"type": "gdelt_event"}},
                "aggs": {"vals": {"terms": {"field": "quad_class", "size": 10}}},
            },
            "event_root_code": {
                "filter": {"term": {"type": "gdelt_event"}},
                "aggs": {"vals": {"terms": {"field": "event_root_code", "size": 20}}},
            },
            "languages": {
                "filter": {"term": {"type": "rss_article"}},
                "aggs": {"vals": {"terms": {"field": "language", "size": 20}}},
            },
            "avg_tone_over_time": {
                "filter": {"term": {"type": "gdelt_event"}},
                "aggs": {
                    "over_time": {
                        "date_histogram": {"field": "content_date", "calendar_interval": "day"},
                        "aggs": {"avg_tone": {"avg": {"field": "avg_tone"}}},
                    }
                },
            },
        },
    }
