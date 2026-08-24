"""Validate an RSS feed config before wiring it into a DAG.

RSS URLs rot constantly -- sites migrate, drop feeds, or move them without
redirects. Run this first so you find dead feeds in 30 seconds rather than
inside a failing Airflow task.

    pip install feedparser requests
    python validate_feeds.py feeds.json
    python validate_feeds.py feeds.json --write-clean feeds.clean.json

Reports per feed: HTTP status, entry count, whether entries carry full text
(content:encoded) or only a short summary, and a sample title.
"""
from __future__ import annotations

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import feedparser
import requests

UA = "Mozilla/5.0 (compatible; BigDataProject/1.0; +student-project)"
TIMEOUT = 20


def probe(feed: dict) -> dict:
    """Fetch and parse one feed; never raises."""
    out = {**feed, "ok": False, "status": None, "entries": 0,
           "has_full_text": False, "avg_summary_chars": 0,
           "sample": None, "error": None}
    try:
        resp = requests.get(feed["url"], timeout=TIMEOUT,
                            headers={"User-Agent": UA}, allow_redirects=True)
        out["status"] = resp.status_code
        if resp.status_code != 200:
            out["error"] = f"HTTP {resp.status_code}"
            return out

        parsed = feedparser.parse(resp.content)
        # bozo means the parser hit malformed XML; it often still yields
        # usable entries, so only treat it as fatal when nothing parsed
        entries = parsed.entries
        out["entries"] = len(entries)
        if not entries:
            out["error"] = f"parsed 0 entries (bozo={parsed.bozo})"
            return out

        lengths = []
        for e in entries[:10]:
            if e.get("content"):
                out["has_full_text"] = True
            body = (e.get("content", [{}])[0].get("value")
                    if e.get("content") else e.get("summary", ""))
            lengths.append(len(body or ""))

        out["avg_summary_chars"] = int(sum(lengths) / len(lengths)) if lengths else 0
        out["sample"] = (entries[0].get("title") or "")[:70]
        out["ok"] = True
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
    return out


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("config", help="Path to feeds.json")
    p.add_argument("--write-clean", help="Write only the working feeds to this path")
    p.add_argument("--workers", type=int, default=8)
    args = p.parse_args()

    feeds = json.loads(Path(args.config).read_text(encoding="utf-8"))
    print(f"Probing {len(feeds)} feeds...\n")

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(probe, feeds))

    hdr = f"{'':2} {'NAME':26} {'CC':4} {'LG':3} {'N':>4} {'CHARS':>6} {'FULL':>5}  DETAIL"
    print(hdr)
    print("-" * len(hdr))
    for r in sorted(results, key=lambda x: (not x["ok"], x["name"])):
        mark = "OK" if r["ok"] else "!!"
        detail = r["sample"] if r["ok"] else r["error"]
        print(f"{mark:2} {r['name'][:26]:26} {r['country']:4} {r['language']:3} "
              f"{r['entries']:>4} {r['avg_summary_chars']:>6} "
              f"{'yes' if r['has_full_text'] else '-':>5}  {detail}")

    working = [r for r in results if r["ok"]]
    total_entries = sum(r["entries"] for r in working)
    full_text = [r for r in working if r["has_full_text"]]

    print(f"\n{len(working)}/{len(feeds)} feeds working")
    print(f"{total_entries} entries available across one poll")
    print(f"{len(full_text)} feeds ship full article text (content:encoded)")
    print(f"languages: {sorted({r['language'] for r in working})}")

    if args.write_clean:
        clean = [{k: r[k] for k in ("name", "url", "country", "language", "category")}
                 for r in working]
        Path(args.write_clean).write_text(
            json.dumps(clean, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"\nWrote {len(clean)} working feeds -> {args.write_clean}")

    return 0 if working else 1


if __name__ == "__main__":
    sys.exit(main())
