# News Events Search

A big-data pipeline that collects world news from two sources, **GDELT** and about **190 RSS feeds**, and stores it in Elasticsearch. It adds the people, organizations and places mentioned in each document, then serves everything through a search API and a web app with search, a dashboard and a map.

```
  GDELT 2.0 (every 15 min)        RSS feeds (hourly)
            │                            │
            ▼                            ▼
   ┌──────────────────┐        ┌──────────────────┐
   │ gdelt_export DAG │        │  rss_export DAG  │        Apache Airflow
   └────────┬─────────┘        └────────┬─────────┘
            └─────────────┬─────────────┘
                          ▼
              ┌───────────────────────┐   ◀── entity_enrichment DAG (every 30 min)
              │ Elasticsearch         │       spaCy NER + Argos Translate
              │ index: news_events    │
              └───────────┬───────────┘
                          ▼
              ┌───────────────────────┐
              │ FastAPI backend :8000 │
              └───────────┬───────────┘
                          ▼
              ┌───────────────────────┐
              │ React frontend :5173  │   Search · Dashboard · Map
              └───────────────────────┘
```

## Where the data comes from

### 1. GDELT 2.0 Events

The **[GDELT Project](https://www.gdeltproject.org/)** (Global Database of Events, Language, and Tone) monitors news media worldwide in over 100 languages. It picks out *events* ("who did what to whom, where") and publishes them as free, open data. No API key is needed.

Every 15 minutes GDELT publishes a new file at:

```
http://data.gdeltproject.org/gdeltv2/{YYYYmmddHHMMSS}.export.CSV.zip
```

The timestamp always falls on `:00`, `:15`, `:30` or `:45` UTC. Each file is a zipped, **tab-separated** file with **no header row** and 61 columns, which are defined in the [GDELT 2.0 Event Codebook](https://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf). Each row is one event, with:

- **Actors:** the two parties involved (`Actor1Name`, `Actor2Name`) and their countries.
- **Event type:** a [CAMEO](https://parusanalytics.com/eventdata/data.dir/cameo.html) code. The root codes `01`–`20` run from "Make statement" to "Mass violence". `QuadClass` sorts each event into one of four groups: verbal cooperation, material cooperation, verbal conflict or material conflict.
- **Scores:** `GoldsteinScale` (−10 to +10, how much the event is likely to destabilize or stabilize a country) and `AvgTone` (how negative or positive the coverage is).
- **Location:** where the event happened (`ActionGeo_*`): place name, country, latitude/longitude.
- **Source:** the URL of the news article the event was taken from (`SOURCEURL`).

### 2. RSS news feeds

[`feeds.clean.json`](feeds.clean.json) lists **191 working RSS feeds** from news outlets in **26 countries**, plus a few international ones, in **15 languages**:

| Language | Feeds | Language | Feeds | Language | Feeds |
|---|---|---|---|---|---|
| English (en) | 98 | Polish (pl) | 7 | Japanese (ja) | 5 |
| Spanish (es) | 17 | Russian (ru) | 7 | Bengali (bn) | 4 |
| Italian (it) | 11 | Persian (fa) | 6 | German (de) | 3 |
| French (fr) | 9 | Romanian (ro) | 6 | Chinese (zh) | 3 |
| Ukrainian (uk) | 8 | Portuguese (pt) | 5 | Indonesian (id) | 2 |

Each entry has a `name`, `url`, `country` (ISO 3166 alpha-3, or `INT` for international outlets), `language` and `category`.

**How this list was made.** [`feeds.json`](feeds.json) is the original list of 269 candidate feeds. [`validate_feeds.py`](validate_feeds.py) fetched every one of them and kept the 191 that returned HTTP 200 and at least one parseable entry. For each feed it also reports the entry count, the average text length, and whether the feed includes full article text (`content:encoded`) or only summaries.

```bash
python validate_feeds.py feeds.json --write-clean feeds.clean.json
```

## How it works

### Ingestion (Airflow DAGs, in [`dags/`](dags/))

| DAG | Schedule | What it does |
|---|---|---|
| `gdelt_export` | every 15 min | Download → unzip → index GDELT events |
| `rss_export` | hourly | Fetch all RSS feeds → index articles |
| `entity_enrichment` | every 30 min | Add people/organizations/locations to documents that don't have them yet |

All three have `catchup=False` and `max_active_runs=1`.

**`gdelt_export`** ([`gdelt_export.py`](dags/gdelt_export.py)) has four tasks:
1. `compute_ts`: rounds the run's `logical_date` down to the quarter hour to get the file timestamp.
2. `download`: fetches the zip into `{gdelt_output_root}/{ts}/`. If GDELT hasn't published the file yet (HTTP 404), the task fails and Airflow retries it, up to 3 times with exponential backoff starting at 2 minutes.
3. `extract_csv`: unzips the file to `{ts}.csv` and adds a header row with the 61 codebook column names.
4. `index_to_es`: turns each row into a document and bulk-indexes it in batches of 500. The document ID is the `GlobalEventID`, so re-running an interval overwrites documents rather than duplicating them. Coordinates of `0,0` count as missing.

**`rss_export`** ([`rss_export.py`](dags/rss_export.py)) fetches all feeds in parallel (20 threads, 15-second timeout) and parses them with `feedparser`. A feed that fails is logged and skipped, so one broken feed doesn't fail the whole run. Each entry becomes a document with HTML stripped from its summary and content. The document ID is the entry's `guid`, or its link if there's no guid, so an article seen in several polls is stored only once.

**`entity_enrichment`** ([`entity_enrichment.py`](dags/entity_enrichment.py) + [`nlp_enrich.py`](dags/nlp_enrich.py)) finds documents that have no `enriched_at` field yet and handles up to 2,000 per type per run:
- **GDELT events:** the two actors become `organizations` and the event location becomes `locations`. No NLP is needed for these.
- **RSS articles:** it runs **spaCy** named-entity recognition on the article content, falling back to the summary and then the title. Only the first 20,000 characters are used. Small spaCy models cover 12 languages: en, de, es, fr, it, ja, pl, pt, ro, ru, uk and zh. Non-English entity names are then translated to English offline with **Argos Translate** and stored in `entities_en`, so you can search for "Germany" and match "Deutschland". The other three feed languages (bn, fa, id) have no model. Those articles are marked as enriched with no entities, so they aren't picked up again on every run.

[`news_es.py`](dags/news_es.py) holds the shared Elasticsearch code: the index mapping, the row/entry → document conversion, and bulk indexing. It also works as a command-line maintenance tool:

```bash
python dags/news_es.py --count                   # number of documents in the index
python dags/news_es.py --recreate                # delete and recreate the index (destroys all data!)
python dags/news_es.py --load path/to/file.csv   # load a GDELT file by hand
python dags/news_es.py --backfill-html           # strip HTML from already-stored RSS text
python dags/news_es.py --backfill-fetch-status   # fix fetch_status on older documents
```

### Storage (Elasticsearch)

Both sources go into **one index**, `news_events` (1 shard, 0 replicas). The `type` field tells them apart: `gdelt_event` or `rss_article`. The mapping is `strict`, so a document with a field that isn't in the mapping is rejected.

| Field group | Fields |
|---|---|
| Common | `type`, `url`, `domain`, `content_date`, `ingested_at`, `interval_ts`, `fetch_status`, `enriched_at` |
| GDELT | `event_code`, `event_base_code`, `event_root_code`, `quad_class`, `is_root_event`, `goldstein`, `avg_tone`, `num_mentions`/`num_sources`/`num_articles`, `actor1`/`actor2` (+ countries), `place`, `country`, `adm1`, `geo_type`, `location` (`geo_point`), `seen_at` |
| RSS | `title`, `summary`, `content`, `language`, `author`, `guid`, `feed_name`, `feed_category`, `source_country` |
| Entities | `entities.{persons,organizations,locations}`, plus the English versions in `entities_en.*` |

Text fields use an analyzer that lowercases text and removes accents (`asciifolding`), so `Zürich` matches `zurich`. `content_date` is the GDELT event day for GDELT documents and the publish date for RSS documents. It's the date used for sorting, filtering and charts.

### Backend (FastAPI, in [`backend/`](backend/))

A read-only API on port 8000 that queries Elasticsearch directly. If Elasticsearch can't be reached, every endpoint returns `503`.

| Endpoint | Description |
|---|---|
| `GET /api/health` | Health check |
| `GET /api/search` | Full-text search. Params: `q`, `type`, `language`, `feed_category`, `entity`, `date_from`, `date_to`, `sort` (`relevance` / `date_desc` / `date_asc`), `page`, `page_size` (max 100). List params can be repeated, e.g. `?type=rss_article&type=gdelt_event`. |
| `GET /api/facets` | Counts by type, language and feed category, used for the filter checkboxes |
| `GET /api/documents/{id}` | One document with all its fields |
| `GET /api/stats` | Dashboard numbers for the **last 7 full days**: totals, daily volume per source, quad-class split, top event categories, RSS languages, daily average tone |
| `GET /api/map/events` | GDELT events grouped into geohash cells of about 156 km (up to 300). Each cell has its centre point, an event count and one example event. Params: `date_from`, `date_to`, `quad_class`, `event_root_code`. |

How search ranks results ([`search.py`](backend/app/search.py)): it combines a fuzzy `multi_match` on title (×3), summary (×2), content, actors (×1.5) and place with partial matches on the entity fields. English entity names count more than original-language ones, and people count more than organizations, which count more than locations. Matches in titles and text are highlighted with `<mark>`. When there's no query, results are sorted newest first.

Interactive API docs are at http://localhost:8000/docs once the backend is running.

### Frontend (React + Vite, in [`frontend/`](frontend/))

A React 19 + TypeScript single-page app. In development, Vite forwards `/api` requests to the backend on port 8000.

- **Search** (`/`): search box, filters (source type, language, feed category, entity, date range), sorting, pagination and highlighted matches.
- **Document detail** (`/documents/:id`): every field of a document, with its entities shown in both the original language and English.
- **Dashboard** (`/dashboard`): stat tiles plus [Recharts](https://recharts.org/) charts of volume over time, quad class, top CAMEO event categories, RSS languages and average tone over time.
- **Map** (`/map`): a [Leaflet](https://leafletjs.com/) map on [OpenStreetMap](https://www.openstreetmap.org/) tiles. Clusters are sized by event count and coloured cooperation vs. conflict. You can filter by date, quad class and event category.

The dashboard and map are loaded only when you open them, so the search page stays light.

## Project layout

```
.
├── dags/
│   ├── gdelt_export.py        # GDELT ingestion DAG
│   ├── rss_export.py          # RSS ingestion DAG
│   ├── entity_enrichment.py   # Entity enrichment DAG
│   ├── nlp_enrich.py          # spaCy NER + Argos Translate helpers
│   └── news_es.py             # Index mapping, document conversion, bulk indexing, maintenance CLI
├── backend/
│   ├── requirements.txt
│   └── app/
│       ├── main.py            # FastAPI app, CORS, routers
│       ├── es_client.py       # Elasticsearch client
│       ├── search.py          # Search query builder
│       ├── stats.py           # Dashboard query
│       ├── map_stats.py       # Map query
│       ├── schemas.py         # Pydantic response models
│       └── routers/           # search, documents, stats, map_events
├── frontend/
│   └── src/
│       ├── pages/             # SearchPage, DetailPage, DashboardPage, MapPage
│       ├── components/        # Filters, results, pagination, charts/
│       ├── api/client.ts      # API calls
│       └── cameo.ts           # CAMEO root code labels
├── feeds.json                 # All 269 candidate RSS feeds
├── feeds.clean.json           # The 191 feeds that passed validation (used by rss_export)
└── validate_feeds.py          # Feed checker
```

## Running it locally

### Requirements

- Python 3.13
- Node.js 20+
- Elasticsearch 9.x on `localhost:9200`. The Python client is `elasticsearch` 9.5, which needs a 9.x server.

A single-node Elasticsearch for development, with security turned off so it runs over plain HTTP:

```bash
docker run -d --name es -p 9200:9200 \
  -e discovery.type=single-node -e xpack.security.enabled=false \
  docker.elastic.co/elasticsearch/elasticsearch:9.1.0
```

### 1. Airflow and the DAGs

```bash
python3.13 -m venv .venv && source .venv/bin/activate
pip install "apache-airflow==3.3.1" elasticsearch requests feedparser spacy argostranslate

# spaCy models (only install the languages you need)
for m in en_core_web_sm de_core_news_sm es_core_news_sm fr_core_news_sm it_core_news_sm \
         ja_core_news_sm pl_core_news_sm pt_core_news_sm ro_core_news_sm ru_core_news_sm \
         uk_core_news_sm zh_core_web_sm; do python -m spacy download "$m"; done
```

Argos Translate also needs a language package for each language pair it translates into English, e.g. de→en, es→en and so on. Install them with `argospm install translate-de_en` and the like. If a package is missing, the entity is kept untranslated and a warning is logged.

```bash
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"
export ES_URL=http://localhost:9200            # optional, this is the default

airflow variables set gdelt_output_root "$(pwd)/data/gdelt"
airflow variables set rss_feeds_file "$(pwd)/feeds.clean.json"   # optional, defaults to the repo copy

airflow standalone                             # UI at http://localhost:8080
```

Turn on the three DAGs in the Airflow UI. The index is created automatically the first time anything writes to it.

### 2. Backend

```bash
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

### 3. Frontend

```bash
cd frontend
npm install
npm run dev                                    # http://localhost:5173
```

## Configuration

| Setting | Where | Default | Purpose |
|---|---|---|---|
| `ES_URL` | env var (DAGs + backend) | `http://localhost:9200` | Elasticsearch address |
| `NEWS_INDEX` | env var (DAGs + backend) | `news_events` | Index name |
| `gdelt_output_root` | Airflow Variable | `/opt/airflow/data/gdelt` | Where GDELT zips/CSVs are saved |
| `rss_feeds_file` | Airflow Variable | `feeds.clean.json` in the repo root | Feed list used by `rss_export` |

The backend's CORS settings only allow `http://localhost:5173` and `http://127.0.0.1:5173`. Change them in [`backend/app/main.py`](backend/app/main.py) if the frontend runs somewhere else.

Downloaded data (`data/`) and the virtualenv (`.venv/`) are excluded by `.gitignore`.
