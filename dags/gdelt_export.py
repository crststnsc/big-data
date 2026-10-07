from __future__ import annotations

import logging
import shutil
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from airflow.sdk import Variable, dag, task
from airflow.sdk.exceptions import AirflowException

logger = logging.getLogger(__name__)

GDELT_BASE_URL = "http://data.gdeltproject.org/gdeltv2"

# https://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf
GDELT_EVENT_COLUMNS = [
    "GlobalEventID", "Day", "MonthYear", "Year", "FractionDate",
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode",
    "Actor1EthnicCode", "Actor1Religion1Code", "Actor1Religion2Code",
    "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode",
    "Actor2EthnicCode", "Actor2Religion1Code", "Actor2Religion2Code",
    "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
    "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode",
    "QuadClass", "GoldsteinScale", "NumMentions", "NumSources",
    "NumArticles", "AvgTone",
    "Actor1Geo_Type", "Actor1Geo_Fullname", "Actor1Geo_CountryCode",
    "Actor1Geo_ADM1Code", "Actor1Geo_ADM2Code", "Actor1Geo_Lat",
    "Actor1Geo_Long", "Actor1Geo_FeatureID",
    "Actor2Geo_Type", "Actor2Geo_Fullname", "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code", "Actor2Geo_ADM2Code", "Actor2Geo_Lat",
    "Actor2Geo_Long", "Actor2Geo_FeatureID",
    "ActionGeo_Type", "ActionGeo_Fullname", "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code", "ActionGeo_ADM2Code", "ActionGeo_Lat",
    "ActionGeo_Long", "ActionGeo_FeatureID",
    "DATEADDED", "SOURCEURL",
]


def gdelt_ts_for(moment: datetime) -> str:
    floored_minute = (moment.minute // 15) * 15
    boundary = moment.replace(minute=floored_minute, second=0, microsecond=0)
    return boundary.strftime("%Y%m%d%H%M%S")


@dag(
    dag_id="gdelt_export",
    description="Download the full GDELT v2 event export file every 15 min",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
    },
    tags=["gdelt", "ingestion"],
)
def gdelt_export():
    @task
    def compute_ts(logical_date=None) -> str:
        file_ts = gdelt_ts_for(logical_date)
        logger.info("Resolved GDELT file_ts=%s for logical_date=%s", file_ts, logical_date)
        return file_ts

    @task
    def download(file_ts: str) -> str:
        output_root = Path(Variable.get("gdelt_output_root", default="/opt/airflow/data/gdelt"))
        url = f"{GDELT_BASE_URL}/{file_ts}.export.CSV.zip"
        out_dir = output_root / file_ts
        out_dir.mkdir(parents=True, exist_ok=True)
        zip_path = out_dir / f"{file_ts}.export.CSV.zip"

        logger.info("Downloading %s", url)
        resp = requests.get(url, timeout=60)
        if resp.status_code == 404:
            raise AirflowException(f"GDELT file not yet published for file_ts={file_ts} ({url})")
        resp.raise_for_status()

        zip_path.write_bytes(resp.content)
        logger.info("Saved %s (%d bytes)", zip_path, len(resp.content))
        return str(zip_path)

    @task
    def extract_csv(zip_path: str, file_ts: str) -> str:
        out_dir = Path(zip_path).parent
        csv_path = out_dir / f"{file_ts}.csv"

        with zipfile.ZipFile(zip_path) as zf:
            csv_name = next(n for n in zf.namelist() if n.endswith(".CSV"))
            with zf.open(csv_name) as raw, csv_path.open("wb") as out_f:
                out_f.write(("\t".join(GDELT_EVENT_COLUMNS) + "\n").encode("utf-8"))
                shutil.copyfileobj(raw, out_f)

        with csv_path.open(encoding="utf-8", errors="replace") as f:
            row_count = sum(1 for _ in f) - 1  # exclude header row
        logger.info("file_ts=%s: staged %d rows -> %s", file_ts, row_count, csv_path)
        return str(csv_path)

    @task
    def index_to_es(csv_path: str, file_ts: str) -> dict:
        from news_es import bulk_index_file
        return bulk_index_file(csv_path, file_ts)

    file_ts = compute_ts()
    zip_path = download(file_ts)
    csv_path = extract_csv(zip_path, file_ts)
    index_to_es(csv_path, file_ts)


gdelt_export()
