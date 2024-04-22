import datetime
import requests
from pathlib import Path
from dagster import Backoff, Jitter, RetryPolicy, asset

retry_policy = RetryPolicy(
    max_retries=3,
    delay=1,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.PLUS_MINUS,
)


def create_write_path(name: str) -> str:
    month_num = datetime.datetime.now().month
    year_num = datetime.datetime.now().year
    path = f"data-lake/bronze/{name}/{year_num}/{month_num}"
    Path(path).mkdir(parents=True, exist_ok=True)
    return path


def download_dataset(url: str, name: str) -> None:
    r = requests.get(url)
    day = datetime.datetime.now().day
    path = create_write_path(name)
    with open(f"{path}/{day}.tsv.tgz", "wb") as f:
        f.write(r.content)


@asset(retry_policy=retry_policy)
def download_akas() -> None:
    url = "https://datasets.imdbws.com/title.akas.tsv.gz"
    download_dataset(url, "title.akas")


@asset(retry_policy=retry_policy)
def download_basics() -> None:
    url = "https://datasets.imdbws.com/title.basics.tsv.gz"
    download_dataset(url, "title.basics")


@asset(retry_policy=retry_policy)
def download_ratings() -> None:
    url = "https://datasets.imdbws.com/title.ratings.tsv.gz"
    download_dataset(url, "title.ratings")
