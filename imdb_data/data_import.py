import datetime
from io import BytesIO
import requests
from dagster import Backoff, Jitter, RetryPolicy, asset

from imdb_data.storage import write_to_bucket

retry_policy = RetryPolicy(
    max_retries=3,
    delay=1,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.PLUS_MINUS,
)


def create_write_path(name: str) -> str:
    """
    Create the path for the file to be written to the bronze layer of the data lake.
    Args:
        name (str): The name of the file. (e.g. "title.akas")

    Returns:
        str: The path to write the file to.
    """
    month_num = datetime.datetime.now().month
    year_num = datetime.datetime.now().year
    day_num = datetime.datetime.now().day
    path = f"bronze/{name}/{year_num}/{month_num}-{day_num}.tsv.gz"
    return path


def download_dataset(url: str, name: str) -> None:
    """
    Download a dataset from a URL and write it to the bronze layer of the data lake.

    Args:
        url (str): The URL of the dataset to download.
        name (str): The name of the file to write.
    """
    response = requests.get(url).content
    path = create_write_path(name)
    response_file = BytesIO(response)
    write_to_bucket(path, response_file)


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
