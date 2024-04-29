from dagster import graph_asset, op
import polars as pl
from io import StringIO, BytesIO
import polars.selectors as cs
import datetime

from imdb_data.storage import read_bronze, write_to_bucket


def source_data_to_df(source_data: dict[str, str]) -> pl.LazyFrame:
    """
    Convert a dictionary of source data to a Polars DataFrame.

    Args:
        source_data (dict[str, str]): A dictionary where the keys
        are the source names and the values are the file contents.

    Returns:
        pl.LazyFrame: A Polars DataFrame.
    """
    dfs = []
    for source, data in source_data.items():
        memory_file = StringIO(data)
        df = pl.read_csv(memory_file, separator="\t")
        df = df.with_columns(pl.lit(source).alias("source")).lazy()
        dfs.append(df)
    return pl.concat(dfs)


def deduplicate_df(df: pl.LazyFrame) -> pl.LazyFrame:
    """
    Deduplicate a Polars DataFrame. This function will remove all rows that are duplicates.
    This is the case because we download the nearly the same data every day. New data is appended
    to the dataset, but old data is not removed. This function will remove the old data.

    Args:
        df (pl.LazyFrame): The DataFrame to deduplicate.

    Returns:
        pl.LazyFrame: The deduplicated DataFrame.
    """
    all_cols_without_source = cs.all().exclude("source")
    return df.unique(all_cols_without_source)


def write_df_to_parquet(df: pl.LazyFrame, name: str) -> None:
    """
    Write a Polars DataFrame to the silver layer of the data lake.
    This data is cleaned and ready for analysis.

    Args:
        df (pl.LazyFrame): The DataFrame to write.
        name (str): The name of the source. (e.g. "title.akas")
    """
    month_num = datetime.datetime.now().month
    year_num = datetime.datetime.now().year
    day_num = datetime.datetime.now().day
    path = f"silver/{name}/{year_num}/{month_num}-{day_num}.parquet"
    buffer = BytesIO()
    df_ = df.collect()
    df_.write_parquet(buffer)
    buffer.seek(0)
    write_to_bucket(path, buffer)


@op
def akas_from_disk() -> dict[str, str]:
    return read_bronze("title.akas")


@op
def basics_from_disk() -> dict[str, str]:
    return read_bronze("title.basics")


@op
def ratings_form_disk() -> dict[str, str]:
    return read_bronze("title.ratings")


@op
def akas_to_df(source_data: dict[str, str]) -> pl.LazyFrame:
    return source_data_to_df(source_data)


@op
def basics_to_df(source_data: dict[str, str]) -> pl.LazyFrame:
    return source_data_to_df(source_data)


@op
def ratings_to_df(source_data: dict[str, str]) -> pl.LazyFrame:
    return source_data_to_df(source_data)


@op
def deduplicate_akas(df: pl.LazyFrame) -> pl.LazyFrame:
    return deduplicate_df(df)


@op
def deduplicate_basics(df: pl.LazyFrame) -> pl.LazyFrame:
    return deduplicate_df(df)


@op
def deduplicate_ratings(df: pl.LazyFrame) -> pl.LazyFrame:
    return deduplicate_df(df)


@op
def write_akas_to_parquet(df: pl.LazyFrame) -> None:
    write_df_to_parquet(df, "title.akas")


@op
def write_basics_to_parquet(df: pl.LazyFrame) -> None:
    write_df_to_parquet(df, "title.basics")


@op
def write_ratings_to_parquet(df: pl.LazyFrame) -> None:
    write_df_to_parquet(df, "title.ratings")


@graph_asset
def process_akas() -> None:
    df = akas_to_df(akas_from_disk())
    df = deduplicate_akas(df)
    return write_akas_to_parquet(df)


@graph_asset
def process_basics() -> None:
    df = basics_to_df(basics_from_disk())
    df = deduplicate_basics(df)
    return write_basics_to_parquet(df)


@graph_asset
def process_ratings() -> None:
    df = ratings_to_df(ratings_form_disk())
    df = deduplicate_ratings(df)
    return write_ratings_to_parquet(df)
