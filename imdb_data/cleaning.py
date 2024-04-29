from dagster import graph_asset, op
import polars as pl
from io import StringIO, BytesIO
import polars.selectors as cs
import datetime

from imdb_data.storage import read_bronze, write_to_bucket


def read_source(source: str) -> dict[str, str]:
    return read_bronze(source)


def source_data_to_df(source_data: dict[str, str]) -> pl.LazyFrame:
    dfs = []
    for source, data in source_data.items():
        memory_file = StringIO(data)
        df = pl.read_csv(memory_file, separator="\t")
        df = df.with_columns(pl.lit(source).alias("source")).lazy()
        dfs.append(df)
    return pl.concat(dfs)


def deduplicate_df(df: pl.LazyFrame) -> pl.LazyFrame:
    all_cols_without_source = cs.all().exclude("source")
    return df.unique(all_cols_without_source)


def write_df_to_parquet(df: pl.LazyFrame, name: str) -> None:
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
    return read_source("title.akas")


@op
def basics_from_disk() -> dict[str, str]:
    return read_source("title.basics")


@op
def ratings_form_disk() -> dict[str, str]:
    return read_source("title.ratings")


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
