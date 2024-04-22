import gzip
from pathlib import Path
from dagster import graph_asset, op
import polars as pl
from io import StringIO
import polars.selectors as cs
import datetime


def ungzip_file(file_path: str) -> str:
    with gzip.open(file_path, "rt") as f_in:
        return f_in.read()


def read_source(source: str) -> dict[str, str]:
    source = f"data-lake/bronze/{source}"
    source_path = Path(source)
    if not source_path.exists():
        raise ValueError(f"Source {source} does not exist")
    files_to_process = list(source_path.glob("**/*.tsv.tgz"))
    return {source: ungzip_file(str(file_path)) for file_path in files_to_process}


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
    path = f"data-lake/silver/{name}/{year_num}/{month_num}/{day_num}"
    Path(path).mkdir(parents=True, exist_ok=True)
    df.collect().write_parquet(f"{path}/{name}.parquet")


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
