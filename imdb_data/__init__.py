from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)

from . import data_import
from . import cleaning

all_assets = load_assets_from_modules([data_import, cleaning])

import_assets = load_assets_from_modules([data_import], group_name="data_import")
cleaning_assets = load_assets_from_modules([cleaning], group_name="data_cleaning")

asset_download_job = define_asset_job(
    "download_data_job",
    selection=AssetSelection.groups("data_import"),
)

cleaning_job = define_asset_job(
    "cleaning_job",
    selection=AssetSelection.groups("data_cleaning"),
)

cleaning_schedule = ScheduleDefinition(
    name="clean_imdb_data",
    cron_schedule="0 3 * * *",
    job=cleaning_job,
)

import_schedule = ScheduleDefinition(
    name="download_imdb_data",
    cron_schedule="0 0 * * *",
    job=asset_download_job,
)

defs = Definitions(
    assets=all_assets,  # type: ignore
    schedules=[import_schedule, cleaning_schedule],
)
