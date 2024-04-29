# Dagster polars ELT example

Simple dagster example project that demonstrates how to do an ELT pipeline. 

The pipeline reads data from the IMDB API, persists it to disk in one job. In another job it reads the data from disk, transforms it, and persists it to disk again. The second time the data is persisted, it is deduplicated and stored in a parquet file.

The pipeline does not scale for obvious reasons, but it is a good starting point for a more complex pipeline. It requires loading all the data into memory, which is not feasible after a couple of runs since one of the datasets is 600MB. This can be resolved with a relatively small amount of work.

The project can also be enhanced by adding tests, logging, monitoring, and an infrastructure component like delta lake. To be continued...

update: 

* The project now persists the data to a minio.db data lake.
* The project is now dockerized.

## How to run the project

1. Clone the project
2. If not present, install docker and docker-compose
3. Run `docker-compose up -d` in the root of the project

