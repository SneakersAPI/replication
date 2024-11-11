# Replication

Script used at [SneakersAPI.dev](https://sneakersapi.dev) to replicate data from ClickHouse to PostgreSQL.
This script is used to sync data from ClickHouse to Postgres based on a YAML configuration file.

**Key features:**

- Replicates data from ClickHouse to Postgres.
- Create and ensure indexes are kept in sync.
- Batch processing to improve performance and memory usage.
- Cursor-based processing for time-based data replication.

**About performance:**

Measured from table creation to last upsert, with batch size of 50k rows:

- 800k rows with 5 columns: around 12s, 67k rows/s
- 170k rows with 18 columns: around 6s, 28k rows/s

> Note:
>
> - There is no concurrency yet, everything is single-threaded.
> - This tool was not designed for high volume of data, this solution might not be the best fit for 10M+ rows. However, this might change in the future!

## Configuration

Configuration is done via a YAML file. See `config.example.yml` for reference.

## Running

```bash
go run . [-only=<table_name>] [-config=<path>]
```

- `-only=<table_name>`: Avoid running all tables and only process the one specified.
- `-config=<path>`: Path to the configuration file. Defaults to `config.yml`.

## Docker

```bash
docker build -t replication .
docker run replication [-only=<table_name>] [-config=<path>]
```
