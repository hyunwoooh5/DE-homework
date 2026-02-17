"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11




connection: duckdb-default



# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: vendor_id
    type: integer
    description: "TPEP provider code"
    checks:
      - name: not_null

  - name: pickup_datetime
    type: timestamp
    description: "pickup datetime"
    checks:
      - name: not_null

  - name: dropoff_datetime
    type: timestamp
    description: "dropoff datetime"

  - name: passenger_count
    type: integer
    description: "number of passengers"

  - name: trip_distance
    type: float
    description: "trip distance in miles"

  
  - name: taxi_type
    type: string
    description: "yellow or green"

@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python
import pandas as pd
import requests
import os
import json
from datetime import datetime


# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
def materialize():
    """
    TODO: Implement ingestion using Bruin runtime context.

    Required Bruin concepts to use here:
    - Built-in date window variables:
      - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
      - BRUIN_START_DATETIME / BRUIN_END_DATETIME (ISO datetime)
      Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
    - Pipeline variables:
      - Read JSON from BRUIN_VARS, e.g. `taxi_types`
      Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

    Design TODOs (keep logic minimal, focus on architecture):
    - Use start/end dates + `taxi_types` to generate a list of source endpoints for the run window.
    - Fetch data for each endpoint, parse into DataFrames, and concatenate.
    - Add a column like `extracted_at` for lineage/debugging (timestamp of extraction).
    - Prefer append-only in ingestion; handle duplicates in staging.
    """
    # return final_dataframe

    start_date = os.environ.get('BRUIN_START_DATE')
    start_date = pd.to_datetime(start_date)

    vars_str = os.environ.get("BRUIN_VARS", "{}")
    pipeline_vars = json.loads(vars_str)
    taxi_types = pipeline_vars.get("taxi_types", ["yellow", "green"])  # base

    all_trips = []

    for taxi_type in taxi_types:
        year = start_date.year
        month = start_date.month

        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month:02d}.parquet"

        try:
            df = pd.read_parquet(url)

        except Exception as e:
            print(f"Failed to load data from {url}: {e}")
            continue

        if "tpep_pickup_datetime" in df.columns:  # yellow
            df = df.rename(columns={
                "tpep_pickup_datetime": "pickup_datetime",
                "tpep_dropoff_datetime": "dropoff_datetime"
            })
        elif "lpep_pickup_datetime" in df.columns:  # green
            df = df.rename(columns={
                "lpep_pickup_datetime": "pickup_datetime",
                "lpep_dropoff_datetime": "dropoff_datetime"
            })

        df = df[df["pickup_datetime"].dt.date == start_date.date()].copy()

        df["taxi_type"] = taxi_type
        df.columns = [col.lower() for col in df.columns]

        df = df.rename(columns={
            "vendorid": "vendor_id",
            "ratecodeid": "rate_code_id",
            "pulocationid": "pickup_location_id",
            "dolocationid": "dropoff_location_id"
        })

        
        all_trips.append(df)

    if not all_trips:
        return pd.DataFrame()

    final_df = pd.concat(all_trips, ignore_index=True)

    return final_df
