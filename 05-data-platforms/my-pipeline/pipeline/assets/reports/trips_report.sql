/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# TODO: Set the asset name (recommended: reports.trips_report).
name: reports.trip_report

# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependency on the staging asset(s) this report reads from.
depends:
  - staging.trips

# TODO: Choose materialization strategy.
# For reports, `time_interval` is a good choice to rebuild only the relevant time window.
# Important: Use the same `incremental_key` as staging (e.g., pickup_datetime) for consistency.
materialization:
  type: table
  # suggested strategy: time_interval
  strategy: time_interval
  # TODO: set to your report's date column
  incremental_key: trip_date
  # TODO: set to `date` or `timestamp`
  time_granularity: date

# TODO: Define report columns + primary key(s) at your chosen level of aggregation.
columns:
  - name: trip_date
    type: date
    description: "Date of the trip"
    primary_key: true
  - name: taxi_type
    type: string
    description: "Type of taxi (Yellow/Green)"
    primary_key: true
  - name: payment_type_name
    type: string
    description: "Payment method"
    primary_key: true
  - name: total_trips
    type: integer
    description: "Total number of trips"
    checks:
      - name: positive
  - name: total_distance
    type: float
    description: "Total distance traveled"
  - name: avg_distance
    type: float
    description: "Average distance per trip"

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT  -- TODO: replace with your aggregation logic
    CAST(pickup_datetime as DATE) AS trip_date,
    taxi_type,
    payment_type_name,
    COUNT(*) AS total_trips,
    SUM(trip_distance) AS total_distance,
    AVG(trip_distance) AS avg_distance
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY trip_date, taxi_type, payment_type_name