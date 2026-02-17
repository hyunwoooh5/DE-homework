## Q1. Bruin Pipeline Structure

In a Bruin project, what are the required files/directories?

Answer: `.bruin.yml` and `pipeline/` with `pipeline.yml` and `assets/`




## Q2. Materialization Strategies

You're building a pipeline that processes NYC taxi data organized by month based on `pickup_datetime`. Which materialization strategy should you use for the staging layer that deduplicates and cleans the data?


Answer: `time_interval` - incremental based on a time column






## Q3. Pipeline Variables

You have the following variable defined in `pipeline.yml`:

```yaml
variables:
  taxi_types:
    type: array
    items:
      type: string
    default: ["yellow", "green"]
```

How do you override this when running the pipeline to only process yellow taxis?

Answer: `bruin run --var 'taxi_types=["yellow"]'`







## Q4. Running with Dependencies

You've modified the `ingestion/trips.py` asset and want to run it plus all downstream assets. Which command should you use?

Answer: `bruin run --select ingestion.trips+`





## Q5. Quality Checks

You want to ensure the `pickup_datetime` column in your trips table never has NULL values. Which quality check should you add to your asset definition?

Answer: `not_null: true`




## Q6. Lineage and Dependencies

After building your pipeline, you want to visualize the dependency graph between assets. Which Bruin command should you use?

Answer: `bruin lineage`


## Q7. First-Time Run

You're running a Bruin pipeline for the first time on a new DuckDB database. What flag should you use to ensure tables are created from scratch?


Answer: `--full-refresh`
