## Q1

Within the execution for `Yellow` Taxi data for the year `2020` and month `12`: what is the uncompressed file size (i.e. the output file `yellow_tripdata_2020-12.csv` of the `extract` task)?


Answer: `128.3 MiB`


## Q2

What is the rendered value of the variable `file` when the inputs `taxi` is set to `green`, `year` is set to `2020`, and `month` is set to `04` during execution?

Answer: `green_tripdata_2020-04.csv`



## Q3

How many rows are there for the `Yellow` Taxi data for all CSV files in the year 2020?



```sql
SELECT count(*) FROM public.yellow_tripdata 
WHERE filename like 'yellow_tripdata_2020-%';
```

Answer: `24648499`



## Q4

How many rows are there for the `Green` Taxi data for all CSV files in the year 2020?



```sql
SELECT * FROM public.green_tripdata
WHERE lpep_pickup_datetime >= '2020-01-01' AND lpep_pickup_datetime < '2021-01-01'
```

Answer: 1733998


## Q5

How many rows are there for the `Yellow` Taxi data for the March 2021 CSV file?



```sql

SELECT COUNT(*) FROM public.yellow_tripdata
WHERE tpep_pickup_datetime >= '2021-03-01' AND tpep_pickup_datetime < '2021-04-01'

```

Answer: `1925130`



## Q6

How would you configure the timezone to New York in a Schedule trigger?

Answer: `Add a timezone property set to America/New_York in the Schedule trigger configuration`