## Q1

 
What is count of records for the 2024 Yellow Taxi Data?

```sql
SELECT COUNT(*) FROM `yellow_tripdata_2024_native`;
```

Answer: `20,332,093`


## Q2

Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
What is the **estimated amount** of data that will be read when this query is executed on the External Table and the Table?

```sql
SELECT COUNT(DISTINCT PULocationID) FROM `yellow_tripdata_2024`;

SELECT COUNT(DISTINCT PULocationID) FROM `yellow_tripdata_2024_native`;
```

Answer: `0 MB for the External Table and 155.12 MB for the Materialized Table`


## Q3

Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?

```sql
SELECT PULocationID FROM `yellow_tripdata_2024_native`;

SELECT PULocationID, DOLocationID FROM `yellow_tripdata_2024_native`;
```

Answer: `BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.`


## Q4

How many records have a fare_amount of 0?

```sql
SELECT COUNT(*) FROM `yellow_tripdata_2024_native`
WHERE fare_amount = 0;
```


Answer: `8,333`


## Q5

What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

```sql
CREATE OR REPLACE TABLE `yellow_tripdata_2024_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime) CLUSTER BY VendorID 
AS ( SELECT * FROM `yellow_tripdata_2024`);
```

Answer: `Partition by tpep_dropoff_datetime and Cluster on VendorID`


## Q6


Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?

Choose the answer which most closely matches.

```sql
SELECT COUNT(DISTINCT VendorID) FROM `yellow_tripdata_2024_native`
WHERE tpep_dropoff_datetime >= '2024-03-01' AND tpep_dropoff_datetime <= '2024-03-15';

SELECT COUNT(DISTINCT VendorID) FROM `yellow_tripdata_2024_partitioned`
WHERE tpep_dropoff_datetime >= '2024-03-01' AND tpep_dropoff_datetime <= '2024-03-15';

```

Answer: `310.24 MB for non-partitioned table and 26.84 MB for the partitioned table`


## Q7


Where is the data stored in the External Table you created?


Answer: `GCP Bucket` 


## Q8


It is best practice in Big Query to always cluster your data:


Answer: `False`


## Q9


Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

```sql
`project-93b82bcf-bce9-4928-82e.zoomcamp.yellow_tripdata_2024_native`
```

Answer: `0B`. BigQuery utilizes metadata-only lookups to fetch row counts without scanning actual records.