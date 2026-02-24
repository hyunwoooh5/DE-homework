## Q1: What is the start date and end date of the dataset?

```SQL
SELECT
  MIN(trip_pickup_date_time),
  MAX(trip_pickup_date_time)
FROM "taxi_trips"
```

Answer: `2009-06-01 to 2009-07-01`

## Q2: What proportion of trips are paid with credit card?

```SQL
SELECT
    AVG(CASE WHEN payment_type = 'Credit' THEN 1.0 ELSE 0.0 END) * 100 AS credit_card_proportion
FROM "taxi_trips"
```

Answer: `26.66`


## Q3: What is the total amount of money generated in tips?

```SQL
SELECT
  SUM(tip_amt)
FROM "taxi_trips"
```

Answer: `$6,063.41`