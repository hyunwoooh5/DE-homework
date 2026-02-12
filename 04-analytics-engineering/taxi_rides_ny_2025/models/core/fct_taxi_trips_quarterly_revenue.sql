{{ config(materialized='table') }}

-- create a table with quarterly revenue metrics per pickup zone and service type

with quarterly_revenue as (
    select 
        service_type,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
        SUM(total_amount) AS revenue
     from {{ ref('fact_trips') }}
     WHERE EXTRACT(YEAR FROM pickup_datetime) IN (2019, 2020)
     GROUP BY service_type, year, quarter
),
quarterly_growth AS(
    select
        year,
        quarter,
        service_type,
        revenue,
        LAG(revenue) OVER (PARTITION BY service_type, quarter ORDER BY year) AS prev_year_revenue,
    FROM quarterly_revenue
)

SELECT *, (revenue - prev_year_revenue) / NULLIF(prev_year_revenue, 0) AS yoy_growth FROM quarterly_growth