USE DATABASE POSTGRES_BATCH;
CREATE SCHEMA ANALYTICS;
CREATE SCHEMA SILVER;
CREATE SCHEMA GOLD;

USE SCHEMA ANALYTICS;

-- TOTAL CUSTOMERS PER MONTH BELOW
WITH customers AS (
  SELECT person_id, created_at
  FROM PUBLIC.PERSON_ICEBERG
)

SELECT
  DATE_TRUNC('month', created_at) AS signup_month,
  COUNT(*) AS new_customers
FROM customers
GROUP BY signup_month
ORDER BY signup_month;



-- TOTAL CUSTOMERS PER MONTH ABOVE


-- REVENUE OVER TIME BELOW

WITH revenue_by_week AS (
  SELECT
    DATE_TRUNC('week', created_at) AS revenue_week,
    SUM(total_price) AS total_revenue
  FROM PUBLIC.TRANSACTION_ICEBERG
  GROUP BY revenue_week
)

SELECT *
FROM revenue_by_week
ORDER BY revenue_week;

-- REVENUE OVER TIME ABOVE

--LIFETIME VALUE WITH RANKING

WITH ltv AS (
  SELECT
    fk_person_id,
    SUM(total_price) AS lifetime_value
  FROM TRANSACTION_ICEBERG
  GROUP BY fk_person_id
),
ranked_ltv AS (
  SELECT
    fk_person_id,
    lifetime_value,
    RANK() OVER (ORDER BY lifetime_value DESC) AS rank
  FROM ltv
)

SELECT
  r.rank,
  p.first_name,
  p.last_name,
  r.lifetime_value
FROM ranked_ltv r
JOIN PERSON_ICEBERG p ON p.person_id = r.fk_person_id
WHERE r.rank <= 1000;


--LIFETIME VALUE WITH RANKING







-- Average Order Value Per Customer with Percentile Banding

WITH customer_orders AS (
  SELECT
    fk_person_id,
    COUNT(*) AS order_count,
    SUM(total_price) AS total_spent,
    AVG(total_price) AS avg_order_value
  FROM TRANSACTION_ICEBERG
  GROUP BY fk_person_id
),
banded_customers AS (
  SELECT *,
    NTILE(4) OVER (ORDER BY avg_order_value DESC) AS value_quartile
  FROM customer_orders
)

SELECT
  p.first_name,
  p.last_name,
  c.avg_order_value,
  c.order_count,
  c.total_spent,
  c.value_quartile
FROM banded_customers c
JOIN PERSON_ICEBERG p ON p.person_id = c.fk_person_id
ORDER BY c.avg_order_value DESC;

-- Average Order Value Per Customer with Percentile Banding
