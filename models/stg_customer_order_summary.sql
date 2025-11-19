{{ config(enabled = false) }}

WITH

raw_customers AS (
   SELECT
       id AS customer_id,
       first_name,
       last_name,
       email_address,
       phone_number,
       created_at AS signup_date
   FROM raw.database.orders
   -- Select all customers regardless of status or signup date for simplicity
),


-- Get order data
raw_orders AS (
   SELECT
       o.id AS order_id,
       o.customer_id,
       o.order_date,
       o.order_status,
       o.total_amount
   FROM {{ source('raw', 'orders') }} o
   WHERE o.order_status IN ('completed', 'shipped')
),


order_totals AS (
   SELECT
       raw_orders.customer_id,
       COUNT(raw_orders.order_id) AS total_orders,
       SUM(raw_orders.total_amount) AS total_spent,
       MIN(raw_orders.order_date) AS first_order_date,
       MAX(raw_orders.order_date) AS last_order_date
   FROM raw_orders
   GROUP BY raw_orders.customer_id
),


customer_data AS (
   SELECT
       raw_customers.customer_id,
       CONCAT(raw_customers.first_name, ' ', raw_customers.last_name) AS full_name,
       raw_customers.email_address,
       raw_customers.phone_number,
       order_totals.total_orders,
       order_totals.total_spent,
       order_totals.first_order_date,
       order_totals.last_order_date
   FROM raw_customers
   LEFT JOIN order_totals ON raw_customers.customer_id = order_totals.customer_id
)


SELECT
   customer_data.customer_id,
   customer_data.full_name,
   customer_data.email_address,
   customer_data.phone_number,
   customer_data.total_orders,
   customer_data.total_spent,
   CASE
       WHEN customer_data.total_spent > 500 THEN 'Premium'
       ELSE 'Standard'
   END AS customer_type
FROM customer_data
ORDER BY customer_data.total_spent DESC;
