-- 05_snowflake_queries.sql
-- The three questions the Gold layer must answer.
USE SCHEMA RETAIL_DB.ANALYTICS;

-- Q1: Top 10 sellers by revenue per month, and how each one's rank moved from the
-- previous month. (Given, worked, in the brief.)
WITH r AS (
   SELECT month, seller_id, revenue,
          RANK() OVER (PARTITION BY month ORDER BY revenue DESC) AS rank_now
   FROM   GOLD_SELLER_MONTH
)
SELECT month, seller_id, revenue, rank_now,
       LAG(rank_now) OVER (PARTITION BY seller_id ORDER BY month) AS rank_prev,
       LAG(rank_now) OVER (PARTITION BY seller_id ORDER BY month)
         - rank_now AS places_gained
FROM   r
QUALIFY rank_now <= 10
ORDER  BY month, rank_now;

-- Q2: Share of orders delivered late, per seller per state.
SELECT seller_state,
       seller_id,
       SUM(orders)               AS orders,
       SUM(late_orders)          AS late_orders,
       SUM(late_orders) / SUM(orders) AS late_share   -- numerators/denominators summed, not averaged
FROM   GOLD_SELLER_MONTH
GROUP  BY seller_state, seller_id
ORDER  BY late_share DESC;

-- Q2b: rolled up to state level only
SELECT seller_state,
       SUM(orders)  AS orders,
       SUM(late_orders) AS late_orders,
       SUM(late_orders) / SUM(orders) AS late_share
FROM   GOLD_SELLER_MONTH
GROUP  BY seller_state
ORDER  BY late_share DESC;

-- Q3: Which sellers' average review score falls in the months their late rate rises?
WITH trend AS (
  SELECT seller_id, month, late_rate, avg_review_score,
         LAG(late_rate) OVER (PARTITION BY seller_id ORDER BY month) AS late_rate_prev,
         LAG(avg_review_score) OVER (PARTITION BY seller_id ORDER BY month) AS review_prev
  FROM GOLD_SELLER_MONTH
)
SELECT seller_id, month, late_rate_prev, late_rate,
       review_prev, avg_review_score
FROM   trend
WHERE  late_rate > late_rate_prev
  AND  avg_review_score < review_prev
ORDER  BY seller_id, month;
