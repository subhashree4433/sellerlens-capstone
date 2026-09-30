-- 04_snowflake_setup.sql
-- Marketplace Seller Scorecard: Snowflake serving layer

CREATE WAREHOUSE IF NOT EXISTS CAPSTONE_WH
  WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
USE WAREHOUSE CAPSTONE_WH;

CREATE DATABASE IF NOT EXISTS RETAIL_DB;
CREATE SCHEMA IF NOT EXISTS RETAIL_DB.ANALYTICS;
USE SCHEMA RETAIL_DB.ANALYTICS;

-- One external/internal stage pointed at the Databricks Volume export
CREATE STAGE IF NOT EXISTS GOLD_STAGE
  FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' NULL_IF = ('', 'NULL'));
-- Upload gold_seller_month*.csv from the Databricks Volume export into this stage
-- (via SnowSQL PUT, a cloud storage bridge, or the Spark-Snowflake connector).

CREATE TABLE IF NOT EXISTS GOLD_SELLER_MONTH (
  seller_id         STRING,
  seller_state      STRING,
  month             DATE,
  orders            NUMBER,
  revenue           NUMBER(12,2),
  late_orders       NUMBER,
  late_rate         FLOAT,
  avg_review_score  FLOAT
);

COPY INTO GOLD_SELLER_MONTH
  FROM @GOLD_STAGE
  PATTERN = '.*[.]csv'
  ON_ERROR = 'ABORT_STATEMENT';
-- Re-running this COPY INTO on the same file(s) must report 0 files loaded --
-- Snowflake tracks load metadata per stage+file for 64 days. That is the check.

-- Target table the Task merges into (must exist before the Task is created)
CREATE TABLE IF NOT EXISTS GOLD_SELLER_MONTH_SUMMARY LIKE GOLD_SELLER_MONTH;

-- ---------------- Incremental refresh: Stream + Task ----------------
CREATE STREAM IF NOT EXISTS GOLD_SELLER_MONTH_STREAM ON TABLE GOLD_SELLER_MONTH;

CREATE OR REPLACE TASK REFRESH_SELLER_SCORECARD
  WAREHOUSE = CAPSTONE_WH
  SCHEDULE = 'USING CRON 0 * * * * UTC'          -- hourly; adjust to your refresh cadence
  WHEN SYSTEM$STREAM_HAS_DATA('GOLD_SELLER_MONTH_STREAM')
AS
  MERGE INTO GOLD_SELLER_MONTH_SUMMARY tgt
  USING (SELECT * FROM GOLD_SELLER_MONTH_STREAM) src
  ON tgt.seller_id = src.seller_id AND tgt.month = src.month
  WHEN MATCHED THEN UPDATE SET
     tgt.orders = src.orders, tgt.revenue = src.revenue,
     tgt.late_orders = src.late_orders, tgt.late_rate = src.late_rate,
     tgt.avg_review_score = src.avg_review_score
  WHEN NOT MATCHED THEN INSERT (seller_id, seller_state, month, orders, revenue,
     late_orders, late_rate, avg_review_score)
     VALUES (src.seller_id, src.seller_state, src.month, src.orders, src.revenue,
     src.late_orders, src.late_rate, src.avg_review_score);

ALTER TASK REFRESH_SELLER_SCORECARD RESUME;  -- tasks are created suspended

-- ---------------- Access control ----------------
CREATE ROLE IF NOT EXISTS ANALYST_ROLE;
GRANT USAGE ON WAREHOUSE CAPSTONE_WH TO ROLE ANALYST_ROLE;
GRANT USAGE ON DATABASE RETAIL_DB TO ROLE ANALYST_ROLE;
GRANT USAGE ON SCHEMA RETAIL_DB.ANALYTICS TO ROLE ANALYST_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA RETAIL_DB.ANALYTICS TO ROLE ANALYST_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA RETAIL_DB.ANALYTICS TO ROLE ANALYST_ROLE;
-- ANALYST_ROLE is read-only by construction: no INSERT/UPDATE/DELETE grants issued.
