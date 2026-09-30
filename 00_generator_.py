# Databricks notebook source
MY_ID = "yourname"
VOL = f"/Volumes/workspace/capstone_{MY_ID}/raw"

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS workspace")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS workspace.capstone_{MY_ID}")

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS workspace.capstone_{MY_ID}.raw")

# COMMAND ----------

import pyspark.sql.functions as F  # generator seed 42; every count below is exact

def W(d, n):
    d.write.mode('overwrite').option('header', True).csv(f'{VOL}/raw/{n}')

ST = "array('KA','MH','TN','DL','WB','GJ','TG','KL')"
LR = "round(0.02 + 0.33 * (id % 40) / 39, 3)"  # hidden: S001 2% late .. S040 35%

# COMMAND ----------

W(spark.range(40).selectExpr(
    "format_string('S%03d', id + 1) as seller_id",
    f"element_at({ST}, cast(id % 8 as int) + 1) as seller_state",
    "format_string('Hub%02d', id % 8 + 1) as seller_city"), 'sellers')

# COMMAND ----------

o = spark.range(6000).selectExpr("id", "format_string('O%06d', id) as order_id",
    "format_string('S%03d', id % 40 + 1) as seller_id",
    "format_string('C%05d', pmod(hash(id, 1), 4000)) as customer_id",
    "date_add(date'2025-01-01', cast(cast(id / 40 as int) * 181 / 150 as int)) as dy",
    f"{LR} as lr")

# COMMAND ----------

o = o.selectExpr("*", "id >= 5700 as undel", "cast(dy as timestamp) as ts",
    "if(month(dy) >= 4 and id % 4 = 0, least(0.9, lr * 3), lr) as lr2")

# COMMAND ----------

o = o.selectExpr("*", "pmod(hash(id, 2), 1000) < lr2 * 1000 as is_late")

# COMMAND ----------

o = o.selectExpr("id", "order_id", "seller_id", "customer_id", "is_late", "ts",
     "if(undel, 'shipped', 'delivered') as order_status",
     "timestampadd(day, 7, ts) as estimated_delivery_at",
     "if(undel, null, timestampadd(day, if(is_late, 8, 2)"
     " + pmod(hash(id, 3), 4), ts)) as delivered_at")

od = o.selectExpr("id", "order_id", "customer_id", "order_status",
    "ts as order_purchase_ts", "estimated_delivery_at", "delivered_at")
W(od.unionByName(od.filter('id between 1000 and 1119')).drop('id'), 'orders')

# COMMAND ----------

it = o.withColumn('item_no', F.expr('explode(sequence(1, cast(id % 4 + 1 as int)))'))
W(it.selectExpr("order_id", "item_no",
     "if(id between 3000 and 3089 and item_no = 1, 'S999', seller_id) as seller_id",
     "format_string('PR%04d', pmod(hash(id, item_no, 4), 800)) as product_id",
     "pmod(hash(id, item_no, 5), 3) + 1 as quantity",
     "if(id between 4000 and 4199 and item_no = 1, 'NA',"
     " string(round(exp(4.79 + randn(42) * 0.5), 2))) as unit_price",
    "round(15 + pmod(hash(id, item_no, 6), 4000) / 100, 2) as freight_value"), 'order_items')

# COMMAND ----------

W(o.selectExpr("format_string('RV%06d', id) as review_id", "order_id",
    "if(is_late, greatest(1, pmod(hash(id, 7), 5)), pmod(hash(id, 7), 5) + 1) as review_score",
    "timestampadd(day, 12, ts) as review_ts"), 'order_reviews')

print("Generator complete. Files landed under", VOL + "/raw/")

# COMMAND ----------

