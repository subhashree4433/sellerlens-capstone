# Databricks notebook source
MY_ID = "yourname"
CATALOG_SCHEMA = f"workspace.capstone_{MY_ID}"

from pyspark.sql import functions as F, Window as W

# COMMAND ----------

orders_c = spark.table(f"{CATALOG_SCHEMA}.bronze_orders").selectExpr(
    "order_id", "customer_id", "order_status",
    "try_cast(order_purchase_ts as timestamp) as order_purchase_ts",
    "try_cast(estimated_delivery_at as timestamp) as estimated_delivery_at",
    "try_cast(delivered_at as timestamp) as delivered_at",
    "_source_file", "_ingested_at", "_row_hash")

items_c = spark.table(f"{CATALOG_SCHEMA}.bronze_order_items").selectExpr(
    "order_id", "try_cast(item_no as int) as item_no", "seller_id", "product_id",
    "try_cast(quantity as int) as quantity",
    "try_cast(unit_price as double) as unit_price",
    "try_cast(freight_value as double) as freight_value",
    "_source_file", "_ingested_at", "_row_hash")

print("NULL unit_price after try_cast:", items_c.filter("unit_price is null").count())   # expect 200
print("NULL delivered_at:", orders_c.filter("delivered_at is null").count())            # expect 300 (x1 or a bit more with duplicates)

# COMMAND ----------

before = orders_c.count()
w = W.partitionBy("order_id").orderBy(F.col("_ingested_at").desc(), F.monotonically_increasing_id().asc())
silver_orders = orders_c.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
after = silver_orders.count()
print("orders before:", before, " after:", after, " dropped:", before - after)   # 6120 -> 6000

# COMMAND ----------

sellers = spark.table(f"{CATALOG_SCHEMA}.bronze_sellers")

silver_rejects = (items_c.join(sellers.select("seller_id"), "seller_id", "left_anti")
                  .withColumn("reason", F.lit("seller_id not in master (orphan)")))
silver_items = items_c.join(sellers.select("seller_id"), "seller_id", "left_semi")

print("items on disk:", items_c.count(), " rejected:", silver_rejects.count(), " silver_items:", silver_items.count())
# expect 15000, 90, 14910

# COMMAND ----------

# the orders file has no seller_id, so derive it from the order number: seller = n % 40 + 1
silver_orders = silver_orders.withColumn(
    "seller_id", F.format_string("S%03d", (F.substring("order_id", 2, 6).cast("int") % 40) + 1))

silver_orders = silver_orders.withColumn(
    "late", F.col("delivered_at").isNotNull() & (F.col("delivered_at") > F.col("estimated_delivery_at")))

silver_reviews = spark.table(f"{CATALOG_SCHEMA}.bronze_order_reviews").selectExpr(
    "review_id", "order_id",
    "try_cast(review_score as int) as review_score",
    "try_cast(review_ts as timestamp) as review_ts")

for name, df in [("silver_orders", silver_orders), ("silver_items", silver_items),
                 ("silver_rejects", silver_rejects), ("silver_sellers", sellers),
                 ("silver_reviews", silver_reviews)]:
    df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG_SCHEMA}.{name}")
    print("written", name)

# COMMAND ----------

print("silver_orders :", spark.table(f"{CATALOG_SCHEMA}.silver_orders").count())    # 6000
print("silver_items  :", spark.table(f"{CATALOG_SCHEMA}.silver_items").count())      # 14910
print("silver_rejects:", spark.table(f"{CATALOG_SCHEMA}.silver_rejects").count())    # 90
print("NULL prices   :", spark.table(f"{CATALOG_SCHEMA}.silver_items").filter("unit_price is null").count())  # 200

# COMMAND ----------

