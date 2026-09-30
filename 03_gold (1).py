# Databricks notebook source
MY_ID = "yourname"
VOL = f"/Volumes/workspace/capstone_{MY_ID}/raw"
CATALOG_SCHEMA = f"workspace.capstone_{MY_ID}"

from pyspark.sql import functions as F

# COMMAND ----------

orders = spark.table(f"{CATALOG_SCHEMA}.silver_orders").withColumn("month", F.trunc("order_purchase_ts", "month"))
items = spark.table(f"{CATALOG_SCHEMA}.silver_items")
sellers = spark.table(f"{CATALOG_SCHEMA}.silver_sellers")
reviews = spark.table(f"{CATALOG_SCHEMA}.silver_reviews")

# COMMAND ----------

# revenue: only priced items (NULL prices excluded), joined to the already-deduped orders
rev = (items.filter("unit_price is not null")
       .withColumn("line_revenue", F.col("quantity") * F.col("unit_price"))
       .join(orders.select("order_id", "month"), "order_id")
       .groupBy("seller_id", "month")
       .agg(F.round(F.sum("line_revenue"), 2).alias("revenue")))

# orders + late: sum numerator and denominator, then divide once (the "trap" in the brief)
ord_agg = (orders.groupBy("seller_id", "month")
           .agg(F.count("order_id").alias("orders"),
                F.sum(F.col("late").cast("int")).alias("late_orders"))
           .withColumn("late_rate", F.col("late_orders") / F.col("orders")))

rev_score = (reviews.join(orders.select("order_id", "seller_id", "month"), "order_id")
             .groupBy("seller_id", "month")
             .agg(F.avg("review_score").alias("avg_review_score")))

# COMMAND ----------

gold = (ord_agg.join(rev, ["seller_id", "month"], "left")
        .join(rev_score, ["seller_id", "month"], "left")
        .join(sellers.select("seller_id", "seller_state"), "seller_id", "left")
        .select("seller_id", "seller_state", "month", "orders", "revenue",
                "late_orders", "late_rate", "avg_review_score"))

gold.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG_SCHEMA}.gold_seller_month")
print("Gold rows:", spark.table(f"{CATALOG_SCHEMA}.gold_seller_month").count())   # expect 240

# COMMAND ----------

g = spark.table(f"{CATALOG_SCHEMA}.gold_seller_month")

print("NULL revenue rows:", g.filter("revenue is null").count())   # expect 0

print("Orders per month (all sellers should show the same pattern):")
g.groupBy("month").agg(F.min("orders").alias("min_orders"), F.max("orders").alias("max_orders")) \
 .orderBy("month").show()                                          # 26, 23, 26, 25, 26, 24

gold_rev = g.agg(F.sum("revenue")).first()[0]
silver_rev = (items.filter("unit_price is not null")
              .agg(F.sum(F.col("quantity") * F.col("unit_price"))).first()[0])
print("Gold revenue:", gold_rev, " Silver revenue:", silver_rev, " diff:", gold_rev - silver_rev)

# COMMAND ----------

(g.coalesce(1).write.mode("overwrite").option("header", True)
   .csv(f"{VOL}/gold_export/gold_seller_month"))
print("exported")

# COMMAND ----------

