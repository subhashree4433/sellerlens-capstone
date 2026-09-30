# Databricks notebook source
MY_ID = "yourname"
VOL = f"/Volumes/workspace/capstone_{MY_ID}/raw"
CATALOG_SCHEMA = f"workspace.capstone_{MY_ID}"

from pyspark.sql import functions as F

# COMMAND ----------

def land(name):
    raw = spark.read.option("header", True).csv(f"{VOL}/raw/{name}")
    cols = raw.columns
    df = raw.select(
        *[F.col(c).cast("string").alias(c) for c in cols],
        F.col("_metadata.file_path").alias("_source_file")
    )
    df = (df
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_row_hash", F.sha2(F.concat_ws("|", *[F.col(c) for c in cols]), 256)))
    (df.write.format("delta").mode("overwrite")
       .saveAsTable(f"{CATALOG_SCHEMA}.bronze_{name}"))
    print(name, "->", f"{CATALOG_SCHEMA}.bronze_{name}", df.count(), "rows")

# COMMAND ----------

for t in ["sellers", "orders", "order_items", "order_reviews"]:
    land(t)

# COMMAND ----------

