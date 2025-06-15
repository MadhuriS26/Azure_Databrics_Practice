# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DLT PIPELINE**

# COMMAND ----------

# Expectations

rules = {
    "rule1": "product_id is not null",
    "rule2": "product_name is not null"
}

# COMMAND ----------

# Stream read on table
@dlt.table()
@dlt.expect_all_or_drop(rules)

def DimProducts_stg():

    df = spark.readStream.table("dbx_catlg.silver.products")
    return df

# COMMAND ----------

# Streaming view
@dlt.view()

def DimProducts_view():
    df = spark.readStream.table("Live.DimProducts_stg")
    return df

# COMMAND ----------

# DimProducts
dlt.create_streaming_table("DimProducts")

# COMMAND ----------

dlt.apply_changes(
    target = "DimProducts",
    source = "Live.DimProducts_view",
    keys = ["product_id"],
    sequence_by="product_id",
    #except_column_list=None,
    stored_as_scd_type=2
)