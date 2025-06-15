# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Read**

# COMMAND ----------

df = spark.read.format("parquet").load("abfss://bronze@dbxhc.dfs.core.windows.net/products")
df.display()

# COMMAND ----------

df = df.drop("_rescued_data")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Functions**

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION dbx_catlg.bronze.discount_func(p_price double)
# MAGIC RETURNS double
# MAGIC LANGUAGE SQL
# MAGIC RETURN p_price * 0.90

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, price, dbx_catlg.bronze.discount_func(price) as discounted_price 
# MAGIC from products

# COMMAND ----------

df = df.withColumn("discounted_price",expr("dbx_catlg.bronze.discount_func(price)"))
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION dbx_catlg.bronze.standardize_brandname(p_brand string) 
# MAGIC RETURNS string
# MAGIC LANGUAGE PYTHON 
# MAGIC AS
# MAGIC $$
# MAGIC   return p_brand.upper()
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, brand, dbx_catlg.bronze.standardize_brandname(brand) 
# MAGIC from products

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Write**

# COMMAND ----------

df.write.mode("overwrite").format("delta").save("abfss://silver@dbxhc.dfs.core.windows.net/products")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dbx_catlg.silver.products
# MAGIC using delta 
# MAGIC LOCATION 'abfss://silver@dbxhc.dfs.core.windows.net/products'

# COMMAND ----------

