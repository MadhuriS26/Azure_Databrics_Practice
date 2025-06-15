# Databricks notebook source
df = spark.read.table("dbx_catlg.bronze.regions")

df.display()

# COMMAND ----------

df = df.drop('_rescued_data')
df.display()

# COMMAND ----------

df.write.mode("overwrite").save("abfss://silver@dbxhc.dfs.core.windows.net/regions")

# COMMAND ----------

#Read check on all delta tables in Silver layer
df = spark.read.format("delta").load("abfss://silver@dbxhc.dfs.core.windows.net/products")
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dbx_catlg.silver.regions
# MAGIC using delta 
# MAGIC LOCATION 'abfss://silver@dbxhc.dfs.core.windows.net/regions'