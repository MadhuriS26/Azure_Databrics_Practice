# Databricks notebook source
# MAGIC %md
# MAGIC ### **Parameterizing Container Name**

# COMMAND ----------

dbutils.widgets.text("file_nm", "")
p_file_nm = dbutils.widgets.get("file_nm")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Streaming Data Read**

# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", f"abfss://bronze@dbxhc.dfs.core.windows.net/checkpoint_{p_file_nm}") \
    .load(f"abfss://source@dbxhc.dfs.core.windows.net/{p_file_nm}")
    



# COMMAND ----------

# MAGIC %md
# MAGIC ### **Streaming Data Write**

# COMMAND ----------

df.writeStream.format("parquet")\
    .option("checkpointLocation", f"abfss://bronze@dbxhc.dfs.core.windows.net/checkpoint_{p_file_nm}")\
    .option("path", f"abfss://bronze@dbxhc.dfs.core.windows.net/{p_file_nm}")\
    .trigger(once=True)\
    .start()


# COMMAND ----------

# MAGIC %md
# MAGIC ### **Display Written Data**

# COMMAND ----------

df1 = spark.read.format("parquet").load(f"abfss://bronze@dbxhc.dfs.core.windows.net/{p_file_nm}")
df1.display()