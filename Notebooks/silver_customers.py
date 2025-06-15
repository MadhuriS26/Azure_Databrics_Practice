# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time

# COMMAND ----------

df = spark.read.format("parquet").load("abfss://bronze@dbxhc.dfs.core.windows.net/customers")
df.display()

# COMMAND ----------

df = df.drop('_rescued_data')
df.display()

# COMMAND ----------

df = df.withColumn("domain", split(col("email"), "@")[1])
df.display()

# COMMAND ----------

df1 = df.groupBy("domain").agg(count(col("customer_id")).alias("total_customers")).sort("total_customers",ascending=False)
df1.display()

# COMMAND ----------

df_gmail = df.filter(col("domain") == "gmail.com")
df_gmail.display()

time.sleep(5)

df_yahoo = df.filter(col("domain") == "yahoo.com")
df_yahoo.display()

time.sleep(5)

df_hotmail = df.filter(col("domain") == "hotmail.com")
df_hotmail.display()

# COMMAND ----------

df = df.withColumn("fullname", concat(col("first_name"), lit(" "), col("last_name")))
df = df.drop("first_name", "last_name")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Write**

# COMMAND ----------

df.write.mode("overwrite").format("delta").save("abfss://silver@dbxhc.dfs.core.windows.net/customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dbx_catlg.silver.customers
# MAGIC using delta 
# MAGIC LOCATION 'abfss://silver@dbxhc.dfs.core.windows.net/customers'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dbx_catlg.silver.customers;