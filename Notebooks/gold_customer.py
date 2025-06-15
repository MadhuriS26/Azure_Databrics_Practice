# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

init_load_flag = int(dbutils.widgets.get("init_load_flag"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Read**

# COMMAND ----------

df = spark.sql("select * from dbx_catlg.silver.customers")


# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **De-duplication**

# COMMAND ----------

df = df.dropDuplicates(subset=['customer_id'])
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **New & Old Records**

# COMMAND ----------

if init_load_flag == 0:
  df_old = spark.sql('''select DimCustKey, customer_id, create_date, update_date 
                     from dbx_catlg.gold.DimCustomers''')

else:
    df_old = spark.sql('''select 0 DimCustKey, customer_id, 0 create_date, 0 update_date 
                     from dbx_catlg.silver.customers 
                     where 1 = 0''')


# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Renaming old df columns**

# COMMAND ----------

df_old = df_old.withColumnRenamed("DimCustKey", "old_DimCustKey")
df_old = df_old.withColumnRenamed("customer_id", "old_customer_id")
df_old = df_old.withColumnRenamed("create_date", "old_create_date")
df_old = df_old.withColumnRenamed("update_date", "old_update_date")


# COMMAND ----------

# MAGIC %md
# MAGIC ### **Join old records**

# COMMAND ----------

df_join = df.join(df_old, df.customer_id == df_old.old_customer_id, 'left')
df_join.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### **Separating old vs new records**

# COMMAND ----------

df_new = df_join.filter(df_join.old_DimCustKey.isNull())

df_old = df_join.filter(df_join.old_DimCustKey.isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Prepping Old**

# COMMAND ----------

#df_old = df_old.select('customer_id', 'email', 'city', 'state', 'domain', 'fullname', 'old_create_date', 'old_update_date', 'old_DimCustKey')

# Dropping the unwanted columns
df_old = df_old.drop( 'old_customer_id', "old_update_date")

# Renaming old_create_date & converting the data type of the create_date column to timestamp 
df_old = df_old.withColumnRenamed("old_create_date", "create_date")
df_old = df_old.withColumn("create_date",to_timestamp(col("create_date")))

# Recreate update_date column
df_old = df_old.withColumn("update_date",current_timestamp())

# Renaming old_DimCustKey column
df_old = df_old.withColumnRenamed("old_DimCustKey", "DimCustKey")

df_old = df_old.select('customer_id', 'email', 'city', 'state', 'domain', 'fullname', 'create_date', 'update_date', 'DimCustKey')

df_old.display()






# COMMAND ----------

# MAGIC %md
# MAGIC ### **Prepping new**

# COMMAND ----------

# Dropping the unwanted columns
df_new = df_new.drop('old_DimCustKey', 'old_customer_id', "old_create_date", "old_update_date")

# Recreate create_date and update_date columns
df_new = df_new.withColumn("create_date", current_timestamp())
df_new = df_new.withColumn("update_date", current_timestamp())


# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Add Surrogate Key**

# COMMAND ----------

df_new = df_new.withColumn("DimCustKey",monotonically_increasing_id()+lit(1))
df_new.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### **Max Surrogate Key**

# COMMAND ----------

if init_load_flag == 1:
  max_surrogate_key = 0
else:
  df_maxsk = spark.sql('''select max(DimCustKey) as max_surrogate_key from dbx_catlg.gold.DimCustomers''')
  max_surrogate_key = df_maxsk.collect()[0]['max_surrogate_key']

# COMMAND ----------

df_new = df_new.withColumn("DimCustKey",df_new.DimCustKey+lit(max_surrogate_key))
df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Union df_old and df_new**

# COMMAND ----------

df_final = df_new.union(df_old)


# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **SCD Type1**

# COMMAND ----------

if spark.catalog.tableExists("dbx_catlg.gold.DimCustomers"):

  dlt_obj = DeltaTable.forPath(spark, "abfss://gold@dbxhc.dfs.core.windows.net/DimCustomers")
  dlt_obj.alias("tgt").merge(df_final.alias("src"), "tgt.DimCustKey = src.DimCustKey")\
  .whenMatchedUpdateAll()\
  .whenNotMatchedInsertAll()\
  .execute()
  
else:
  
  df_final.write.mode("overwrite")\
    .format("delta")\
    .option("path", "abfss://gold@dbxhc.dfs.core.windows.net/DimCustomers")\
    .saveAsTable("dbx_catlg.gold.DimCustomers")



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dbx_catlg.gold.DimCustomers

# COMMAND ----------

