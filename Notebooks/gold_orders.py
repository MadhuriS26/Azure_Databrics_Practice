# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# Data Read

df = spark.sql("select * from dbx_catlg.silver.orders")
df.display()

# COMMAND ----------

df_dimcust = spark.sql("select DimCustKey, customer_id from dbx_catlg.gold.dimcustomers")
df_dimprdct = spark.sql("select product_id as DimPdctKey, product_id from dbx_catlg.gold.dimproducts")

# COMMAND ----------

# Fact df
df_fact = df.join(df_dimcust, df.customer_id == df_dimcust.customer_id, how='left').join(df_dimprdct, df.product_id == df_dimprdct.product_id, how='left')
df_fact_new = df_fact.drop("customer_id","product_id")


# COMMAND ----------

df_fact_new.display()

# COMMAND ----------

# Upsert on fact table

from delta.tables import *

if(spark.catalog.tableExists("dbx_catlg.gold.factorders")):
  
  dlt_obj = DeltaTable.forName(spark, "dbx_catlg.gold.factorders")
  
  dlt_obj.alias("tgt").merge(df_fact_new.alias("src")\
      ,"tgt.order_id = src.order_id AND tgt.DimCustKey = src.DimCustKey AND tgt.DimPdctKey = src.DimPdctKey")\
          .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
                .execute()

else:
  df_fact_new.write\
      .format("delta")\
      .option("path", "abfss://gold@dbxhc.dfs.core.windows.net/factorders")\
      .saveAsTable("dbx_catlg.gold.factorders")
  


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dbx_catlg.gold.factorders