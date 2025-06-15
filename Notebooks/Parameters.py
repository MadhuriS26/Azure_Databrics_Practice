# Databricks notebook source
datasets = [
    {"file_nm" : "orders"},
    {"file_nm" : "customers"},
    {"file_nm" : "products"}
]

# COMMAND ----------

dbutils.jobs.taskValues.set("output_datasets", datasets)