# Databricks notebook source
# DBTITLE 1,UDF in DF
from pyspark.sql import functions as F
from pyspark.sql import types as T

@F.udf(T.StringType())
def lower_name(name):
    return name.lower()

df = spark.createDataFrame([('Jen', 2), ('Bob', 4), ('Zoe', 5)], ['name', 'age'])
display(df.select(lower_name(df.name).alias('lower_name'), '*'))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- EXPLAIN FORMATTED 
# MAGIC
