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

# DBTITLE 1,register UDF to use in SQL
spark.udf.register("lower_name", lower_name)

# COMMAND ----------

# DBTITLE 1,EXPLAIN SQL query
# MAGIC %sql
# MAGIC EXPLAIN FORMATTED 
# MAGIC select lower(name), count(*) from workspace.bronze.name group by lower(name)

# COMMAND ----------

# DBTITLE 1,EXPLAIN UDF based SQL query
# MAGIC %sql
# MAGIC EXPLAIN FORMATTED 
# MAGIC select lower_name(name), count(*) from workspace.bronze.name group by lower_name(name)
