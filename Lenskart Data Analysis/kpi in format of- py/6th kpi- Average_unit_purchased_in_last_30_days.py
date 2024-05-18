# Databricks notebook source
display(dbutils.fs.ls('dbfs:/mnt/blobstorage/dataset_Lenskart/'))

# COMMAND ----------

df_product = spark.read.csv('dbfs:/mnt/blobstorage/dataset_Lenskart/products_tf.csv',inferSchema=True,header=True)
df_transaction = spark.read.csv('dbfs:/mnt/blobstorage/dataset_Lenskart/transaction.csv',inferSchema=True,header=True)

# COMMAND ----------

from pyspark.sql.functions import col

df_combined= df_transaction.join(df_product,'product_id','inner')

# COMMAND ----------

df_combined.display()

# COMMAND ----------

from pyspark.sql.functions import col
from datetime import datetime, timedelta

# Finding the latest date in the order_date column
latest_date = df_combined.selectExpr("max(order_date) as max_date").collect()[0]["max_date"]

# Calculating the date 30 days ago
date_30_days_ago = latest_date - timedelta(days=30)

# Filter the DataFrame for data from the last 30 days based on the latest date
df_last_30_day = df_combined.filter(col("order_date") >= date_30_days_ago)
df_last_30_day=df_last_30_day.orderBy('order_date')


# COMMAND ----------

# Show the resulting DataFrame
df_last_30_day.display()

# COMMAND ----------

from pyspark.sql import functions as F

# Group by 'product_id' and aggregate the total quantity and average quantity
df_last_30_days_product = df_last_30_day.groupBy('product_id') \
    .agg(F.sum('quantity').alias('total_unit_purchased'), F.round(F.avg('quantity')).alias('average_unit_purchased'))




# COMMAND ----------

# Show the resulting DataFrame
df_last_30_days_product.display()

# COMMAND ----------

df_final= df_last_30_days_product.join(df_product,'product_id','inner')
df_final.display()
