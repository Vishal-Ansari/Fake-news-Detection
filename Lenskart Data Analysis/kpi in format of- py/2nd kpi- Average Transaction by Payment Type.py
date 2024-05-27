# Databricks notebook source
display(dbutils.fs.ls('dbfs:/mnt/blobstorage/dataset_Lenskart/'))

# COMMAND ----------

df_product = spark.read.csv('dbfs:/mnt/blobstorage/dataset_Lenskart/products_tf.csv',inferSchema=True,header=True)
df_transaction = spark.read.csv('dbfs:/mnt/blobstorage/dataset_Lenskart/transaction.csv',inferSchema=True,header=True)

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql import functions as F
df_combined= df_transaction.join(df_product,'product_id','inner')
df_combined= df_combined.orderBy('store_id')

# COMMAND ----------


df_combined= df_combined.withColumn('revenue',F.col('quantity')*F.col(' price'))

# COMMAND ----------

df_combined.display()

# COMMAND ----------

average_transaction_revenue = df_combined.groupBy('store_id', 'payment_method').avg('revenue')
average_transaction_revenue = average_transaction_revenue.select('store_id', 'payment_method', 'avg(revenue)').withColumnRenamed('avg(revenue)', 'revenue')


# COMMAND ----------

average_transaction_revenue.display()
