# Databricks notebook source
# List the files inside the dataset_Lenskart directory
display(dbutils.fs.ls('dbfs:/mnt/blobstorage/dataset_Lenskart/'))

# COMMAND ----------

df_product= spark.read.csv('/mnt/blobstorage/dataset_Lenskart/products_tf.csv',inferSchema=True, header=True)
df_transaction= spark.read.csv('/mnt/blobstorage/dataset_Lenskart/transaction.csv',inferSchema=True, header=True)

# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum

# Merge transaction and product DataFrames
df_combined = df_transaction.join(df_product,'product_id','inner')

# COMMAND ----------

df_combined.display()

# COMMAND ----------

from pyspark.sql.functions import sum, col
from pyspark.sql import functions as F
df_combined = df_combined.withColumn("order_year", F.date_format("order_date", "yyyy"))
df_combined= df_combined.withColumn("revenue",F.col('quantity')*F.col('cost_of_product'))


# COMMAND ----------

df_combined.display()

# COMMAND ----------


from pyspark.sql import functions as F

# Group by 'store_id' and 'order_year' and sum the 'revenue'
df_grouped = df_combined.groupby('store_id', 'order_year').agg(F.sum('revenue').alias('total_revenue'))

df_grouped= df_grouped.orderBy('store_id')



# COMMAND ----------

# Show the resulting DataFrame
df_grouped.display()
