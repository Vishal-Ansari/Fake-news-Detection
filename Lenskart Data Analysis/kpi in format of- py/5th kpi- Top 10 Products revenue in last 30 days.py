# Databricks notebook source
# List the files inside the dataset_Lenskart directory
display(dbutils.fs.ls('dbfs:/mnt/blobstorage/dataset_Lenskart/'))

# COMMAND ----------

df_product= spark.read.csv('/mnt/blobstorage/dataset_Lenskart/products_tf.csv',inferSchema=True, header=True)
df_transaction= spark.read.csv('/mnt/blobstorage/dataset_Lenskart/transaction.csv',inferSchema=True, header=True)

# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum

# Merge transaction and product DataFrames
merged_df = df_transaction.join(df_product,'product_id','inner')

# COMMAND ----------

# Drop duplicates
merged_df = merged_df.dropDuplicates()

# COMMAND ----------

from pyspark.sql import functions as F

# Convert order_date column to datetime
merged_df = merged_df.withColumn("order_date", F.col("order_date").cast("date"))

# Calculate start date and end date for the last 30 days
max_order_date = merged_df.select(F.max("order_date")).collect()[0][0]
start_date = max_order_date - F.expr("INTERVAL 30 DAYS")

# Filter for the last 30 days
last_30_days_df = merged_df.filter((F.col("order_date") >= start_date) & (F.col("order_date") <= max_order_date))


# COMMAND ----------

last_30_days_df.display()

# COMMAND ----------

# Calculate revenue as the product of quantity and price
last_30_days_df = last_30_days_df.withColumn("revenue", F.col("quantity") * F.col(' price'))

# Group by product_id and title, aggregate sum of quantity and revenue
revenue_by_product = last_30_days_df.groupBy("product_id", ' title', ' price') \
    .agg(F.sum("quantity").alias("total_quantity"), F.sum("revenue").alias("total_revenue"))

# Calculate top 10 products by revenue
top_10_products = revenue_by_product.orderBy(F.col("total_revenue").desc()).limit(10)


# COMMAND ----------

top_10_products.display()
