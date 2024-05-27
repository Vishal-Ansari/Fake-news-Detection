# Databricks notebook source
display(dbutils.fs.ls('dbfs:/mnt/blobstorage/dataset_Lenskart/'))

# COMMAND ----------

df_product = spark.read.csv('dbfs:/mnt/blobstorage/dataset_Lenskart/products_tf.csv',inferSchema=True,header=True)
df_transaction = spark.read.csv('dbfs:/mnt/blobstorage/dataset_Lenskart/transaction.csv',inferSchema=True,header=True)
df_stores = spark.read.csv('dbfs:/mnt/blobstorage/dataset_Lenskart/stores_tf.csv',inferSchema=True,header=True)

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql import functions as F
df_combined_tr_pd= df_transaction.join(df_product,'product_id','inner')

# COMMAND ----------

df_combined= df_combined_tr_pd.join(df_stores,'store_id','inner')
df_combined.display()

# COMMAND ----------

df_combined= df_combined.withColumn('sales',F.col('quantity') * F.col(' price'))

# Calculate sales for each product
df_combined = df_combined.withColumn('sales', F.col('quantity') * F.col(' price'))

# Calculate cost for each product
df_combined = df_combined.withColumn('cost', F.col('quantity') * F.col('cost_of_product'))

# Group by product_id and sum the sales and cost
df_combined = df_combined.groupBy('store_id').agg(F.sum('sales').alias('total_sales'), F.sum('cost').alias('total_cost'))

df_combined= df_combined.withColumn('profit',F.col('total_sales')-F.col('total_cost'))

# COMMAND ----------

df_combined=df_combined.orderBy('profit')

# COMMAND ----------

df_combined.display();

# COMMAND ----------


median_profit = df_combined.approxQuantile('profit', [0.5], 0.0)[0]

print("Median profit:", median_profit)


# COMMAND ----------

from pyspark.sql.functions import lit

df_store_sales_gt_median = df_combined.filter(df_combined["profit"] > median_profit)

df_store_sales_gt_median = df_store_sales_gt_median.withColumn("Threshold_value",lit(median_profit))


# COMMAND ----------

df_store_sales_gt_median.display()

# COMMAND ----------

from pyspark.sql.functions import expr
from pyspark.sql.functions import lit
df_combined = df_combined.withColumn("median_profit",lit(median_profit))
df_store_status = df_combined.withColumn("store_status", expr("CASE WHEN profit > {0} THEN 'profitable' ELSE 'non_profitable' END".format(median_profit)))



# COMMAND ----------

df_store_status.display()

# COMMAND ----------

df_final= df_store_status.join(df_stores,'store_id','inner')
df_final.display()
