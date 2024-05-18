# Databricks notebook source
# List the files inside the dataset_Lenskart directory
display(dbutils.fs.ls('dbfs:/mnt/blobstorage/dataset_Lenskart/'))

# COMMAND ----------

df_product= spark.read.csv('/mnt/blobstorage/dataset_Lenskart/products_tf.csv',inferSchema=True, header=True)
df_transaction= spark.read.csv('/mnt/blobstorage/dataset_Lenskart/transaction.csv',inferSchema=True, header=True)

# COMMAND ----------

from pyspark.sql.functions import col

# Join transaction and product tables
df_combined = df_transaction.join(df_product, 'product_id', 'inner')

# COMMAND ----------

df_combined.display()

# COMMAND ----------

from pyspark.sql import functions as F

# Join the DataFrames on product_id
df_combined = df_product.join(df_transaction, on='product_id', how='inner')

# Calculate sales for each product
df_combined = df_combined.withColumn('sales', F.col('quantity') * F.col(' price'))

# Calculate cost for each product
df_combined = df_combined.withColumn('cost', F.col('quantity') * F.col('cost_of_product'))

# Group by product_id and sum the sales and cost
product_sales = df_combined.groupby('product_id').agg(F.sum('sales').alias('total_sales'), F.sum('cost').alias('total_cost'))

product_sales= product_sales.withColumn('profit',F.col('total_sales')-F.col('total_cost'))


# COMMAND ----------

# Display the result
product_sales.display()

# COMMAND ----------

top_15_product_acc_sales = product_sales.orderBy('total_sales', ascending=False).limit(15)


# COMMAND ----------

top_15_product_acc_sales.display()

# COMMAND ----------

top_15_product_acc_profit = product_sales.orderBy('profit', ascending=False).limit(15)

# COMMAND ----------

top_15_product_acc_profit.display()

# COMMAND ----------

# Joining the top_15_sales with df_combined to get details of top selling products
top_15_products_details_acc_sales = top_15_product_acc_sales.join(df_product, on='product_id', how='inner')

# Sorting the top_15_products_details DataFrame by total_sales in descending order
top_15_products_details_acc_sales = top_15_products_details_acc_sales.orderBy('total_sales', ascending=False)



# Displaying the result
top_15_products_details_acc_sales.display()


# COMMAND ----------

# Joining the top_15_sales with df_combined to get details of top selling products
top_15_products_details_acc_profit = top_15_product_acc_profit.join(df_product, on='product_id', how='inner')

# Sorting the top_15_products_details DataFrame by total_sales in descending order
top_15_products_details_acc_profit = top_15_products_details_acc_profit.orderBy('total_sales', ascending=False)



# Displaying the result
top_15_products_details_acc_profit.display()
