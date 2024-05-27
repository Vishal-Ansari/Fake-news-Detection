# Databricks notebook source
display(dbutils.fs.ls('dbfs:/mnt/blobstorage/dataset_Lenskart/'))

# COMMAND ----------

df_product= spark.read.csv('/mnt/blobstorage/dataset_Lenskart/products_tf.csv',inferSchema=True, header=True)
df_transaction= spark.read.csv('/mnt/blobstorage/dataset_Lenskart/transaction.csv',inferSchema=True, header=True)

# COMMAND ----------

df_product.display()

# COMMAND ----------

df_transaction.display()

# COMMAND ----------

from pyspark.sql.functions import sum, col

df_combined = df_transaction.join(df_product, 'product_id', 'inner')


# COMMAND ----------

df_combined.display()

# COMMAND ----------

from pyspark.sql.functions import sum, col

# Calculate total sales

df_combined = df_transaction.join(df_product, 'product_id', 'inner')
df_sales = df_transaction.join(df_product, 'product_id', 'inner') \
    .select('product_id', (col('quantity') * col(' price')).alias('sales'))

total_sales = df_sales.groupBy().agg(sum('sales')).collect()[0][0]

# Calculate total cost
df_cost = df_transaction.join(df_product, 'product_id', 'inner') \
    .select('product_id', (col('quantity') * col('cost_of_product')).alias('cost'))

total_cost = df_cost.groupBy().agg(sum('cost')).collect()[0][0]

# Calculate gross profit
gross_profit = total_sales - total_cost

print("Total Sales:", total_sales)
print("Total Cost:", total_cost)
print("Gross Profit:", gross_profit)


# COMMAND ----------

import matplotlib.pyplot as plt

# Defining labels and values for the bar chart
labels = ['Total Sales', 'Total Cost', 'Gross Profit']
values = [total_sales, total_cost, gross_profit]

# Create bar chart
plt.figure(figsize=(10, 6))
plt.bar(labels, values, color=['blue', 'orange', 'green'])
plt.xlabel('Metrics')
plt.ylabel('Amount')
plt.title('Sales, Cost, and Gross Profit')
plt.show()


# COMMAND ----------

import matplotlib.pyplot as plt

# Define labels and values for the pie chart
labels = ['Total Sales', 'Total Cost', 'Gross Profit']
values = [total_sales, total_cost, gross_profit]

# Create pie chart
plt.figure(figsize=(8, 8))
plt.pie(values, labels=labels, autopct='%1.1f%%', colors=['blue', 'orange', 'green'])
plt.title('Sales, Cost, and Gross Profit')
plt.show()

