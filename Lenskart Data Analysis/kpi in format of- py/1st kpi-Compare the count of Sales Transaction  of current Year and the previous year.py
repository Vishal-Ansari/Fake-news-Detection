# Databricks notebook source
display(dbutils.fs.ls('dbfs:/mnt/blobstorage/dataset_Lenskart/'))

# COMMAND ----------

from pyspark.sql.functions import year, to_date

df_transaction = spark.read.format("csv").option("header", "true").load("dbfs:/mnt/blobstorage/dataset_Lenskart/transaction.csv")

# adding year column to perform kpi on yearly basis
df_transaction = df_transaction.withColumn("order_year", year(to_date("order_date", "dd-MM-yyyy")))


# COMMAND ----------

df_transaction.display()

# COMMAND ----------

from pyspark.sql import functions as F

# group by the data on store id and order id 
grouped_df = df_transaction.groupBy('store_id', 'order_year')

result_df = grouped_df.agg(F.count('transaction_id').alias('transaction_count'))
result_df= result_df.orderBy('store_id')

result_df.display()


# COMMAND ----------


current_year = 2024
previous_year = current_year - 1

# Filter data for current year and previous year
current_year_transactions = df_transaction.filter(df_transaction["order_year"] == current_year).count()
previous_year_transactions = df_transaction.filter(df_transaction["order_year"] == previous_year).count()

# Display results
print("Current Year Transactions:", current_year_transactions)
print("Previous Year Transactions:", previous_year_transactions)

# COMMAND ----------

import matplotlib.pyplot as plt

# Define data for visualization
years = ['Current Year', 'Previous Year']
transaction_counts = [current_year_transactions, previous_year_transactions]

# Create bar chart
plt.figure(figsize=(8, 6))
plt.bar(years, transaction_counts, color=['blue', 'orange'])
plt.xlabel('Year')
plt.ylabel('Transaction Count')
plt.title('Comparison of Transaction Counts between Current and Previous Year')
plt.show()

