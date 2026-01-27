# Databricks notebook source
# MAGIC %md
# MAGIC 🧹 Silver Layer Business Rules (VERY IMPORTANT)
# MAGIC
# MAGIC We apply real-world retail rules 👇
# MAGIC
# MAGIC ### ✅ 1. Remove cancelled invoices
# MAGIC
# MAGIC InvoiceNo starting with C → cancellation / return
# MAGIC
# MAGIC ### ✅ 2. Remove anonymous customers
# MAGIC
# MAGIC CustomerID IS NULL → cannot predict churn
# MAGIC
# MAGIC ### ✅ 3. Fix data types
# MAGIC
# MAGIC InvoiceDate → TIMESTAMP
# MAGIC
# MAGIC Quantity → INT
# MAGIC
# MAGIC UnitPrice → DOUBLE
# MAGIC
# MAGIC ### ✅ 4. Create revenue column
# MAGIC total_amount = quantity * unit_price
# MAGIC ### 
# MAGIC ### ✅ 5. Remove invalid transactions
# MAGIC
# MAGIC Quantity ≤ 0
# MAGIC
# MAGIC UnitPrice ≤ 0
# MAGIC
# MAGIC total_amount ≤ 0
# MAGIC
# MAGIC 🧠 Why this matters for Churn
# MAGIC
# MAGIC Churn prediction needs:
# MAGIC
# MAGIC Purchase recency
# MAGIC
# MAGIC Purchase frequency
# MAGIC
# MAGIC Monetary value
# MAGIC
# MAGIC ❌ Bad data = ❌ wrong churn prediction

# COMMAND ----------

# DBTITLE 1,Untitled
from pyspark.sql import functions as F

# Read from Bronze
df_bronze = spark.table(
    "customer_churn_project.bronze.online_retail_raw"
)

# Apply Silver transformations
df_silver = (
    df_bronze
    # 1. Remove cancelled invoices
    .filter(~F.col("InvoiceNo").startswith("C"))

    # 2. Remove null customers
    .filter(F.col("CustomerID").isNotNull())

    # 3. Cast data types
    .withColumn("InvoiceDate", F.to_timestamp("InvoiceDate","M/d/yyyy H:mm"))
    .withColumn("Quantity", F.col("Quantity").cast("int"))
    .withColumn("UnitPrice", F.col("UnitPrice").cast("double"))

    # 4. Create revenue
    .withColumn(
        "total_amount",
        F.col("Quantity") * F.col("UnitPrice")
    )

    # 5. Filter invalid records
    .filter(F.col("Quantity") > 0)
    .filter(F.col("UnitPrice") > 0)
    .filter(F.col("total_amount") > 0)
)

# Write to Silver as Delta table
(
    df_silver
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(
        "customer_churn_project.silver.online_retail_clean"
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Validate Silver Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC FROM customer_churn_project.silver.online_retail_clean;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM customer_churn_project.silver.online_retail_clean
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 6
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED customer_churn_project.silver.online_retail_clean;