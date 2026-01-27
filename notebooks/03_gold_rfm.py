# Databricks notebook source
# MAGIC %md
# MAGIC ###🥇 GOLD LAYER – CUSTOMER CHURN FEATURES (RFM)
# MAGIC 🎯 Objective
# MAGIC
# MAGIC Convert transaction-level Silver data into customer-level churn features.
# MAGIC This table will be used for:
# MAGIC
# MAGIC Churn risk analysis
# MAGIC
# MAGIC ML model training
# MAGIC
# MAGIC Dashboards

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧠 FEATURES WE WILL BUILD (RFM)
# MAGIC
# MAGIC Feature& there Meaning
# MAGIC
# MAGIC - recency	   =  Days since last purchase
# MAGIC - frequency=	Number of unique invoices
# MAGIC - monetary=	Total customer spend
# MAGIC - avg_order_value=	monetary / frequency
# MAGIC - tenure_days=	Days between first & last purchase
# MAGIC
# MAGIC These are industry-standard churn features.

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 1: Read Silver Data

# COMMAND ----------

from pyspark.sql import functions as F

df_silver = spark.table(
    "customer_churn_project.silver.online_retail_clean"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 2: Define Reference Date

# COMMAND ----------

reference_date = df_silver.select(
    F.max("InvoiceDate")
).collect()[0][0]


# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 3: Build Customer-Level Aggregates

# COMMAND ----------

df_gold = (
    df_silver
    .groupBy("CustomerID")
    .agg(
        F.max("InvoiceDate").alias("last_purchase_date"),
        F.min("InvoiceDate").alias("first_purchase_date"),
        F.countDistinct("InvoiceNo").alias("frequency"),
        F.sum("total_amount").alias("monetary")
    )
    .withColumn(
        "recency",
        F.datediff(F.lit(reference_date), F.col("last_purchase_date"))
    )
    .withColumn(
        "tenure_days",
        F.datediff(
            F.col("last_purchase_date"),
            F.col("first_purchase_date")
        )
    )
    .withColumn(
        "monetary",
        F.round(F.col("monetary"), 1)
    )
    .withColumn(
        "avg_order_value",
        F.round(F.col("monetary") / F.col("frequency"), 1)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 4: Write Gold Table (Delta + UC)

# COMMAND ----------

(
    df_gold
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(
        "customer_churn_project.gold.customer_churn_features"
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Gold layer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC FROM customer_churn_project.gold.customer_churn_features;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM customer_churn_project.gold.customer_churn_features
# MAGIC LIMIT 10;
# MAGIC