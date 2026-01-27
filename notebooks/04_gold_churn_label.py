# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## **Business Logic: Customer Churn Label Creation**
# MAGIC
# MAGIC **This step loads the gold feature table and creates a churn label for each customer. The churn label is set to 1 if the customer's recency (days since last purchase) is greater than 90, indicating potential churn, and 0 otherwise. The resulting DataFrame is saved as an ML-ready Delta table for further modeling and analysis.**

# COMMAND ----------

# DBTITLE 1,Cell 1
from pyspark.sql import functions as F

# Load Gold features table
df_gold = spark.table("customer_churn_project.gold.customer_churn_features")

# Create churn label
df_churn = (
    df_gold
    .withColumn(
        "churn",
        F.when(F.col("recency") > 90, F.lit(1)).otherwise(F.lit(0))
    )
)

# Save ML-ready table
df_churn.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("customer_churn_project.gold.customer_churn_ml")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation check for churn numbers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT churn, COUNT(*) AS customer_count
# MAGIC FROM customer_churn_project.gold.customer_churn_ml
# MAGIC GROUP BY churn;
# MAGIC