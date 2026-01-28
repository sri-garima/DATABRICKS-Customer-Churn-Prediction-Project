# Databricks notebook source
# MAGIC %md
# MAGIC ## Performance Optimization – Customer Churn Project
# MAGIC
# MAGIC This notebook applies Delta Lake performance optimizations such as Z-ORDER clustering,
# MAGIC file compaction, and caching on Silver and Gold tables to improve query and ML performance.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Optimize Silver Layer**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE customer_churn_project.silver.online_retail_clean
# MAGIC ZORDER BY (CustomerID, InvoiceDate);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Optimize GOLD – Feature Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE customer_churn_project.gold.customer_churn_features
# MAGIC ZORDER BY (CustomerID);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Optimize GOLD – ML Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE customer_churn_project.gold.customer_churn_ml
# MAGIC ZORDER BY (CustomerID);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Cache ML table (for model training)**

# COMMAND ----------

# DBTITLE 1,Cache ML table (for model training)
# MAGIC %sql
# MAGIC -- CACHE TABLE is not supported on serverless compute. See https://docs.databricks.com/en/sql/language-manual/cache-table.html for details.
# MAGIC -- If you need caching, switch to a supported cluster type or optimize queries instead.
# MAGIC -- Original code:
# MAGIC
# MAGIC -- CACHE TABLE customer_churn_project.gold.customer_churn_ml;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance optimization was achieved using Delta Lake OPTIMIZE and Z-ORDER clustering.
# MAGIC Manual caching was intentionally avoided due to the use of Databricks Serverless compute,
# MAGIC which manages memory automatically.The project runs on Databricks Serverless compute, where manual caching is not supported.
# MAGIC Instead, I relied on Delta Lake OPTIMIZE with Z-ORDER for performance improvements.

# COMMAND ----------

# MAGIC %md
# MAGIC **Verify optimization**

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL customer_churn_project.gold.customer_churn_ml;
# MAGIC