# Databricks notebook source
# MAGIC %md
# MAGIC ## Step 1️⃣ Create new DATABASE (catalog simulation)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS customer_churn_project;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2️⃣ Create schemas (layers)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG customer_churn_project;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3️⃣ Migrate Bronze table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE customer_churn_project.bronze.online_retail
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM ecommerce_catalog.bronze.online_retail;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4️⃣ Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM customer_churn_project.bronze.online_retail;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 Rename the file as raw

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE customer_churn_project.bronze.online_retail_raw
# MAGIC AS
# MAGIC SELECT * FROM customer_churn_project.bronze.online_retail;
# MAGIC

# COMMAND ----------

