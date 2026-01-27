# Databricks notebook source
# MAGIC %md
# MAGIC ### STEP 1: LOAD ML DATA

# COMMAND ----------

df = spark.table("customer_churn_project.gold.customer_churn_ml")
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 2: TRAIN–TEST SPLIT

# COMMAND ----------

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)


# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 3: FEATURE VECTOR ASSEMBLY

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

feature_cols = ["recency", "frequency", "monetary"]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features"
)

train_vec = assembler.transform(train_df)
test_vec = assembler.transform(test_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 4: TRAIN LOGISTIC REGRESSION (Baseline)

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(
    featuresCol="features",
    labelCol="churn"
)

model = lr.fit(train_vec)


# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 5: PREDICTIONS

# COMMAND ----------

# DBTITLE 1,Cell 10
predictions = model.transform(test_vec)
display(predictions.select("CustomerID", "churn", "prediction", "probability"))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 6: MODEL EVALUATION

# COMMAND ----------

from pyspark.ml.evaluation import BinaryClassificationEvaluator

evaluator = BinaryClassificationEvaluator(
    labelCol="churn",
    metricName="areaUnderROC"
)

auc = evaluator.evaluate(predictions)
print("AUC:", auc)


# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 7: MLflow TRACKING

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS customer_churn_project.mlflow;
# MAGIC CREATE VOLUME IF NOT EXISTS customer_churn_project.mlflow.tmp;

# COMMAND ----------

# DBTITLE 1,Cell 15

import mlflow
import mlflow.spark

mlflow.set_experiment("/Shared/customer_churn_ml")

with mlflow.start_run():
    mlflow.log_param("model_type", "LogisticRegression")
    mlflow.log_param("features", feature_cols)
    mlflow.log_metric("AUC", auc)

    mlflow.spark.log_model(
        model,
        artifact_path="churn_model",
        dfs_tmpdir="/Volumes/customer_churn_project/mlflow/tmp/"
    )
