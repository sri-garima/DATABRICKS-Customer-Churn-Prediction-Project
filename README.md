# DATABRICKS-Customer-Churn-Prediction-Project
This project is done under Databricks-AI challenge organized by Codebasics,Indian Data Club and sponsored by Databricks and the day is 24 january 2026.

# 🛒 Customer Churn Prediction & Retention Analytics Platform

## 📌 Project Overview

Customer churn is one of the biggest challenges for e-commerce businesses. Acquiring new customers is significantly more expensive than retaining existing ones. This project builds an **end-to-end, production-grade churn prediction platform** using **Databricks**, following modern data engineering and analytics best practices.

The solution ingests raw transactional data, transforms it using a **Medallion Architecture (Bronze → Silver → Gold)**, engineers customer behavior features (RFM), trains a machine learning model to predict churn probability, and surfaces actionable business insights through SQL analytics and dashboards.

This project is designed as a **portfolio-ready Databricks capstone**, demonstrating real-world data engineering, analytics, and ML skills.

---

## 🎯 Business Problem

E-commerce platforms often lose customers silently when they become inactive. Without early identification, businesses face:

* Revenue loss
* Increased marketing costs
* Reduced customer lifetime value

**Goal:**

* Predict which customers are likely to churn
* Identify high-risk customers early
* Estimate revenue at risk
* Enable data-driven retention strategies

---

## 🧠 Solution Approach

### Key Idea

Customer churn can be predicted using **behavioral patterns derived from transaction data**. Even with a single transactional table, meaningful customer features can be engineered.

We use **RFM Analysis**:

* **Recency** – How recently a customer made a purchase
* **Frequency** – How often a customer purchases
* **Monetary** – How much a customer spends

These features are used to train a machine learning model that outputs **churn probability per customer**.

---

## 🏗️ Architecture

```
Raw CSV (Online Retail Dataset)
        ↓
Bronze Layer (Delta Lake)
- Raw transactional data
        ↓
Silver Layer
- Cleaned transactions
- Customer-level aggregation
- RFM features
        ↓
Gold Layer
- Churn labels
- Churn predictions
- Revenue at risk
        ↓
Consumption Layer
- SQL Dashboards
- MLflow Models
- Business KPIs
```

---

## 🧱 Data Architecture (Medallion)

### 🥉 Bronze Layer

* Raw ingestion of online retail transactions
* No transformations applied
* Stored as Delta tables for auditability

**Table:**

* `bronze.retail_raw`

---

### 🥈 Silver Layer

* Data cleaning and standardization
* Removal of invalid records (null customers, cancelled invoices)
* Feature engineering

**Tables:**

* `silver.customer_orders_clean`
* `silver.customer_rfm`

**Features Engineered:**

* Recency (days since last purchase)
* Frequency (number of invoices)
* Monetary (total spend)
* Average order value
* Active months

---

### 🥇 Gold Layer

* Business-ready and ML-ready datasets
* Churn labels and predictions

**Tables:**

* `gold.customer_churn_label`
* `gold.churn_predictions`
* `gold.revenue_at_risk`

**Churn Definition:**

```
If last_purchase_date > 180 days ago → churn = 1
Else → churn = 0
```

---

## 🤖 Machine Learning

* **Model Type:** Logistic Regression (baseline), Random Forest (optional)
* **Input Features:** RFM metrics and engagement features
* **Output:** Churn probability (0–1)

### MLflow

* Experiment tracking
* Parameter & metric logging
* Model registration

---

## 📊 Analytics & Dashboards

Key insights generated:

* Overall churn rate
* High-risk customer segments
* Revenue at risk due to churn
* Churn trends over time
* Country-level churn distribution

Dashboards are built using **Databricks SQL** on Gold tables.

---

## ⚙️ Orchestration

The entire pipeline is automated using **Databricks Jobs**:

1. Bronze ingestion
2. Silver transformation
3. Gold aggregation
4. ML model training
5. Dashboard refresh

This ensures reproducibility and production readiness.

---

## 🔐 Governance & Security

* Unity Catalog used for data governance
* Layer-wise access control
* Schema-based permissions

---

## 🛠️ Tech Stack

* Databricks
* Apache Spark (PySpark & SQL)
* Delta Lake
* MLflow
* Unity Catalog
* Databricks Workflows & SQL

---

## 📂 Dataset

**Source:** Online Retail Dataset (Kaggle)

**Core Columns:**

* InvoiceNo
* StockCode
* Description
* Quantity
* InvoiceDate
* UnitPrice
* CustomerID
* Country

---

## 🚀 Outcomes & Business Value

* Early identification of at-risk customers
* Targeted retention strategies
* Reduced revenue loss
* Scalable and production-ready churn prediction pipeline

---

## 📌 Future Enhancements

* Real-time churn scoring using streaming data
* Customer segmentation with clustering
* Automated retention recommendations
* Advanced models (XGBoost)

---

## 👩‍💻 Author

Garima Srivastava

---

⭐ If you found this project useful, feel free to star the repository!

