# SDP_PROJECT
📊 Delta Live Tables – Medallion Architecture Pipeline
🚀 Overview

This project demonstrates the design and implementation of Delta Live Tables (DLT) pipelines using a Medallion Architecture (Bronze → Silver → Gold) to process sales, customers, and products data.

The pipeline is built using PySpark and Databricks DLT, enabling scalable, reliable, and incremental data processing for analytics such as sales performance and discount analysis.

🏗️ Architecture
The project follows the 3-layer Medallion Architecture:

🥉 Bronze Layer (Raw Ingestion)

Ingests raw data from source files using Auto Loader (cloudFiles)

Handles:
Schema inference
Schema evolution (addNewColumns)
Streaming ingestion

Adds metadata column:
ingestion_time

Datasets:
customers
sales
products

🥈 Silver Layer (Cleaned & Transformed Data)

Applies data cleaning and transformations

Implements:
CDC (Change Data Capture) using create_auto_cdc_flow
SCD Type 2 for customers
Deduplication and watermarking applied
Data quality checks using expectations

Key Transformations:
Remove duplicates (dropDuplicates)
Apply watermark on ingestion_time
Filter active records (__END_AT IS NULL)
Handle delete and truncate operations

🥇 Gold Layer (Business Aggregations)
Provides analytics-ready datasets
Aggregates and joins data across silver tables

Key Metrics:
Total Sales & Discount per Customer
Top Product Category by Sales Revenue

⚙️ Tech Stack

Databricks Delta Live Tables (DLT)
PySpark
Delta Lake
Auto Loader (cloudFiles)
Structured Streaming


🔄 Data Flow
Raw Files → Bronze Tables → Silver Tables → Gold Views
