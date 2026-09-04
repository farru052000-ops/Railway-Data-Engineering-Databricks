# Railway Data Engineering Architecture

## Overview

This project implements an end-to-end Azure-style data engineering solution using Databricks Free Edition.

The architecture follows a layered data engineering approach:

Raw Data → Bronze → Silver → Data Quality → Gold → SQL → Dashboard

## Architecture Flow

```text
Railway_info.csv
       |
       v
Databricks Volume
(Raw / Landing)
       |
       v
Bronze Delta Table
bronze_railway
       |
       v
Silver Delta Table
silver_railway
       |
       v
Data Quality Validation
railway_data_quality
       |
       v
Gold Analytical Tables
       |
       +-----------------------+
       |           |           |
       v           v           v
Station       Day Summary   Route Summary
Summary
       |
       v
Databricks SQL
       |
       v
AI/BI Dashboard
       |
       v
Business Insights
Data Layers
Raw / Landing

The source CSV file is uploaded to a Databricks Volume.

Source:
Railway_info.csv

Databricks location:
workspace.default.railway_raw

The raw layer represents the landing zone that would typically be implemented using Azure Data Lake Storage Gen2 in an Azure environment.

Bronze Layer

The Bronze layer stores the ingested source data as a Delta table.

Table:
workspace.default.bronze_railway

Additional ingestion metadata is captured, including ingestion timestamp and source file information.

Silver Layer

The Silver layer contains cleaned and standardized data.

Transformations include:

Trimming text fields
Converting station names to uppercase
Standardizing day information
Creating day_name
Creating day_type_flag
Preserving the original source values

Table:
workspace.default.silver_railway

Data Quality Layer

The pipeline validates the Silver data before analytical processing.

Checks include:

NULL values
Duplicate records
Empty station names
Invalid day values
Overall pipeline quality status

Table:
workspace.default.railway_data_quality

The pipeline stops if data quality validation fails.

Gold Layer

The Gold layer contains business-ready analytical datasets.

Tables:

gold_station_summary
gold_day_summary
gold_route_summary
gold_train_category

These tables are optimized for analysis, reporting and dashboard consumption.

Orchestration

The pipeline is implemented using a Databricks Lakeflow Job.

Pipeline:

Bronze Ingestion
       ↓
Silver Transformation
       ↓
Data Quality
       ↓
Gold Analytics

The workflow is configured with a daily scheduled trigger.

Analytics and Reporting

Databricks SQL is used to query the analytical datasets.

The project includes SQL analysis for:

Executive KPIs
Top source stations
Train activity by day

The results are presented through a Databricks AI/BI Dashboard.

Azure-to-Databricks Mapping
Azure Service / Concept	Databricks Implementation
Azure Data Lake Storage Gen2	Databricks Volume
Azure Data Factory	Databricks Lakeflow Job
Azure Databricks	Databricks Free Edition
Delta Lake	Delta Tables
Azure Synapse Analytics	Databricks SQL
Power BI	Databricks AI/BI Dashboard
Azure Monitor	Job and Data Quality Monitoring Concepts

Note: This project was developed in Databricks Free Edition. The Azure services above represent the equivalent production architecture rather than actual Azure resources deployed in this project.

Business Use Cases

The solution supports:

Railway station activity analysis
Train activity by day of week
High-volume route identification
Weekday versus weekend analysis
Operational capacity planning
Data quality monitoring
Stakeholder reporting
Technology Stack
Databricks Free Edition
Apache Spark
PySpark
Delta Lake
Databricks SQL
Databricks Lakeflow Jobs
Databricks AI/BI Dashboard
Python
Matplotlib
Project Outcome

The project demonstrates a complete data engineering lifecycle:

Ingestion → Transformation → Validation → Aggregation → Analytics → Visualization → Reporting
