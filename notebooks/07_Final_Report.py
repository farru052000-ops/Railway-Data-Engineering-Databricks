# Databricks notebook source
# ============================================================
# RAILWAY DATA ENGINEERING PROJECT
# FINAL STAKEHOLDER REPORT
# ============================================================

from pyspark.sql.functions import col, desc

# ------------------------------------------------------------
# 1. LOAD DATA
# ------------------------------------------------------------

silver_df = spark.table("workspace.default.silver_railway")

station_df = spark.table("workspace.default.gold_station_summary")
day_df = spark.table("workspace.default.gold_day_summary")
route_df = spark.table("workspace.default.gold_route_summary")
category_df = spark.table("workspace.default.gold_train_category")

print("============================================================")
print("RAILWAY DATA ENGINEERING - STAKEHOLDER REPORT")
print("============================================================")


# ------------------------------------------------------------
# 2. EXECUTIVE SUMMARY
# ------------------------------------------------------------

total_records = silver_df.count()

unique_sources = (
    silver_df
    .select("Source_Station_Name")
    .distinct()
    .count()
)

unique_destinations = (
    silver_df
    .select("Destination_Station_Name")
    .distinct()
    .count()
)

print("\nEXECUTIVE SUMMARY")
print("------------------------------------------------------------")
print(f"Total train records: {total_records:,}")
print(f"Unique source stations: {unique_sources:,}")
print(f"Unique destination stations: {unique_destinations:,}")


# ------------------------------------------------------------
# 3. TOP SOURCE STATION
# ------------------------------------------------------------

top_source = (
    station_df
    .orderBy(col("total_trains").desc())
    .first()
)

print("\nTOP SOURCE STATION")
print("------------------------------------------------------------")

if top_source:
    print(
        f"{top_source['Source_Station_Name']} "
        f"with {top_source['total_trains']:,} train records"
    )


# ------------------------------------------------------------
# 4. TOP DESTINATION STATION
# ------------------------------------------------------------

destination_df = (
    silver_df
    .groupBy("Destination_Station_Name")
    .count()
    .withColumnRenamed("count", "total_trains")
    .orderBy(col("total_trains").desc())
)

top_destination = destination_df.first()

print("\nTOP DESTINATION STATION")
print("------------------------------------------------------------")

if top_destination:
    print(
        f"{top_destination['Destination_Station_Name']} "
        f"with {top_destination['total_trains']:,} train records"
    )


# ------------------------------------------------------------
# 5. TRAIN ACTIVITY BY DAY
# ------------------------------------------------------------

print("\nTRAIN ACTIVITY BY DAY")
print("------------------------------------------------------------")

display(
    day_df
    .select(
        "day_name",
        "total_trains",
        "percentage_of_total"
    )
)


# ------------------------------------------------------------
# 6. BUSIEST DAY
# ------------------------------------------------------------

busiest_day = (
    day_df
    .orderBy(col("total_trains").desc())
    .first()
)

print("\nBUSIEST DAY")
print("------------------------------------------------------------")

if busiest_day:
    print(
        f"{busiest_day['day_name']} "
        f"with {busiest_day['total_trains']:,} train records"
    )


# ------------------------------------------------------------
# 7. WEEKDAY VS WEEKEND
# ------------------------------------------------------------

print("\nWEEKDAY VS WEEKEND")
print("------------------------------------------------------------")

display(category_df)


# ------------------------------------------------------------
# 8. TOP 10 SOURCE STATIONS
# ------------------------------------------------------------

print("\nTOP 10 SOURCE STATIONS")
print("------------------------------------------------------------")

display(
    station_df
    .orderBy(col("total_trains").desc())
    .limit(10)
)


# ------------------------------------------------------------
# 9. TOP 10 ROUTES
# ------------------------------------------------------------

print("\nTOP 10 TRAIN ROUTES")
print("------------------------------------------------------------")

display(
    route_df
    .orderBy(col("total_trains").desc())
    .limit(10)
)


# ------------------------------------------------------------
# 10. DATA QUALITY
# ------------------------------------------------------------

quality_df = spark.table(
    "workspace.default.railway_data_quality"
)

print("\nDATA QUALITY RESULTS")
print("------------------------------------------------------------")

display(quality_df)


# ------------------------------------------------------------
# 11. PROJECT ARCHITECTURE
# ------------------------------------------------------------

print("\nPROJECT ARCHITECTURE")
print("------------------------------------------------------------")

print("""
Railway_info.csv
       |
       v
Databricks Volume
       |
       v
BRONZE - bronze_railway
       |
       v
SILVER - silver_railway
       |
       v
DATA QUALITY
       |
       v
GOLD ANALYTICS
       |
       +--------------------------+
       |            |             |
       v            v             v
Station       Day Summary     Route Summary
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
""")


# ------------------------------------------------------------
# 12. AZURE-STYLE ARCHITECTURE MAPPING
# ------------------------------------------------------------

print("\nAZURE-TO-DATABRICKS MAPPING")
print("------------------------------------------------------------")

print("""
Azure Data Lake Storage Gen2  -> Databricks Volume
Azure Data Factory            -> Databricks Lakeflow Job
Azure Databricks              -> Databricks Free Edition
Delta Lake                    -> Delta Tables
Azure Synapse                 -> Databricks SQL
Power BI                      -> Databricks AI/BI Dashboard
Azure Monitor                 -> Job/Data Quality Monitoring
""")


# ------------------------------------------------------------
# 13. BUSINESS RECOMMENDATIONS
# ------------------------------------------------------------

print("\nBUSINESS RECOMMENDATIONS")
print("------------------------------------------------------------")

print("""
1. Monitor high-volume source stations closely because they
   represent important operational hubs.

2. Use day-level train activity to support scheduling,
   staffing and operational planning.

3. Monitor frequently occurring routes for capacity planning
   and service optimization.

4. Compare weekday and weekend activity when planning
   operational resources.

5. Continue automated data-quality validation before Gold
   tables are generated.

6. Use the dashboard as a management-level monitoring layer
   for railway operations.
""")


# ------------------------------------------------------------
# 14. CONCLUSION
# ------------------------------------------------------------

print("\nCONCLUSION")
print("------------------------------------------------------------")

print("""
The Railway Data Engineering project demonstrates an
end-to-end Azure-style data engineering architecture using
Databricks Free Edition.

The solution implements:

- Raw data ingestion
- Bronze Delta storage
- Silver data transformation
- Data quality validation
- Gold analytical tables
- Automated workflow orchestration
- SQL-based analytics
- Interactive dashboard visualization
- Stakeholder-oriented business insights

The architecture provides a structured and scalable foundation
for railway operational analysis while maintaining clear
separation between raw, transformed, validated and analytical
data layers.
""")

print("============================================================")
print("END OF STAKEHOLDER REPORT")
print("============================================================")