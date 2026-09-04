# Databricks notebook source
#Load the Gold tables
from pyspark.sql.functions import col, desc

station_df = spark.table("workspace.default.gold_station_summary")
day_df = spark.table("workspace.default.gold_day_summary")
route_df = spark.table("workspace.default.gold_route_summary")
category_df = spark.table("workspace.default.gold_train_category")

print("Gold tables loaded successfully!")

# COMMAND ----------

#Overall Railway KPIs
total_trains = silver_df.count() if "silver_df" in globals() else spark.table(
    "workspace.default.silver_railway"
).count()

unique_sources = spark.table(
    "workspace.default.silver_railway"
).select("Source_Station_Name").distinct().count()

unique_destinations = spark.table(
    "workspace.default.silver_railway"
).select("Destination_Station_Name").distinct().count()

print("Total train records:", total_trains)
print("Unique source stations:", unique_sources)
print("Unique destination stations:", unique_destinations)

# COMMAND ----------

#Most common source station
top_source = station_df.orderBy(
    col("total_trains").desc()
).limit(1)

display(top_source)

# COMMAND ----------

#Most common destination station
silver_df = spark.table("workspace.default.silver_railway")

destination_summary = (
    silver_df
    .groupBy("Destination_Station_Name")
    .count()
    .withColumnRenamed("count", "total_trains")
    .orderBy(col("total_trains").desc())
)

display(destination_summary.limit(10))

# COMMAND ----------

#Busiest operating day
display(
    day_df
    .orderBy(col("total_trains").desc())
    .limit(1)
)

# COMMAND ----------

#Least busy operating day
display(
    day_df
    .orderBy(col("total_trains").asc())
    .limit(1)
)

# COMMAND ----------

#Weekday vs Weekend
display(
    category_df
    .orderBy(col("total_train_records").desc())
)

# COMMAND ----------

display(
    route_df
    .orderBy(col("total_trains").desc())
    .limit(10)
)

# COMMAND ----------

#Create a KPI summary
from pyspark.sql.functions import lit

top_source_row = station_df.orderBy(
    col("total_trains").desc()
).first()

top_destination_row = destination_summary.first()

top_day_row = day_df.orderBy(
    col("total_trains").desc()
).first()

kpi_data = [(
    total_trains,
    unique_sources,
    unique_destinations,
    top_source_row["Source_Station_Name"],
    top_source_row["total_trains"],
    top_destination_row["Destination_Station_Name"],
    top_destination_row["total_trains"],
    top_day_row["day_name"],
    top_day_row["total_trains"]
)]

kpi_df = spark.createDataFrame(
    kpi_data,
    [
        "total_train_records",
        "unique_source_stations",
        "unique_destination_stations",
        "top_source_station",
        "top_source_train_count",
        "top_destination_station",
        "top_destination_train_count",
        "busiest_day",
        "busiest_day_train_count"
    ]
)

display(kpi_df)