# Databricks notebook source
from pyspark.sql.functions import col

day_df = spark.table("workspace.default.gold_day_summary")
station_df = spark.table("workspace.default.gold_station_summary")
route_df = spark.table("workspace.default.gold_route_summary")
category_df = spark.table("workspace.default.gold_train_category")

print("Gold tables loaded successfully!")

# COMMAND ----------

day_pd = (
    day_df
    .orderBy(col("total_trains").desc())
    .toPandas()
)

display(day_pd)

# COMMAND ----------

import matplotlib.pyplot as plt

plt.figure(figsize=(10, 5))

plt.bar(
    day_pd["day_name"],
    day_pd["total_trains"]
)

plt.xlabel("Day")
plt.ylabel("Number of Train Records")
plt.title("Train Journeys by Day")

plt.xticks(rotation=45)
plt.tight_layout()

plt.show()

# COMMAND ----------

top_station_pd = (
    station_df
    .orderBy(col("total_trains").desc())
    .limit(10)
    .toPandas()
)

display(top_station_pd)

# COMMAND ----------

import matplotlib.pyplot as plt

plt.figure(figsize=(12, 6))

plt.barh(
    top_station_pd["Source_Station_Name"],
    top_station_pd["total_trains"]
)

plt.xlabel("Number of Train Records")
plt.ylabel("Source Station")
plt.title("Top 10 Source Stations by Train Records")

plt.gca().invert_yaxis()
plt.tight_layout()

plt.show()

# COMMAND ----------

top_routes_pd = (
    route_df
    .orderBy(col("total_trains").desc())
    .limit(15)
    .toPandas()
)

display(top_routes_pd)

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

route_matrix = top_routes_pd.pivot_table(
    index="Source_Station_Name",
    columns="Destination_Station_Name",
    values="total_trains",
    fill_value=0
)

plt.figure(figsize=(14, 8))

plt.imshow(route_matrix, aspect="auto")

plt.xticks(
    range(len(route_matrix.columns)),
    route_matrix.columns,
    rotation=90
)

plt.yticks(
    range(len(route_matrix.index)),
    route_matrix.index
)

plt.xlabel("Destination Station")
plt.ylabel("Source Station")
plt.title("Top Train Routes Heatmap")

plt.colorbar(label="Number of Train Records")

plt.tight_layout()
plt.show()

# COMMAND ----------

day_order = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday"
]

day_pd_line = (
    day_df
    .toPandas()
)

day_pd_line["day_name"] = pd.Categorical(
    day_pd_line["day_name"],
    categories=day_order,
    ordered=True
)

day_pd_line = day_pd_line.sort_values("day_name")

display(day_pd_line)

# COMMAND ----------

plt.figure(figsize=(10, 5))

plt.plot(
    day_pd_line["day_name"],
    day_pd_line["total_trains"],
    marker="o"
)

plt.xlabel("Day")
plt.ylabel("Number of Train Records")
plt.title("Train Activity Across the Week")

plt.xticks(rotation=45)
plt.tight_layout()

plt.show()

# COMMAND ----------

category_pd = (
    category_df
    .toPandas()
)

display(category_pd)

# COMMAND ----------

plt.figure(figsize=(8, 5))

plt.bar(
    category_pd["train_category"],
    category_pd["total_train_records"]
)

plt.xlabel("Train Category")
plt.ylabel("Number of Train Records")
plt.title("Weekday vs Weekend Train Records")

plt.tight_layout()
plt.show()

# COMMAND ----------

