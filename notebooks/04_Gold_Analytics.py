# Databricks notebook source
from pyspark.sql.functions import col, count, lit
silver_df = spark.table("workspace.default.silver_railway")
print("Silver records:", silver_df.count())
display(silver_df.limit(10))

# COMMAND ----------

#Create gold_station_summary
gold_station_summary = (
    silver_df
    .groupBy("Source_Station_Name")
    .agg(
        count("*").alias("total_trains")
    )
    .orderBy(col("total_trains").desc())
)

display(gold_station_summary.limit(20))

# COMMAND ----------

#Save the Gold table
gold_station_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_station_summary")

print("gold_station_summary created successfully!")
display(
    spark.table("workspace.default.gold_station_summary").limit(20)
)

# COMMAND ----------

#Count trains by day
gold_day_summary = (
    silver_df
    .groupBy("day_name")
    .agg(
        count("*").alias("total_trains")
    )
    .orderBy(col("total_trains").desc())
)

display(gold_day_summary)

# COMMAND ----------

#Add percentage of total
total_trains = silver_df.count()
gold_day_summary = gold_day_summary.withColumn(
    "percentage_of_total",
    (col("total_trains") / lit(total_trains) * 100)
)
display(gold_day_summary)


# COMMAND ----------

gold_day_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_day_summary")

print("gold_day_summary created successfully!")
display(
    spark.table("workspace.default.gold_day_summary")
)

# COMMAND ----------

#Create route summary
gold_route_summary = (
    silver_df
    .groupBy(
        "Source_Station_Name",
        "Destination_Station_Name"
    )
    .agg(
        count("*").alias("total_trains")
    )
    .orderBy(col("total_trains").desc())
)

display(gold_route_summary.limit(20))

# COMMAND ----------

#Save the route table
gold_route_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_route_summary")

print("gold_route_summary created successfully!")
display(
    spark.table("workspace.default.gold_route_summary").limit(20)
)

# COMMAND ----------

#Check the number of unique routes
unique_routes = gold_route_summary.count()
print("Unique source-destination routes:", unique_routes)

# COMMAND ----------

#Create Weekday / Weekend category
from pyspark.sql.functions import when
train_category_df = (
    silver_df
    .withColumn(
        "train_category",
        when(
            col("day_name").isin("Saturday", "Sunday"),
            "Weekend"
        )
        .otherwise("Weekday")
    )
)
display(
    train_category_df
    .select(
        "Train_No",
        "Train_Name",
        "day_name",
        "train_category"
    )
    .limit(20)
)


# COMMAND ----------

#Summarize the categories
gold_train_category = (
    train_category_df
    .groupBy("train_category")
    .agg(
        count("*").alias("total_train_records")
    )
    .orderBy(col("total_train_records").desc())
)

display(gold_train_category)

# COMMAND ----------

#Add percentage
gold_train_category = gold_train_category.withColumn(
    "percentage_of_total",
    (
        col("total_train_records") /
        lit(silver_df.count()) * 100
    )
)

display(gold_train_category)

# COMMAND ----------

#Save the Gold table
gold_train_category.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_train_category")

print("gold_train_category created successfully!")

# COMMAND ----------

#Verify all four Gold tables
gold_tables = [
    "gold_station_summary",
    "gold_day_summary",
    "gold_route_summary",
    "gold_train_category"
]

for table in gold_tables:
    table_df = spark.table(f"workspace.default.{table}")
    print(f"{table}: {table_df.count()} records")