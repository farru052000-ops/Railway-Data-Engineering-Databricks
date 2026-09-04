# Databricks notebook source
#Load the Silver table
from pyspark.sql.functions import col, sum, trim
silver_df = spark.table("workspace.default.silver_railway")
print("Silver record count:", silver_df.count())
display(silver_df.limit(10))

# COMMAND ----------

#Check missing values
null_check = silver_df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in silver_df.columns
])
display(null_check)

# COMMAND ----------

#Check duplicates
total_records = silver_df.count()
distinct_records = silver_df.distinct().count()
duplicate_records = total_records - distinct_records
print("Total records:", total_records)
print("Distinct records:", distinct_records)
print("Duplicate records:", duplicate_records)

# COMMAND ----------

#Check empty station names
empty_station_check = silver_df.filter(
    (trim(col("Source_Station_Name")) == "") |
    (trim(col("Destination_Station_Name")) == "")
)

print(
    "Records with empty station names:",
    empty_station_check.count()
)

# COMMAND ----------

#Check invalid days
valid_days = [
    "Friday", "Fridayd",
    "Monday", "Mondayd",
    "Saturday", "Saturdayd",
    "Sunday", "Sundayd",
    "Thursday", "Thursdayd",
    "Tuesday", "Tuesdayd",
    "Wednesday", "Wednesdayd"
]

invalid_days = silver_df.filter(
    ~col("days").isin(valid_days)
)

print("Invalid day records:", invalid_days.count())

display(
    invalid_days.select("days").distinct()
)

# COMMAND ----------

#Overall quality status
from builtins import sum as python_sum
null_values = null_check.collect()[0]

null_total = python_sum(
    null_values[c]
    for c in silver_df.columns
)
invalid_day_count = invalid_days.count()
quality_status = "PASSED"
if null_total > 0:
    quality_status = "FAILED"
if duplicate_records > 0:
    quality_status = "FAILED"
if invalid_day_count > 0:
    quality_status = "FAILED"
print("Total NULL values:", null_total)
print("Duplicate records:", duplicate_records)
print("Invalid day records:", invalid_day_count)
print("DATA QUALITY STATUS:", quality_status)

# COMMAND ----------

#Create the Data Quality Summary Table
quality_summary = spark.createDataFrame(
    [
        (
            total_records,
            distinct_records,
            duplicate_records,
            null_total,
            invalid_day_count,
            quality_status
        )
    ],
    [
        "total_records",
        "distinct_records",
        "duplicate_records",
        "null_records",
        "invalid_day_records",
        "quality_status"
    ]
)

display(quality_summary)

# COMMAND ----------

#Save the Quality Results
quality_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.railway_data_quality")

print("Data quality table created successfully!")

# COMMAND ----------

#Verify the table
quality_check = spark.table(
    "workspace.default.railway_data_quality"
)

display(quality_check)
#Add a clear PASS/FAIL gate
if quality_status == "PASSED":
    print("✅ DATA QUALITY PASSED — Pipeline can continue to GOLD.")
else:
    raise Exception(
        "❌ DATA QUALITY FAILED — Pipeline must stop."
    )