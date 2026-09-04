# Databricks notebook source
#Load the Bronze table
from pyspark.sql.functions import col, trim, upper
bronze_df = spark.table("workspace.default.bronze_railway")
display(bronze_df.limit(10))

# COMMAND ----------


# Standardize the station names
silver_df = (
    bronze_df
    .withColumn(
        "Source_Station_Name",
        upper(trim(col("Source_Station_Name")))
    )
    .withColumn(
        "Destination_Station_Name",
        upper(trim(col("Destination_Station_Name")))
    )
)

display(silver_df.limit(10))

# COMMAND ----------

display(
    silver_df
    .select("days")
    .distinct()
    .orderBy("days")
)

# COMMAND ----------

#Create standardized day columns

from pyspark.sql.functions import col, trim, upper, regexp_replace

silver_df = (
    silver_df
    .withColumn(
        "days_clean",
        trim(col("days"))
    )
    .withColumn(
        "day_name",
        regexp_replace(trim(col("days")), "d$", "")
    )
)

display(
    silver_df
    .select("days", "days_clean", "day_name")
    .distinct()
    .orderBy("day_name", "days")
)

# COMMAND ----------

from pyspark.sql.functions import when

silver_df = (
    silver_df
    .withColumn(
        "day_type_flag",
        when(col("days").endswith("d"), "D")
        .otherwise("Standard")
    )
)

display(
    silver_df
    .select("days", "day_name", "day_type_flag")
    .distinct()
    .orderBy("day_name", "day_type_flag")
)

# COMMAND ----------

print("Total Silver candidate rows:", silver_df.count())
print("Distinct rows:", silver_df.distinct().count())


# COMMAND ----------

#Remove duplicate records
duplicate_count = (
    silver_df.count() - silver_df.dropDuplicates().count()
)

print("Duplicate rows:", duplicate_count)

# COMMAND ----------


#Check NULL values again
from pyspark.sql.functions import sum

silver_missing = silver_df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in silver_df.columns
])

display(silver_missing)
silver_df.printSchema()

# COMMAND ----------

#Save Silver as a Delta table
silver_table = "workspace.default.silver_railway"
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(silver_table)

print("Silver table created successfully!")
print("Table:", silver_table)

# COMMAND ----------

#Verify Silver
silver_check = spark.table("workspace.default.silver_railway")
print("Silver rows:", silver_check.count())
display(silver_check.limit(10))