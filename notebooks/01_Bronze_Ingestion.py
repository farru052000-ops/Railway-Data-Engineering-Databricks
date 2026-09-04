# Databricks notebook source
print("Railway Data Engineering Project")
print("Spark is working!")
print(spark)

# COMMAND ----------

file_path = "/Volumes/workspace/default/railway_raw/Railway_info.csv"

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(file_path)

display(df)

# COMMAND ----------

df.printSchema()


# COMMAND ----------

print("Number of rows:", df.count())
print("Number of columns:", len(df.columns))
display(df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import col, sum

missing_values = df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in df.columns
])

display(missing_values)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

bronze_df = (
    df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", lit(file_path))
)

display(bronze_df.limit(10))
print("Bronze record count:", bronze_df.count())

# COMMAND ----------

print("Rows in bronze dataframe:", bronze_df.count())
print("Columns:", bronze_df.columns)
bronze_table = "workspace.default.bronze_railway"
bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(bronze_table)
print("Bronze table created successfully!")
print("Table name:", bronze_table)
