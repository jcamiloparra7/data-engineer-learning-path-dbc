# Databricks notebook source
# MAGIC %python
# MAGIC import dlt
# MAGIC from pyspark.sql.functions import *

# COMMAND ----------

json_path = "/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json"

# COMMAND ----------

@dlt.table(
    comment="The raw wikipedia clickstream dataset, ingested from /databricks-datasets." 
)
def clickstream_raw_python():
    return (spark.read.format("json").load(json_path))

# COMMAND ----------

@dlt.table(
    comment="A table containing the top pages linking to the Apache Spark page."
)
@dlt.expect("valid_current_page", "current_page_title IS NOT NULL")
@dlt.expect_or_fail("valid_count", "click_count > 0")

def clickstream_prepared_python():
    return (
    dlt
    .read("clickstream_raw_python")
    .withColumn("current_page_title", col("curr_title"))
    .withColumn("click_count", col("n").cast("integer"))
    .withColumn("previous_page_title", col("prev_title"))
    .select("current_page_title", "click_count", "previous_page_title")
    )

# COMMAND ----------

@dlt.table(
    comment="A table containing the top pages linking to the Apache Spark page."
)
def top_spark_referers_sql_python():
    return ( 
    dlt
    .read("clickstream_prepared_python")
    .withColumn("referrer", col("previous_page_title"))
    .where(col("current_page_title") == "Apache_Spark")
    .select("referrer", "click_count")
    .sort(col("click_count").desc())
    .limit(10)
    )
