-- Databricks notebook source
-- MAGIC %sql
-- MAGIC CREATE OR REFRESH LIVE TABLE clickstream_raw_sql
-- MAGIC COMMENT "The raw wikipedia clickstream dataset, ingested from /databricks-datasets."
-- MAGIC AS
-- MAGIC   SELECT
-- MAGIC     *
-- MAGIC   FROM
-- MAGIC     json.`/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json`;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE OR REFRESH LIVE TABLE clickstream_prepared_sql(
-- MAGIC   CONSTRAINT valid_current_page EXPECT (current_page_title IS NOT NULL),
-- MAGIC   CONSTRAINT valid_count EXPECT (click_count > 0) ON VIOLATION FAIL UPDATE
-- MAGIC )
-- MAGIC COMMENT "Wikipedia clickstream data cleaned and prepared for analysis."
-- MAGIC AS SELECT
-- MAGIC   curr_title AS current_page_title,
-- MAGIC   CAST(n AS INT) AS click_count,
-- MAGIC   prev_title AS previous_page_title
-- MAGIC FROM live.clickstream_raw_sql;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE top_spark_referers_sql
COMMENT "A table containing the top pages linking to the Apache Spark page."
AS SELECT
  previous_page_title as referrer,
  click_count
FROM
  live.clickstream_prepared_sql
WHERE
  current_page_title = 'Apache_Spark'
ORDER BY
  click_count DESC
LIMIT 10
