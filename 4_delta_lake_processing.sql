-- Databricks notebook source
CREATE TABLE gold.driver_standings_delta
USING DELTA
AS
SELECT * FROM gold.race_results;

-- COMMAND ----------

SHOW TABLES IN gold;

-- COMMAND ----------

USE gold;

-- COMMAND ----------

SELECT *
FROM race_results
LIMIT 10;

-- COMMAND ----------

SELECT driver_name,
       COUNT(*) AS total_races
FROM gold.race_results
GROUP BY driver_name
ORDER BY total_races DESC;