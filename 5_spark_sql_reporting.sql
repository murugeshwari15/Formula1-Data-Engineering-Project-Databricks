-- Databricks notebook source
SHOW TABLES IN gold;

-- COMMAND ----------

SELECT * 
FROM gold.driver_championship
LIMIT 5;

-- COMMAND ----------

SELECT * 
FROM gold.driver_championship
LIMIT 5;

-- COMMAND ----------

SELECT driver_name, total_points
FROM gold.driver_championship
ORDER BY total_points DESC;

-- COMMAND ----------

SELECT constructor_name, total_points
FROM gold.constructor_performance
ORDER BY total_points DESC;

-- COMMAND ----------

SELECT constructor_name, total_wins
FROM gold.constructor_performance
ORDER BY total_wins DESC;

-- COMMAND ----------

SELECT d.driver_name, d.total_points AS driver_points,
       c.constructor_name, c.total_points AS constructor_points
FROM gold.driver_championship d
CROSS JOIN gold.constructor_performance c
ORDER BY driver_points DESC, constructor_points DESC;

-- COMMAND ----------

SELECT driver_name, total_points
FROM gold.driver_championship
ORDER BY total_points DESC
LIMIT 1;

-- COMMAND ----------

SELECT constructor_name, total_points
FROM gold.constructor_performance
ORDER BY total_points DESC
LIMIT 1;

-- COMMAND ----------

SELECT constructor_name, total_points
FROM gold.constructor_performance
ORDER BY total_points DESC
LIMIT 1;

-- COMMAND ----------

-- Example: Export top 10 drivers
SELECT driver_name, total_points
FROM gold.driver_championship
ORDER BY total_points DESC
LIMIT 10;

-- COMMAND ----------

SELECT driver_name, total_points
FROM gold.driver_championship
ORDER BY total_points DESC
LIMIT 10;

-- COMMAND ----------

SELECT constructor_name, total_points
FROM gold.constructor_performance
ORDER BY total_points DESC
LIMIT 10;

-- COMMAND ----------

SELECT constructor_name, total_wins
FROM gold.constructor_performance
ORDER BY total_wins DESC
LIMIT 10;

-- COMMAND ----------

SELECT d.driver_name, d.total_points AS driver_points,
       c.constructor_name, c.total_points AS constructor_points
FROM gold.driver_championship d
CROSS JOIN gold.constructor_performance c
ORDER BY driver_points DESC, constructor_points DESC
LIMIT 10;