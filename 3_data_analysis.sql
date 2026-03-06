-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS gold;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.driver_championship AS
SELECT
    r.year,
    d.driver_id,
    d.driver_name,
    SUM(res.points) AS total_points
FROM silver.results res
JOIN silver.drivers d
ON res.driver_id = d.driver_id
JOIN silver.races r
ON res.race_id = r.race_id
GROUP BY r.year, d.driver_id, d.driver_name
ORDER BY r.year, total_points DESC;

-- COMMAND ----------

SELECT * FROM gold.driver_championship;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS gold;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.constructor_championship AS
SELECT
    r.year,
    c.name AS constructor_name,
    SUM(res.points) AS total_points
FROM silver.results res
JOIN silver.races r
ON res.race_id = r.race_id
JOIN silver.constructors c
ON res.constructor_id = c.constructor_id
GROUP BY r.year, c.name
ORDER BY r.year, total_points DESC;

-- COMMAND ----------

SELECT * FROM gold.constructor_championship;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.driver_performance AS
SELECT
    d.driver_name,
    SUM(res.points) AS total_points,
    SUM(CASE WHEN try_cast(res.position AS INT) = 1 THEN 1 ELSE 0 END) AS total_wins,
    SUM(CASE WHEN try_cast(res.position AS INT) <= 3 THEN 1 ELSE 0 END) AS total_podiums
FROM silver.results res
JOIN silver.drivers d
ON res.driver_id = d.driver_id
GROUP BY d.driver_name
ORDER BY total_points DESC;

-- COMMAND ----------

SELECT * FROM gold.driver_performance
ORDER BY total_points DESC;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.constructor_performance AS
SELECT
    c.name AS constructor_name,
    SUM(res.points) AS total_points,
    SUM(CASE WHEN try_cast(res.position AS INT) = 1 THEN 1 ELSE 0 END) AS total_wins
FROM silver.results res
JOIN silver.constructors c
ON res.constructor_id = c.constructor_id
GROUP BY c.name
ORDER BY total_points DESC;

-- COMMAND ----------

SELECT * 
FROM gold.constructor_performance
ORDER BY total_points DESC;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.yearly_driver_champion AS
SELECT
    r.year,
    d.driver_name,
    SUM(res.points) AS total_points
FROM silver.results res
JOIN silver.drivers d
ON res.driver_id = d.driver_id
JOIN silver.races r
ON res.race_id = r.race_id
GROUP BY r.year, d.driver_name
ORDER BY r.year, total_points DESC;

-- COMMAND ----------

SELECT * 
FROM gold.yearly_driver_champion
ORDER BY year DESC, total_points DESC;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.race_results AS
SELECT
    r.year,
    r.race_name,
    d.driver_name,
    res.position,
    res.points
FROM silver.results res
JOIN silver.drivers d
ON res.driver_id = d.driver_id
JOIN silver.races r
ON res.race_id = r.race_id;

-- COMMAND ----------

SELECT * FROM gold.race_results
LIMIT 10;