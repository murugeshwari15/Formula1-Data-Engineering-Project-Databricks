-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS silver;

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.drivers AS
SELECT
    driverId AS driver_id,
    driverRef AS driver_ref,
    number,
    code,
    CONCAT(forename, ' ', surname) AS driver_name,
    dob,
    nationality
FROM bronze.drivers;

-- COMMAND ----------

SELECT * FROM silver.drivers;

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.races AS
SELECT
    raceId AS race_id,
    year,
    round,
    circuitId AS circuit_id,
    name AS race_name,
    date
FROM bronze.races;

-- COMMAND ----------

SELECT * FROM silver.races;

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.circuits AS
SELECT
    circuitId AS circuit_id,
    circuitRef AS circuit_ref,
    name AS circuit_name,
    location,
    country
FROM bronze.circuits;

-- COMMAND ----------

SELECT * FROM silver.circuits;

-- COMMAND ----------

SHOW TABLES IN silver;

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.results AS
SELECT
    resultId AS result_id,
    raceId AS race_id,
    driverId AS driver_id,
    constructorId AS constructor_id,
    grid,
    position,
    points,
    laps
FROM bronze.results;

-- COMMAND ----------

SELECT * FROM silver.results;

-- COMMAND ----------

SHOW TABLES IN silver;

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.constructors AS
SELECT
    constructorId AS constructor_id,
    constructorRef AS constructor_ref,
    name,
    nationality
FROM bronze.constructors;

-- COMMAND ----------

SELECT * FROM silver.constructors;

-- COMMAND ----------

SHOW TABLES IN silver;