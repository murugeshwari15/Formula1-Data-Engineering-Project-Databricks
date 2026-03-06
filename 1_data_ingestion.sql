-- Databricks notebook source
-- MAGIC %python
-- MAGIC circuits_df = spark.read.csv(
-- MAGIC     "/Workspace/Users/murugeshwari.n15@gmail.com/formula1-data-engineering-project/f1_data/circuits.csv",
-- MAGIC     header=True,
-- MAGIC     inferSchema=True
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC circuits_df.write.mode("overwrite").saveAsTable("bronze.circuits")

-- COMMAND ----------

SELECT * FROM bronze.circuits;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC drivers_df = spark.read.csv(
-- MAGIC     "/Workspace/Users/murugeshwari.n15@gmail.com/formula1-data-engineering-project/f1_data/drivers.csv",
-- MAGIC     header=True,
-- MAGIC     inferSchema=True
-- MAGIC )
-- MAGIC
-- MAGIC drivers_df.write.mode("overwrite").saveAsTable("bronze.drivers")

-- COMMAND ----------

SHOW TABLES IN bronze;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = [
-- MAGIC "circuits",
-- MAGIC "drivers",
-- MAGIC "races",
-- MAGIC "results",
-- MAGIC "constructors",
-- MAGIC "constructor_results",
-- MAGIC "constructor_standings",
-- MAGIC "driver_standings",
-- MAGIC "lap_times",
-- MAGIC "pit_stops",
-- MAGIC "qualifying",
-- MAGIC "seasons",
-- MAGIC "sprint_results",
-- MAGIC "status"
-- MAGIC ]
-- MAGIC
-- MAGIC for file in files:
-- MAGIC     
-- MAGIC     path = f"/Workspace/Users/murugeshwari.n15@gmail.com/formula1-data-engineering-project/f1_data/{file}.csv"
-- MAGIC     
-- MAGIC     df = spark.read.csv(path, header=True, inferSchema=True)
-- MAGIC     
-- MAGIC     df.write.mode("overwrite").saveAsTable(f"bronze.{file}")

-- COMMAND ----------

SHOW TABLES IN bronze;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS silver;