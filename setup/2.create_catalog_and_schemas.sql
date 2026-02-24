-- Databricks notebook source
create catalog if not exists f1;

-- COMMAND ----------

show catalogs;

-- COMMAND ----------

use catalog f1;

-- COMMAND ----------

drop schema if exists bronze;

-- COMMAND ----------

create schema if not exists bronze
managed location "abfss://bronze@databricksf1extadls.dfs.core.windows.net/"

-- COMMAND ----------

create schema if not exists silver;

-- COMMAND ----------

create schema if not exists gold;

-- COMMAND ----------

show schemas;