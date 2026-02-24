-- Databricks notebook source
create external location if not exists f1external_bronze
url "abfss://bronze@databricksf1extadls.dfs.core.windows.net/"
with (storage credential f1_credential);

-- COMMAND ----------

desc external location f1external_bronze;

-- COMMAND ----------

