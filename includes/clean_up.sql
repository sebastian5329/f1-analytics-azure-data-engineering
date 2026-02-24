-- Databricks notebook source
show catalogs;

-- COMMAND ----------

use catalog f1;

-- COMMAND ----------

drop schema if exists silver cascade;

-- COMMAND ----------

create schema if not exists silver;

-- COMMAND ----------

drop schema if exists gold cascade;

-- COMMAND ----------

create schema if not exists gold;

-- COMMAND ----------

