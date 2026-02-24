# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

drivers_df=spark.read.table("f1.silver.drivers").withColumnsRenamed({"name":"driver_name","number":"driver_number","nationality":"driver_nationality"})

# COMMAND ----------

constructors_df=spark.read.table("f1.silver.constructors").withColumnRenamed("name","team")

# COMMAND ----------

circuits_df=spark.read.table("f1.silver.circuits").withColumnRenamed("location","circuit_location")

# COMMAND ----------

races_df=spark.read.table("f1.silver.races").withColumnsRenamed({"name":"race_name","race_timestamp":"race_date"})

# COMMAND ----------

results_df=spark.read.table("f1.silver.results").filter(f"file_date='{v_file_date}'").withColumnRenamed("time","race_time").withColumnRenamed("race_id","result_race_id").withColumnRenamed("file_date","result_file_date").withColumnRenamed("driver_id","result_driver_id")

# COMMAND ----------

display(results_df)

# COMMAND ----------

race_circuits_df=races_df.join(circuits_df,races_df.circuit_id==circuits_df.circuit_id)

# COMMAND ----------

race_results_df=results_df.join(race_circuits_df,results_df.result_race_id==race_circuits_df.race_id).join(drivers_df,results_df.result_driver_id==drivers_df.driver_id).join(constructors_df,results_df.constructor_id==constructors_df.constructor_id)

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df=race_results_df.select("result_race_id","race_year","race_name","race_date","circuit_location","result_driver_id","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time","points","position","result_file_date").withColumn("created_date",current_timestamp()).withColumnRenamed("result_file_date","file_date").withColumnRenamed("result_race_id","race_id").withColumnRenamed("result_driver_id","driver_id")
display(final_df)

# COMMAND ----------

merge_condition="tgt.driver_id=src.driver_id AND tgt.race_id=src.race_id AND tgt.race_id=src.race_id"
merge_delta_data(final_df,"f1","gold","race_results",merge_condition,"race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,driver_id,count(1)
# MAGIC from f1.gold.race_results
# MAGIC group by race_id,driver_id
# MAGIC order by race_id desc;

# COMMAND ----------

