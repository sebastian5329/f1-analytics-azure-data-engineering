# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

race_results_df=spark.read.table("f1.gold.race_results").filter(f"file_date='{v_file_date}'")

# COMMAND ----------

race_year_list=df_column_to_list(race_results_df,"race_year")

# COMMAND ----------

from pyspark.sql.functions import col
race_results_df=spark.read.table("f1.gold.race_results").filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum,when,count

# COMMAND ----------

driver_standings_df=race_results_df.groupBy("race_year","driver_name","driver_id","driver_nationality").agg(sum("points").alias("total_points"),count(when(col("position")==1,True)).alias("wins"))

# COMMAND ----------

display(driver_standings_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank
driver_rank_spec=Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df=driver_standings_df.withColumn("rank",rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df)

# COMMAND ----------

merge_condition="tgt.driver_id=src.driver_id AND tgt.race_year=src.race_year"
merge_delta_data(final_df,"f1","gold","driver_standings",merge_condition,"race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from f1.gold.driver_standings
# MAGIC where race_year=2021
# MAGIC order by rank;

# COMMAND ----------

