# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

race_results_df=spark.read.table("f1.gold.race_results").filter(f"file_date='{v_file_date}'")

# COMMAND ----------

race_year_list=df_column_to_list(race_results_df,"race_year")

# COMMAND ----------

from pyspark.sql.functions import col
race_results_df=spark.read.table("f1.gold.race_results").filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum,count,col,when

# COMMAND ----------

constructor_standings_df=race_results_df.groupBy("race_year","team").agg(sum("points").alias("total_points"),count(when(col("position")==1,True)).alias("wins"))
display(constructor_standings_df)

# COMMAND ----------

from pyspark.sql.functions import desc,rank

# COMMAND ----------

from pyspark.sql.window import Window
constructor_rank_spec=Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df=constructor_standings_df.withColumn("rank",rank().over(constructor_rank_spec))
display(final_df)

# COMMAND ----------

merge_condition="tgt.team=src.team AND tgt.race_year=src.race_year"
merge_delta_data(final_df,"f1","gold","constructor_standings",merge_condition,"race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_year,count(1)
# MAGIC from f1.gold.constructor_standings
# MAGIC group by race_year
# MAGIC order by race_year desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1.gold.constructor_standings
# MAGIC where race_year=2021
# MAGIC order by rank;

# COMMAND ----------

