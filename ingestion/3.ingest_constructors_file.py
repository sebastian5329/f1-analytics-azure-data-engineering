# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest constructors file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

display(spark.read.json(f"abfss://bronze@databricksf1extadls.dfs.core.windows.net/{v_file_date}/constructors.json"))


# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

constructors_schema=StructType(fields=[StructField("constructorId",IntegerType(),False),
                                       StructField("constructorRef",StringType(),True),
                                       StructField("name",StringType(),True),
                                       StructField("nationality",StringType(),True),
                                       StructField("url",StringType(),True)])

# COMMAND ----------

constructors_df=spark.read.option("header",True).schema(constructors_schema).json(f"abfss://bronze@databricksf1extadls.dfs.core.windows.net/{v_file_date}/constructors.json")

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructors_dropped_df=constructors_df.drop(col("url"))
display(constructors_dropped_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

#renaming col and add ingestion date
constructors_final_df=add_ingestion_date(constructors_dropped_df).withColumnsRenamed({"constructorId":"constructor_id","constructorRef":"constructor_ref"}).withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))
display(constructors_final_df)

# COMMAND ----------

#creating silver table
constructors_final_df.write.mode("overwrite").format("delta").saveAsTable("f1.silver.constructors")

# COMMAND ----------

constructors_processed_df=spark.read.table("f1.silver.constructors")
display(constructors_processed_df)

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

