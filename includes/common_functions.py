# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

def add_ingestion_date(input_df):
    output_df=input_df.withColumn("ingestion_date",current_timestamp())
    return output_df

# COMMAND ----------

def arrange_partition_column(input_df,partition_column):
    column_list=[]
    for column_name in input_df.columns:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)
    output_df=input_df.select(column_list)
    return output_df;

# COMMAND ----------

def overwrite_partition(input_df,cat_name,db_name,table_name,partition_column):
    output_df=arrange_partition_column(input_df,partition_column)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if spark.catalog.tableExists(f"{cat_name}.{db_name}.{table_name}"):
        output_df.write.mode("overwrite").insertInto(f"{cat_name}.{db_name}.{table_name}")
    else:
        output_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{cat_name}.{db_name}.{table_name}")

# COMMAND ----------

def df_column_to_list(input_df,column_name):
    race_results_list=input_df.select(column_name).distinct().collect()
    column_value_list=[row[column_name] for row in race_results_list]
    return column_value_list

# COMMAND ----------

# DBTITLE 1,Cell 6
def merge_delta_data(input_df,cat_name,db_name,table_name,merge_column,partition_column):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")

    from delta.tables import DeltaTable
    if(spark.catalog.tableExists(f"{cat_name}.{db_name}.{table_name}")):
        deltatable=DeltaTable.forName(spark,f"{cat_name}.{db_name}.{table_name}")
        deltatable.alias("tgt").merge(input_df.alias("src"),
        merge_condition) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    else:
        input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{cat_name}.{db_name}.{table_name}")

# COMMAND ----------

