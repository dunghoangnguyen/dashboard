# Databricks notebook source
# MAGIC %md
# MAGIC # Checking the following
# MAGIC ### Step 1. One-off load table from vn_processing_datamart_temp_db
# MAGIC ### Step 2. Refresh table weekly or half-weekly
# MAGIC ### Step 3. Store and automate the process

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>1. Load existing data</strong>

# COMMAND ----------

# MAGIC %run "/Shared/Datalab_Utilities/Function"

# COMMAND ----------

orc_path = '/apps/hive/warehouse/vn_processing_datamart_temp_db.db/master_digital_leads_gen_hist/'

# COMMAND ----------

#check_if_orc_acid(orc_path)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Modify</strong>
# MAGIC ### covert_orc_acid_to_parquet()

# COMMAND ----------

def convert_orc_acid_to_parquet(input_path):
    from pyspark.sql.functions import col
    from pyspark.sql.types import StringType, NullType

    wandisco_container = f"abfss://wandisco@{storage}.dfs.core.windows.net/"
    lab_container = f"abfss://lab@{storage}.dfs.core.windows.net/" + f"{country}" +"/project/scratch/"
    ParquetPath = 'ORC_ACID_to_Parquet_Conversion/'

    paths = get_dir_content(wandisco_container+input_path)
    
    File_Path=[]

    for p in paths:
        if 'delete_delta' not in p:
            if p.endswith('_orc_acid_version'):
                File_Path.append(p.replace('_orc_acid_version',''))
    
    path_list=[]
    path_list = list(set(File_Path))
    
    if len(path_list) > 0:
        for path in path_list:
            df = spark.read.format("orc").option("recursiveFileLookup","True").load(path)
            print(path)
            ParquetDestPath = lab_container + ParquetPath + path.replace(wandisco_container,'')
            rdd = df.rdd.map(lambda x: x[-1])
            schema_df = rdd.toDF(sampleRatio=0.5)
            my_schema=list(schema_df.schema)
            null_cols = []
            for st in my_schema:
                if str(st.dataType) in ['NullType', 'NoneType', 'void']:
                    null_cols.append(st)
            for ncol in null_cols:
                mycolname = str(ncol.name)
                schema_df = schema_df \
                    .withColumn(mycolname, lit('NaN')).cast(StringType())
            fileschema = schema_df.schema
            targetDF = spark.createDataFrame(rdd,fileschema)
            # Cast all NullType columns to StringType
            for column_name in targetDF.schema.names:
                if targetDF.schema[column_name].dataType == NullType():
                    #print(f"Casting column {column_name} to StringType")
                    targetDF = targetDF.withColumn(column_name, col(column_name).cast(StringType()))

            # Verify that there are no longer any NullType columns
            #null_type_columns = [column_name for column_name in targetDF.schema.names if targetDF.schema[column_name].dataType == NullType()]
            #assert len(null_type_columns) == 0, f"There are still {len(null_type_columns)} NullType columns: {null_type_columns}"
            #targetDF.printSchema()
            targetDF.write.mode("overwrite").parquet(ParquetDestPath)
            print(ParquetDestPath)

# COMMAND ----------

convert_orc_acid_to_parquet(f"{orc_path}image_date=2023-06-26/")

# COMMAND ----------


