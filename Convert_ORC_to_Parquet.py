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

# MAGIC %run "/Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions"

# COMMAND ----------

orc_path = '/apps/hive/warehouse/vn_processing_datamart_temp_db.db/move_customers_base/'

# COMMAND ----------

convert_orc_acid_to_parquet(f"{orc_path}")
