# Databricks notebook source
# MAGIC %md
# MAGIC #Welcome Call Monitoring

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load libraries and params</strong>

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load and immidiatetables</strong>

# COMMAND ----------

cntct_lst_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CICS_DB/TCISC_CNTCT_LST/'
service_dtl_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CICS_DB/TCISC_SERVICE_DETAILS/'
obc_vender_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CICS_DB/TCISC_OBC_VENDER_IMPORT/'

cntct_lst = spark.read.format("parquet").load(cntct_lst_path)
service_dtl = spark.read.format("parquet").load(service_dtl_path)
obc_vender = spark.read.format("parquet").load(obc_vender_path)

cntct_lst = cntct_lst.toDF(*[col.lower() for col in cntct_lst.columns])
service_dtl = service_dtl.toDF(*[col.lower() for col in service_dtl.columns])
obc_vender = obc_vender.toDF(*[col.lower() for col in obc_vender.columns])


# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Finalize table</strong>

# COMMAND ----------

cntct_lst.createOrReplaceTempView("cntct_lst")
service_dtl.createOrReplaceTempView("service_dtl")
obc_vender.createOrReplaceTempView("obc_vender")

final = spark.sql("""
select distinct cnt.pol_num as Policy_Number
      ,cnt.csd_num  as Contact_number
      ,cnt.TRACK_TYP as Contact_source
      ,cnt.trxn_dt as File_date_or_Upload_AWS_date
      ,imp.import_dt as Vendor_import_date
      ,imp.result_obc as Vendor_WCC_result
	  ,CASE
	  		when cnt.remark is not null then 'Y'
			else 'N'
	   End as Agent_Feedback_date
      ,ser.cisc_status as Contact_Status
      ,ser.comp_dt as Contact_Close_date
      ,ser.RESULT_CD as Contact_result
      ,ser.cisc_typ as Contact_type
      from cntct_lst cnt 
           left join service_dtl ser on cnt.cntct_dt = ser.cntct_dt and cnt.csd_num =  ser.csd_num
           left join obc_vender imp on cnt.cntct_dt = imp.cntct_dt and  cnt.csd_num = imp.csd_num and cnt.pol_num = imp.pol_num
     where cnt.TRACK_TYP in ('03','40','42' )
     and cnt.trxn_dt >= '2020-01-01'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Store result</strong>

# COMMAND ----------

final.write.mode("overwrite").parquet("abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/dashboard/WELCOME_CALL_MONITORING")
