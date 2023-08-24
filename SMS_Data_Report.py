# Databricks notebook source
# MAGIC %md
# MAGIC #SMS DATA REPORT

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load libls, params and paths</strong>

# COMMAND ----------


cics_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CICS_DB/'
cas_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
lab_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/dashboard/'

trxn_source = 'TSMS_TRXN_HISTORIES/'
template_source = 'TSMS_TEMPLATES/'
tfield_source = 'TFIELD_VALUES/'
output_dest = 'SMS_DATA_REPORT/'

# COMMAND ----------

# MAGIC %md
# MAGIC <Strong>Load working tables</strong>

# COMMAND ----------

tsms_trxnDF = spark.read.parquet(f'{cics_path}{trxn_source}')
tfieldDF = spark.read.parquet(f'{cas_path}{tfield_source}')
tsms_templatesDF = spark.read.parquet(f'{cics_path}{template_source}')

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Generate result</strong>

# COMMAND ----------

tsms_trxnDF.createOrReplaceTempView('tsms_trxn_histories')
tfieldDF.createOrReplaceTempView('tfield_values')
tsms_templatesDF.createOrReplaceTempView('tsms_templates')

resultDF = spark.sql("""
select 
	t_sms.trxn_dt as send_date
	,t_sms.cli_typ as type
	,t_sms.tmp_id as template_id
	,tml.tmp_nm as template_name
	,t_sms.status_vendor as result_status
	,fld_sts.fld_valu_desc as  result_status_desc
	,t_sms.reason_vendor as reason_vendor_code
	,t_sms.msg_txt as sms_content
from
	tsms_trxn_histories t_sms inner join 
 	tfield_values fld_sts on (t_sms.status_vendor = fld_sts.fld_valu and fld_sts.fld_nm = 'SMS_STATUS_VENDOR') inner join
	tsms_templates tml on (t_sms.tmp_id = tml.tmp_id)
where
	t_sms.tmp_id like 'BCS-CS-%'
	and (t_sms.cli_typ = 'C' or t_sms.cli_typ is null)
	and t_sms.trxn_dt >= date_add(current_date, -1095)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Store result to parquet</strong>

# COMMAND ----------

resultDF.write.mode('overwrite').parquet(f'{lab_path}{output_dest}')
