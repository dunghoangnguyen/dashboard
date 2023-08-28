# Databricks notebook source
# MAGIC %md
# MAGIC # NOTTAKEN REPORT

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load libls, params and paths</strong>

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

cas_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
ams_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_AMS_DB/'
lab_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/dashboard/'

pol_src = 'TPOLICYS/'
pol_lnk_src = 'TCLIENT_POLICY_LINKS/'
cli_src = 'TCLIENT_DETAILS/'
plan_src = 'TPLANS/'
agt_src = 'TAMS_AGENTS/'
output_dest = 'NOTTAKEN/'

st_dt = date(2020, 1, 1).strftime('%Y-%m-%d')
print(st_dt)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load working tables and temp views</strong>

# COMMAND ----------

tpol = spark.read.parquet(f'{cas_path}{pol_src}')
tpol_lnk = spark.read.parquet(f'{cas_path}{pol_lnk_src}')
tcli = spark.read.parquet(f'{cas_path}{cli_src}')
tpln = spark.read.parquet(f'{cas_path}{plan_src}')
tagt = spark.read.parquet(f'{ams_path}{agt_src}')

tpol.createOrReplaceTempView('tpolicys')
tpol_lnk.createOrReplaceTempView('tclient_policy_links')
tcli.createOrReplaceTempView('tclient_details')
tpln.createOrReplaceTempView('tplans')
tagt.createOrReplaceTempView('tams_agents')

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Generate result</strong>

# COMMAND ----------

resultDF = spark.sql(f"""
select	 p.pol_num as policy_number
        ,p.SBMT_DT as submit_date
        ,p.POL_EFF_DT as policy_effective_date
        ,p.POL_ISS_DT as policy_issued_date
        ,p.POL_STAT_CD as policy_status_code
        ,p.plan_code_base as plan_code_base
        ,p.POL_TRMN_DT as policy_terminated_date
        ,p.CNFRM_ACPT_DT as confirm_accept_date
        ,p.MODE_PREM as mode_premium
        ,p.PMT_MODE as payment_mode
        ,ins.CLI_NM as insured_name
        ,ins.BIRTH_DT as insured_dob
        ,po.CLI_NM as owner_name
        ,po.BIRTH_DT as owner_dob
        ,p.AGT_CODE as servicing_agent_code
        ,agt.AGT_NM as servicing_agent_name
        ,agt.LOC_CODE as location_code
from 	tpolicys p inner join
	    tclient_policy_links lpo on p.pol_num = lpo.pol_num inner join
	    tclient_details po on lpo.cli_num = po.cli_num inner join
       	tclient_policy_links lins on p.pol_num = lins.pol_num inner join
       	tclient_details ins on lins.cli_num = ins.cli_num inner join
       	tplans pln on p.PLAN_CODE_BASE = pln.PLAN_CODE and p.VERS_NUM_BASE = pln.VERS_NUM inner join
	    tams_agents agt on p.agt_code = agt.agt_code
where 	lpo.link_typ ='O' and lpo.REC_STATUS='A'
	and lins.link_typ ='I' and lins.REC_STATUS='A'
	and	to_date(p.pol_trmn_dt) >= '{st_dt}'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Store result to parquet</strong>

# COMMAND ----------

resultDF.write.mode('overwrite').parquet(f'{lab_path}{output_dest}')
