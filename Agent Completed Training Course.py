# Databricks notebook source
# MAGIC %md
# MAGIC # Agent Completed Training Course

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load libraries, params and paths</strong>

# COMMAND ----------

from pyspark.sql.functions import *

tams_agents_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_AMS_DB/TAMS_AGENTS/'
tams_dsr_rpt_rels_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_AMS_DB/TAMS_DSR_RPT_RELS/'
tawsstg_e_attenders_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_AWSSTG_DB/TAWSSTG_E_ATTENDERS/'
tmis_stru_grp_sm_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TMIS_STRU_GRP_SM/'
tams_dsr_mgr_mappings_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_AMS_DB/TAMS_DSR_MGR_MAPPINGS/'

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load and immediate tables</strong>

# COMMAND ----------

tams_agents = spark.read.format("parquet").load(tams_agents_path)
tams_dsr_rpt_rels = spark.read.format("parquet").load(tams_dsr_rpt_rels_path)
tawsstg_e_attenders = spark.read.format("parquet").load(tawsstg_e_attenders_path)
tmis_stru_grp_sm = spark.read.format("parquet").load(tmis_stru_grp_sm_path)
tams_dsr_mgr_mappings = spark.read.format("parquet").load(tams_dsr_mgr_mappings_path)

#Lowercase column headers
tams_agents = tams_agents.toDF(*[col.lower() for col in tams_agents.columns])
tams_dsr_rpt_rels = tams_dsr_rpt_rels.toDF(*[col.lower() for col in tams_dsr_rpt_rels.columns])
tawsstg_e_attenders = tawsstg_e_attenders.toDF(*[col.lower() for col in tawsstg_e_attenders.columns])
tmis_stru_grp_sm = tmis_stru_grp_sm.toDF(*[col.lower() for col in tmis_stru_grp_sm.columns])
tams_dsr_mgr_mappings = tams_dsr_mgr_mappings.toDF(*[col.lower() for col in tams_dsr_mgr_mappings.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Finalize table</strong>

# COMMAND ----------

tams_agents.createOrReplaceTempView("tams_agents")
tams_dsr_rpt_rels.createOrReplaceTempView("tams_dsr_rpt_rels")
tawsstg_e_attenders.createOrReplaceTempView("tawsstg_e_attenders")
tmis_stru_grp_sm.createOrReplaceTempView("tmis_stru_grp_sm")
tams_dsr_mgr_mappings.createOrReplaceTempView("tams_dsr_mgr_mappings")

final = spark.sql("""
SELECT ams.agt_code agt_code,
       ams.agt_nm agt_nm,
       e.end_dt AS date_completion,
       sm_rh.mis_loc_cd as loc_code,
       sm_rh.sm_cd as sm,
       sm_rh.sm_nm sm_nm,
       sm_rh.rh_cd as rh,
       sm_rh.rh_nm rh_nm
FROM tams_agents ams 
INNER JOIN (
SELECT MIS_LOC_CD,COLLECT_SET(SM_CD)[0] AS SM_CD, COLLECT_SET(RH_CD)[0] AS RH_CD
,COLLECT_SET(SM_NM)[0] AS SM_NM, COLLECT_SET(RH_NM)[0] AS RH_NM
FROM (
Select CASE WHEN a.rpt_level = min (a.rpt_level) over (partition by gsm.loc_code) THEN dsr.mgr_cd ELSE NULL END as SM_CD
      ,CASE WHEN a.rpt_level = min (a.rpt_level) over (partition by gsm.loc_code) THEN mgr.agt_nm ELSE NULL END as SM_NM
      ,CASE WHEN a.rpt_level = (max (a.rpt_level) over (partition by gsm.loc_code)-1) THEN dsr.mgr_cd ELSE NULL END as RH_CD
      ,CASE WHEN a.rpt_level = (max (a.rpt_level) over (partition by gsm.loc_code)-1) THEN mgr.agt_nm ELSE NULL END as RH_NM
      ,gsm.loc_code MIS_LOC_CD
  From TAMS_DSR_RPT_RELS a
    left join tams_agents mgr on a.agt_cd=mgr.agt_code
    left join tams_agents smgr on a.sub_agt_cd=smgr.agt_code
    left join tmis_stru_grp_sm gsm on a.sub_agt_cd=gsm.loc_mrg
    LEFT JOIN tams_dsr_mgr_mappings dsr ON a.agt_cd=dsr.dsr_cd
  Where a.agt_cd is not null
      and mgr.agt_nm <> 'Open Location')tbl1
GROUP BY MIS_LOC_CD
) sm_rh on ams.loc_code = sm_rh.MIS_LOC_CD
INNER JOIN tawsstg_e_attenders e ON ams.agt_code = e.agt_code
WHERE e.stat_cd = 'C'
  AND e.class_code = 'MCA0202054'
  AND ams.chnl_cd='01'
""")

print("Final records:", final.count())

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Store result</strong>

# COMMAND ----------

final.write.mode("overwrite").parquet("abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/dashboard/AGENT_COMPLETED_TRAINING_COURSE")

# COMMAND ----------


