# Databricks notebook source
# MAGIC %md
# MAGIC # List of Dropped or Expired policy report

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load libls, params and paths</strong>

# COMMAND ----------

cas_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
ams_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_AMS_DB/'

lab_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/dashboard/'

chklst = 'TPT_CHKLST_HISTS/'
chg_ctl = 'TPT_CHG_CTL/'
tfield = 'TFIELD_VALUES/'
tpol = 'TPOLICYS/'
tcov = 'TCOVERAGES/'
batch = 'TPT_BATCH_HEADERS/'
tagt = 'TAMS_AGENTS/'
branch = 'TBRANCHES/'
tterm = 'TTERMINATION_DETAILS/'

output_dest = 'LIST_OF_DROPPED_EXPIRED_POLICY/'

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load working tables and generate views</strong>

# COMMAND ----------

tpt_chklst_hists = spark.read.parquet(f'{cas_path}{chklst}')
tpt_chg_ctl = spark.read.parquet(f'{cas_path}{chg_ctl}')
tfield_values = spark.read.parquet(f'{cas_path}{tfield}')
tpolicys = spark.read.parquet(f'{cas_path}{tpol}')
tcoverages = spark.read.parquet(f'{cas_path}{tcov}')
tpt_batch_headers = spark.read.parquet(f'{cas_path}{batch}')
tams_agents = spark.read.parquet(f'{ams_path}{tagt}')
tbranches = spark.read.parquet(f'{cas_path}{branch}')
ttermination_details = spark.read.parquet(f'{cas_path}{tterm}')

tpt_chklst_hists.createOrReplaceTempView('tpt_chklst_hists')
tpt_chg_ctl.createOrReplaceTempView('tpt_chg_ctl')
tfield_values.createOrReplaceTempView('tfield_values')
tpolicys.createOrReplaceTempView('tpolicys')
tcoverages.createOrReplaceTempView('tcoverages')
tpt_batch_headers.createOrReplaceTempView('tpt_batch_headers')
tams_agents.createOrReplaceTempView('tams_agents')
tbranches.createOrReplaceTempView('tbranches')
ttermination_details.createOrReplaceTempView('ttermination_details')

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Generate result</strong>

# COMMAND ----------

resultDF = spark.sql("""
select distinct
	ctl.comp_date
	,pol.pol_stat_cd
	,case
		when pol.pol_stat_cd = 'A' then ctl.comp_date
		else ter.trmn_actv_dt
	end trmn_actv_dt
	,chk.chg_typ
	,bhd.btch_branch
	,ctl.pos_num
	,fv.fld_valu_desc counter_nm
	,pol.pol_num
	,fld_sts.fld_valu_desc pol_stat
	,fld_chg.fld_valu_desc chg_typ_desc
	,ctl.ho_recv_dt
	,ctl.comp_date as comp_date_1
	,case
		when ctl.btch_seq is null then ctl.asign_user_id
		else bhd.btch_submit_by
	end	btch_submit_by
	,ctl.asign_user_id
	,chk.chklst_result
from
	tpt_chklst_hists chk
	inner join tpt_chg_ctl ctl on (chk.pol_num = ctl.pol_num and chk.pos_num = ctl.pos_num)
	inner join tfield_values fld_chg on (chk.chg_typ = fld_chg.fld_valu and fld_chg.fld_nm = 'POS_CHG_TYP')
	inner join tpolicys pol on (pol.pol_num = ctl.pol_num)
	inner join tcoverages cov on (pol.pol_num = cov.pol_num)
	inner join tfield_values fld_sts on (pol.pol_stat_cd = fld_sts.fld_valu and fld_sts.fld_nm = 'STAT_CODE')
	inner join tpt_batch_headers bhd on (ctl.btch_num=bhd.btch_num)
	inner join tfield_values fv on (bhd.btch_branch=fv.fld_valu and fv.fld_nm = 'POS_SRV_CTR')
	inner join tams_agents agt on (pol.agt_code = agt.agt_code)
	inner join tbranches br on (agt.br_code=br.br_code)	
	left join ttermination_details ter on (pol.pol_num = ter.pol_num)
where
	ctl.chg_stat='00'
	and	chk.chg_typ in ('10','12','37')
	and	chk.chklst_id='1'
	and pol.pol_stat_cd in ('A','E','H')
	and (
			(chk.chg_typ='37' and greatest(ctl.comp_date,nvl(ter.trmn_actv_dt,ctl.comp_date)) >= last_day(add_months(current_date(),-4)))
			or
			(chk.chg_typ<>'37' and ctl.comp_date >= last_day(add_months(current_date(),-4)))
	)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Store result to parquet</strong>

# COMMAND ----------

resultDF.write.mode('overwrite').parquet(f'{lab_path}{output_dest}')

# COMMAND ----------


