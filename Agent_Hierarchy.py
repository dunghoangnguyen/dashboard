# Databricks notebook source
# MAGIC %md
# MAGIC ### AGENT HIERARCHY

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load libls, params and paths</strong>

# COMMAND ----------

ams_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_AMS_DB/'
cas_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/'

lab_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/dashboard/'

tams_agents_source = 'TAMS_AGENTS/'
tams_candidates_source = 'TAMS_CANDIDATES/'
tams_locations_source = 'TAMS_LOCATIONS/'
tams_ranks_source = 'TAMS_RANKS/'
tams_dsr_rpt_rels_source = 'TAMS_DSR_RPT_RELS/'
tams_dsr_mgr_mappings_source = 'TAMS_DSR_MGR_MAPPINGS/'
tams_stru_groups_source = 'TAMS_STRU_GROUPS/'
tmis_stru_grp_sm_source = 'TMIS_STRU_GRP_SM/'
tfield_values_source = 'TFIELD_VALUES/'

output_dest = 'AGENT_HIERARCHY/'

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load working tables</strong>

# COMMAND ----------

tams_agentsDF = spark.read.parquet(f'{ams_path}{tams_agents_source}')
tams_candidatesDF = spark.read.parquet(f'{ams_path}{tams_candidates_source}')
tams_locationsDF = spark.read.parquet(f'{ams_path}{tams_locations_source}')
tams_ranksDF = spark.read.parquet(f'{ams_path}{tams_ranks_source}')
tams_dsr_rpt_relsDF = spark.read.parquet(f'{ams_path}{tams_dsr_rpt_rels_source}')
tams_dsr_mgr_mappingsDF = spark.read.parquet(f'{ams_path}{tams_dsr_mgr_mappings_source}')
tams_stru_groupsDF = spark.read.parquet(f'{ams_path}{tams_stru_groups_source}')
tmis_stru_grp_smDF = spark.read.parquet(f'{cas_path}{tmis_stru_grp_sm_source}')
tfield_valuesDF = spark.read.parquet(f'{cas_path}{tfield_values_source}')


# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Generate result</strong>

# COMMAND ----------

tams_agentsDF.createOrReplaceTempView('tams_agents')
tams_candidatesDF.createOrReplaceTempView('tams_candidates')
tams_locationsDF.createOrReplaceTempView('tams_locations')
tams_ranksDF.createOrReplaceTempView('tams_ranks')
tams_dsr_rpt_relsDF.createOrReplaceTempView('tams_dsr_rpt_rels')
tams_dsr_mgr_mappingsDF.createOrReplaceTempView('tams_dsr_mgr_mappings')
tams_stru_groupsDF.createOrReplaceTempView('tams_stru_groups')
tmis_stru_grp_smDF.createOrReplaceTempView('tmis_stru_grp_sm')
tfield_valuesDF.createOrReplaceTempView('tfield_values')

resultDF = spark.sql("""
select
	sm_rh.rh_cd as rh_code
	,sm_rh.rh_nm as rh_name
	,sm_rh.sm_cd as sm_code2
	,sm_rh.sm_nm as sm_incharge_loc
	,ams_sm.mobl_phon_num as sm_code2_phone
	,can_sm.email_addr sm_code2_email
	,drt_mgr.agt_code as direct_mgr
	,drt_mgr.agt_nm as direct_mgr_nm
	,drt_mgr.email_addr as direct_mgr_email
	,drt_mgr.mobl_phon_num as direct_mgr_phone
	,sm_rh.mis_loc_cd as location_code
	,aloc.loc_desc as location_name
	,ams.agt_code as agt_code
	,ams.agt_nm as agt_nm
	,can.sex_code agt_gender
	,ams.rank_cd as rank_cd
	,ams.agt_sup agt_sup
	,ams.mobl_phon_num as mobl_phon_num
	,can.email_addr as email_addr
	,ams.epos_ind as epos_ind
	,ams.dtk_ind as dtk_ind
	,ams.cmp_ind as cmp_ind	
	,agt_group.fld_valu_desc as fta_flag
	,ams.cntrct_eff_dt as cntrct_eff_dt
	,ams.comp_prvd_num as comp_prvd_num
from
	vn_published_ams_db.tams_agents ams 
	inner join (
		select
			mis_loc_cd
			,collect_set(sm_cd)[0] as sm_cd
			,collect_set(rh_cd)[0] as rh_cd
			,collect_set(sm_nm)[0] as sm_nm
			,collect_set(rh_nm)[0] as rh_nm
		from
			(
				select
					case
						when a.rpt_level = min(a.rpt_level) over (partition by gsm.loc_code) then dsr.mgr_cd
						else null
					end as sm_cd
					,case
						when a.rpt_level = min (a.rpt_level) over (partition by gsm.loc_code) then mgr.agt_nm
						else null
					end as sm_nm
					,case
						when a.rpt_level = (max (a.rpt_level) over (partition by gsm.loc_code)-2) then dsr.mgr_cd
						else null
					end as rh_cd
					,case
						when a.rpt_level = (max (a.rpt_level) over (partition by gsm.loc_code)-2) then mgr.agt_nm
						else null
					end as rh_nm
					,gsm.loc_code mis_loc_cd
				from
					tams_dsr_rpt_rels a
					left join tams_agents mgr on (a.agt_cd=mgr.agt_code)
					left join tams_agents smgr on (a.sub_agt_cd=smgr.agt_code)
					left join tmis_stru_grp_sm gsm on (a.sub_agt_cd=gsm.loc_mrg)
					left join tams_dsr_mgr_mappings dsr on (a.agt_cd=dsr.dsr_cd)
				Where
					a.agt_cd is not null
					and mgr.agt_nm <> 'Open Location'
			)tbl1
		group by mis_loc_cd
	) sm_rh on ams.loc_code = sm_rh.mis_loc_cd
	inner join tams_agents ams_sm on  (sm_rh.sm_cd = ams_sm.agt_code)
	inner join tams_candidates can_sm on  (ams_sm.can_num = can_sm.can_num)
	inner join (
		select
			stru.stru_grp_cd
			,drt_agt.agt_code
			,drt_agt.agt_nm
			,drt_agt.mobl_phon_num
			,drt_can.email_addr
		from
			tams_stru_groups stru
			inner join tams_agents drt_agt on (stru.mgr_cd = drt_agt.agt_code)
			inner join tams_candidates drt_can on (drt_agt.can_num = drt_can.can_num)
	) drt_mgr on ams.unit_code = drt_mgr.stru_grp_cd
	inner join tams_ranks rank on (rank.rank_cd = ams.rank_cd )
	inner join tams_candidates can on (ams.can_num = can.can_num)
	left join tfield_values agt_group on (can.agent_group = agt_group.fld_valu and fld_nm = 'AGENT_GROUP')
	inner join tams_locations aloc on (ams.loc_code = aloc.loc_cd)
where
	ams.agt_stat_code ='1'
	and ams.comp_prvd_num in ('01','02', '08', '98','12','94','95','96','97')                  
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Store result to parquet</strong>

# COMMAND ----------

resultDF.write.mode('overwrite').parquet(f'{lab_path}{output_dest}')
