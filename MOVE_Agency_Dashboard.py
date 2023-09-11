# Databricks notebook source
# MAGIC %md
# MAGIC # MOVE AGENCY DASHBOARD

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load defined functions</strong>

# COMMAND ----------

# MAGIC %run "/Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions"

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load libls, params and paths</strong>

# COMMAND ----------

from pyspark.sql.functions import count
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta

# List of ABFSS paths
cas_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
ams_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_AMS_DB/'
move5_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_MOVE5_MONGO_DB/'
out_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/dashboard/'

# List of tables
tbl_src1 = 'TAMS_AGENTS/'
tbl_src2 = 'TAMS_DSR_RPT_RELS/'
tbl_src3 = 'TMIS_STRU_GRP_SM/'
tbl_src4 = 'TAMS_DSR_MGR_MAPPINGS/'
tbl_src5 = 'TAMS_CANDIDATES/'
tbl_src6 = 'TAMS_STRU_GROUPS/'
tbl_src7 = 'TAMS_RANKS/'
tbl_src8 = 'TFIELD_VALUES/'
tbl_src9 = 'TAMS_LOCATIONS/'
tbl_src10 = 'TPOLICYS/'
tbl_src11 = 'TCLIENT_POLICY_LINKS/'
tbl_src12 = 'TCLIENT_ADDRESSES/'
tbl_src13 = 'TPROVINCES/'
tbl_src14 = 'MUSER_FLAT/'
tbl_src15 = 'MANULIFEMEMBER_FLAT/'
tbl_src16 = 'MOVEKEY_FLAT/'
tbl_src17 = 'USERSTATE_FLAT/'

#snapshot_paths = [lab_path,casm_path,asm_path]
daily_paths = [ams_path,cas_path,move5_path]
#snapshot_files = [tbl_src1,tbl_src2,tbl_src3,tbl_src4,tbl_src5,tbl_src6,tbl_src7,tbl_src8,
#                 tbl_src9,tbl_src10,tbl_src11,tbl_src12,tbl_src13,tbl_src14,tbl_src15]
daily_files = [tbl_src1,tbl_src2,tbl_src3,tbl_src4,tbl_src5,tbl_src6,tbl_src7,tbl_src8,tbl_src9,
               tbl_src10,tbl_src11,tbl_src12,tbl_src13,tbl_src14,tbl_src15,tbl_src16,tbl_src17]

lmth = -1
move_launch = datetime.strftime(date(2019, 6, 1), '%Y-%m-01')
prm_ptd = datetime.strftime(date(2022, 7, 4), '%Y-%m-01')
_2022_Jun = datetime.strftime(date(2022, 6, 1), '%Y-%m-01')
print('current_mth:', move_launch)
print('rpt_mth:', prm_ptd)
print('_2022_Jun:', _2022_Jun)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load working tables</strong>

# COMMAND ----------

list_df = {}

#snapshot_df = load_parquet_files(snapshot_paths,snapshot_files)
daily_df = load_parquet_files(daily_paths,daily_files)

#list_df.update(snapshot_df)
list_df.update(daily_df)

#print('List of dataframes: ', list(list_df.keys()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate temp views and result

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Generate temp view</strong>

# COMMAND ----------

generate_temp_view(list_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>agt_hrchy</strong>

# COMMAND ----------

agt_hrchy = spark.sql(f"""
with rs_agt_hrchy as (
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
		,ams.mba_ind as mba_ind	
		,agt_group.fld_valu_desc as fta_ind
		,ams.cntrct_eff_dt
		,ams.comp_prvd_num
		,case
			when aloc.chnl in ('AGT','MGA') then 'Agency'
			when aloc.chnl in ('BK','BR') then 'Banca'
			when aloc.chnl = 'DR' then 'DMTM'
			when aloc.chnl = 'MI' then 'MI'
			else 'Unknown'
		end as channel
	from
		tams_agents ams 
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
		and ams.cntrct_eff_dt <= last_day(add_months(current_date,{lmth}))
)
select
	rh_code
	,rh_name
	,sm_code2 sm_code
	,sm_incharge_loc sm_name
	,sm_code2_email sm_email
	,direct_mgr direct_mgr_code
	,direct_mgr_nm direct_mgr_name
	,agt_code
	,agt_nm agt_name
	,location_code
	,location_name
	,mba_ind	
	,fta_ind
	,12*(year(last_day(add_months(current_date,{lmth}))) - year(cntrct_eff_dt)) + (month(last_day(add_months(current_date,{lmth}))) - month(cntrct_eff_dt)) srv_mth
	,channel
	,comp_prvd_num
	,last_day(add_months(current_date,{lmth})) reporting_date
from
	rs_agt_hrchy a
where
	channel in ('Agency','DMTM')
""")

#df = agt_hrchy.groupBy('rh_name')\
#    .agg(
#        count('agt_code').alias('no_agents')
#	)
    
#df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>ifp_customers</strong>

# COMMAND ----------

agt_hrchy.createOrReplaceTempView('agt_hrchy')

ifp_customers = spark.sql(f"""
with rs_cust as (
	select
		cpl.cli_num
		,pro.prov_nm customer_city
		,pol.pol_num
  		,round((pol.mode_prem*12/cast(pol.pmt_mode as int)),2) ape
		,pol.pol_iss_dt
		,pol.pol_eff_dt
		,pol.pol_stat_cd
		,pol.agt_code
		,row_number() over(partition by cpl.cli_num order by pol.pol_eff_dt desc) rw_num
	from
		tpolicys pol
		inner join tclient_policy_links cpl on (pol.pol_num = cpl.pol_num and cpl.link_typ = 'O')
		inner join tclient_addresses ca on (cpl.cli_num = ca.cli_num and cpl.addr_typ = ca.addr_typ)
		inner join tprovinces pro on (substr(ca.zip_code,1,2) = pro.prov_id)
		inner join tams_agents agt on (pol.agt_code = agt.agt_code)
	where
		cpl.rec_status = 'A'
		and pol.pol_stat_cd not in ('A','N','X','R','8')
		and pol.pol_eff_dt <= last_day(add_months(current_date,{lmth}))
)
-- customers who have at least one active policy
,rs_cust_ifp as (
	select distinct
		cli_num
	from
		rs_cust
	where
		pol_stat_cd in ('1','2','3','5')
)
-- calculate volume of each customer have at specific time
,rs_cust_pol_cnt as (
	select
		cli_num
		,count(case when pol_eff_dt <= '{move_launch}' then pol_num end) vol_of_pol_before_move_launched
		,count(case when pol_eff_dt > '{move_launch}' then pol_num end) vol_of_pol_after_move_launched
		,count(case when last_day(pol_eff_dt) = last_day(add_months(current_date,{lmth})) then pol_num end)  vol_of_pol_in_rpt_mth
		,count(case when last_day(pol_eff_dt) >= last_day(add_months(current_date,{lmth} -3)) then pol_num end) vol_of_pol_in_lst_3mths
		,sum(case when last_day(pol_eff_dt) >= last_day(add_months(current_date,{lmth} -3)) then ape end) ape_of_pol_in_lst_3mths
		,count(case when pol_eff_dt >= '{_2022_Jun}' then pol_num end) vol_of_pol_since_2022_jun
	from
		rs_cust
	where
		cli_num in (select cli_num from rs_cust_ifp)
	group by
		cli_num
)
select
	a.cli_num cli_num
	,a.customer_city
	,b.vol_of_pol_before_move_launched
	,b.vol_of_pol_after_move_launched
	,b.vol_of_pol_in_rpt_mth
	,b.vol_of_pol_in_lst_3mths
	,b.ape_of_pol_in_lst_3mths
	,b.vol_of_pol_since_2022_jun
	,a.agt_code
	,last_day(add_months(current_date,{lmth})) reporting_date
from
	rs_cust a
	inner join rs_cust_pol_cnt b on (a.cli_num = b.cli_num)
where
	a.rw_num = 1          
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>move_users</strong>

# COMMAND ----------

move_users = spark.sql(f"""
select distinct
	mu.`_id` muser_id
	,mk.`value` movekey
	,substr(mk.`value`,2,length(mk.`value`) -1) cli_num
	,me.status manulifemember_status
	,mk.activated movekey_activated
	,to_date(mk.activationdate) activation_date
	,to_date(urt.lastdatasync) last_data_sync_date
from
	muser_flat mu
	inner join manulifemember_flat me on (mu.`_id` = me.userid)
	left join movekey_flat mk on (me.keyid = mk.`_id`)
	left join userstate_flat urt on (mu.`_id` = urt.userid)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>move_customers_base</strong>

# COMMAND ----------

ifp_customers.createOrReplaceTempView('ifp_customers')
move_users.createOrReplaceTempView('move_users')

move_customers_base = spark.sql(f"""
select
	cus.cli_num
	,cus.customer_city
	,cus.vol_of_pol_before_move_launched
	,cus.vol_of_pol_after_move_launched
	,cus.vol_of_pol_in_rpt_mth
	,cus.vol_of_pol_in_lst_3mths
	,cus.vol_of_pol_since_2022_jun
	,cus.agt_code
	,case
		when move.movekey_activated = 1 then 'Activated'
		else 'Not-Activated'
	end move_user_info
	,move.activation_date
	,case when vol_of_pol_after_move_launched > 0 then 'NPB' else 'Legacy' end cust_seg
	,last_day(add_months(current_date,{lmth})) reporting_date
from
	ifp_customers cus left join
	move_users move on (cus.cli_num = move.cli_num)
where
	cus.agt_code in (select agt_code from agt_hrchy)                     
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Generate result</strong>

# COMMAND ----------

move_customers_base.createOrReplaceTempView('move_customers_base')

move_embassador_campaign = spark.sql(f"""
with rs_agt_summarize as (
	select
		agt_code
		,count(distinct cli_num) no_of_cust
		,count(distinct(case when move_user_info = 'Activated' and last_day(activation_date) = last_day(add_months(current_date,{lmth})) then cli_num end)) activation_this_month
		,count(distinct(case when move_user_info = 'Activated' and year(activation_date) =  year(last_day(add_months(current_date,{lmth}))) and quarter(activation_date) = quarter(last_day(add_months(current_date,{lmth}))) then cli_num end)) actvation_this_quarter
		,count(distinct(case when move_user_info = 'Activated' and activation_date >= '{prm_ptd}' then cli_num end)) activation_total
		,count(distinct(case when move_user_info = 'Activated' then cli_num end)) activation_move_ptd
		,max(activation_date) lst_activation_date
		--,sum(vol_of_pol_before_move_launched) vol_of_pol_before_move_launched
		,count(case when vol_of_pol_after_move_launched = 0 then cli_num end) vol_of_cust_legacy
		,count(case when vol_of_pol_after_move_launched = 0 and move_user_info = 'Activated' then cli_num end) vol_of_cust_legacy_activated
		--,sum(vol_of_pol_after_move_launched) vol_of_pol_after_move_launched
		,count(case when vol_of_pol_after_move_launched > 0 then cli_num end) vol_of_cust_nbp
		,count(case when vol_of_pol_after_move_launched > 0 and move_user_info = 'Activated' then cli_num end) vol_of_cust_nbp_activated
		,count(case when vol_of_pol_since_2022_jun > 0 then cli_num end) vol_of_cus_nb_since_jun
		,count(case when vol_of_pol_since_2022_jun > 0 and move_user_info = 'Activated' then cli_num end) vol_of_cus_nb_since_jun_activated
		,sum(vol_of_pol_in_rpt_mth) vol_of_pol_in_rpt_mth
		,sum(vol_of_pol_in_lst_3mths) vol_of_pol_in_lst_3mths
		from
			move_customers_base
		group by
			agt_code
)
select
	a.rh_code
	,a.rh_name
	,a.sm_code
	,a.sm_name
	,a.direct_mgr_code
	,a.direct_mgr_name
	,a.agt_code
	,a.agt_name
	,a.location_code
	,a.location_name
	,a.mba_ind	
	,a.fta_ind
	,cast(a.srv_mth as long) as srv_mth
	,a.channel
	,cast(b.no_of_cust AS double) as no_of_cust
	,b.activation_this_month
	,b.actvation_this_quarter
	,b.activation_total
	,b.lst_activation_date
	,a.comp_prvd_num
	,b.vol_of_pol_in_rpt_mth
	,b.vol_of_pol_in_lst_3mths
	,b.vol_of_cust_legacy
	,b.vol_of_cust_legacy_activated
	,b.vol_of_cust_nbp
	,b.vol_of_cust_nbp_activated
	,b.vol_of_cus_nb_since_jun
	,b.vol_of_cus_nb_since_jun_activated
	,b.activation_move_ptd
	,a.sm_email
	,last_day(add_months(current_date,{lmth})) reporting_date
from
	agt_hrchy a left join 
	rs_agt_summarize b on (a.agt_code = b.agt_code)       
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Store result to parquet</strong>

# COMMAND ----------

spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

move_embassador_campaign.write.mode('overwrite').partitionBy('reporting_date').parquet(f'{out_path}MOVE_EMBASSADOR_CAMPAIGN/')
