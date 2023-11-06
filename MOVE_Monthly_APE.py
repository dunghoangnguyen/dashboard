# Databricks notebook source
# MAGIC %md
# MAGIC # MOVE MONTHLY APE

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load defined functions</strong>

# COMMAND ----------

# MAGIC %run /Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load libls, params and paths</strong>

# COMMAND ----------

import pandas as pd
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta

# List of ABFSS paths
lab_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/scratch/ORC_ACID_to_Parquet_Conversion/apps/hive/warehouse/vn_processing_datamart_temp_db.db/'
casm_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/'
asm_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_AMS_SNAPSHOT_DB/'
cas_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
ams_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_AMS_DB/'
move5_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_MOVE5_MONGO_DB/'
out_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/dashboard/'

# List of tables
in_src = 'move_monthly_ape/'
tpolicys_src = 'TPOLICYS/'
tpol_lnk_src = 'TCLIENT_POLICY_LINKS/'
tclient_src = 'TCLIENT_DETAILS/'
tclient_oth_src = 'TCLIENT_OTHER_DETAILS/'
taddr_src = 'TCLIENT_ADDRESSES/'
tprov_src = 'TPROVINCES/'
tplan_src = 'TPLANS/'
tfield_src = 'TFIELD_VALUES/'
tagt_src = 'TAMS_AGENTS/'
tcomp_src = 'TAMS_COMP_CHNL_MAPPINGS/'
muser_src = 'MUSER_FLAT/'
manulifemember_src = 'MANULIFEMEMBER_FLAT/'
movekey_src= 'MOVEKEY_FLAT/'

snapshot_paths = [lab_path,casm_path,asm_path]
daily_paths = [ams_path,cas_path,move5_path]
snapshot_files = [in_src,tpolicys_src,tpol_lnk_src,tclient_src,tclient_oth_src,taddr_src,tprov_src,
                 tplan_src,tfield_src,tagt_src]
daily_files = [tcomp_src,muser_src,manulifemember_src,movekey_src]

# current month (all the behaviors before the 1st of current month would be considered)
current_mth = datetime.strftime(datetime.now(), '%Y-%m-01')
# the snap shot would be taken on the last day of the last month
rpt_mth = str(pd.to_datetime(current_mth) -relativedelta(days=1))[:10]
print(f'current_mth:', current_mth)
print(f'rpt_mth:', rpt_mth)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load working tables</strong>

# COMMAND ----------

list_df = {}

snapshot_df = load_parquet_files(snapshot_paths,snapshot_files)
daily_df = load_parquet_files(daily_paths,daily_files)

list_df.update(snapshot_df)
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
# MAGIC <strong>tifp</strong>

# COMMAND ----------

tifp = spark.sql(f"""
select distinct
		cd.cli_num customer_id
		,tod.email_addr customer_email
		,pro.prov_nm customer_city
		,pol.pol_num policy_number
		,pol.plan_code_base plan_code
		,pl.plan_lng_desc plan_name
		,(pol.mode_prem*12/pol.pmt_mode) policy_ape
		,fld.fld_valu_desc policy_status
		,pol.pol_eff_dt pol_eff_date
		,date_format(pol.pol_eff_dt,'yyyy-MM') eff_mth
		,pol.cnfrm_acpt_dt cnfrm_acpt_dt
		,compro.chnl_cd agt_channel
  		,case when compro.chnl_cd='AGT' then 'Y' else 'N' end agency_ind
  		,case when compro.chnl_cd='BK' then 'Y' else 'N' end banca_ind
 		,case when compro.chnl_cd='DR' then 'Y' else 'N' end dmtm_ind
	from
		tpolicys pol
		inner join tclient_policy_links cpl on (pol.image_date = cpl.image_date and pol.pol_num = cpl.pol_num and cpl.link_typ = 'O')
		inner join tclient_details cd on (cpl.image_date = cd.image_date and cpl.cli_num = cd.cli_num)
		inner join tclient_other_details tod on (cd.image_date = tod.image_date and cd.cli_num = tod.cli_num)
		inner join tclient_addresses ca on (cpl.image_date = ca.image_date and cpl.cli_num = ca.cli_num and cpl.addr_typ = ca.addr_typ)
		inner join tprovinces pro on (ca.image_date = pro.image_date and substr(ca.zip_code,1,2) = pro.prov_id)
		inner join tplans pl on (pol.image_date = pl.image_date and pol.plan_code_base = pl.plan_code and pol.vers_num_base = pl.vers_num)
		inner join tfield_values fld on (pol.image_date = fld.image_date and fld.fld_nm = 'STAT_CODE' and fld.fld_valu = pol.pol_stat_cd)
		inner join tams_agents agt on (pol.image_date = agt.image_date and pol.agt_code = agt.agt_code)
		inner join tams_comp_chnl_mappings compro on (agt.comp_prvd_num = compro.comp_prvd_num)
	where
		pol.image_date = '{rpt_mth}'
		and pol.pol_stat_cd not in ('1','3','5','7')		-- changed 11/10/2022 KPI definition change
		and cpl.rec_status = 'A'
		and ca.zip_code is not null 
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>t_move_eligible_cutomers</strong>

# COMMAND ----------

tifp.createOrReplaceTempView('tifp')

t_move_eligible_cutomers = spark.sql("""
with tlst_city as (
select	customer_id, max(pol_eff_date) lst_eff_dt
from 	tifp
group by customer_id
),
tlst_pol as (
select	distinct customer_id, policy_number, pol_eff_date
from	tifp
order by customer_id, pol_eff_date desc, policy_number desc
),
tlst as (
select	tlst_city.customer_id, max(tlst_pol.policy_number) lst_pol_num, tlst_city.lst_eff_dt
from	tlst_city inner join
  		tlst_pol on tlst_city.customer_id = tlst_pol.customer_id and tlst_city.lst_eff_dt = tlst_pol.pol_eff_date
group by
  		tlst_city.customer_id, tlst_city.lst_eff_dt
)
select
	tifp.customer_id
	,concat_ws(',',
			   collect_set(case agt_channel
								when 'AGT' then 'Agency'
								when 'DR' then 'DMTM'
								when 'BK' then 'Banca'
							end)
	) agt_channel
	,customer_city
	,max(agency_ind) agency_ind
	,max(banca_ind) banca_ind
	,max(dmtm_ind) dmtm_ind
from
	tifp inner join
	tlst on tifp.customer_id = tlst.customer_id and tifp.policy_number = tlst.lst_pol_num and tifp.pol_eff_date = tlst.lst_eff_dt
where
	agt_channel in ('AGT','DR','BK')
	--and customer_city in ('HN','Hà Nội','Hải Phòng','Đà Nẵng','HCM','Hồ Chí Minh','Cần Thơ')	-- changed 11/10/2022 KPI definition change
group by
	tifp.customer_id,
	customer_city                           
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>tmove_users & rs_b</strong>

# COMMAND ----------

tmove_users = spark.sql("""
select
	mu.`_id` muser_id
	,mk.`value` move_key
	,substr(mk.`value`,2,length(mk.`value`)-1) cli_num
	,me.status manulifemember_status
	,mk.activated movekey_activated
	,to_date(substr(mk.activationdate,1,10)) activation_date
from
	muser_flat mu
	inner join manulifemember_flat me on (mu.`_id` = me.userid)
	left join movekey_flat mk on (me.keyid = mk.`_id`)           
""")

tmove_users.createOrReplaceTempView('move_users')

rs_b = spark.sql(f"""
	select
		customer_id
		,count(case when pol_eff_date <= '2019-06-01' then policy_number end) before_move_launch
		,sum(case when pol_eff_date <= '2019-06-01' then policy_ape end) ape_before_move_launch
		,count(case when pol_eff_date > '2019-06-01' then policy_number end) after_move_launch
		,sum(case when pol_eff_date > '2019-06-01' then policy_ape end) ape_after_move_launch
  		,count(case when pol_eff_date > activation_date then policy_number end) after_activation	-- added 11/10/2022 KPI definition change
  		,sum(case when pol_eff_date > activation_date then policy_ape end) ape_after_activation		-- added 11/10/2022 KPI definition change
		,count(case when last_day(pol_eff_date) = '{rpt_mth}' then policy_number end) new_policies
		,sum(case when last_day(pol_eff_date) = '{rpt_mth}' then policy_ape end) ape_new_policies
	from
		tifp left join
  		move_users mov on tifp.customer_id=mov.cli_num
	group by
		customer_id         
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Generate result</strong>

# COMMAND ----------

t_move_eligible_cutomers.createOrReplaceTempView('t_move_eligible_cutomers')
rs_b.createOrReplaceTempView('rs_b')

move_monthly_ape = spark.sql(f"""
select
	a.customer_id customer_id
	,mv.move_key
	,b.before_move_launch before_move_launch
	,b.ape_before_move_launch ape_before_move_launch
	,b.after_move_launch after_move_launch
	,b.ape_after_move_launch ape_after_move_launch
	,b.new_policies new_policies
	,b.ape_new_policies ape_new_policies
	,a.agt_channel channel
	,case a.customer_city
		when 'HN' then 'Hà Nội'
		when 'HCM' then 'Hồ Chí Minh'
		when 'Hà Nội' then 'Hà Nội'
		when 'Hồ Chí Minh' then 'Hồ Chí Minh'
		when 'Đà Nẵng' then 'Đà Nẵng'
		when 'Hải Phòng' then 'Hải Phòng'
		when 'Cần Thơ' then 'Cần Thơ'
		else 'Others'
	end customer_city
	,mv.manulifemember_status
	,mv.movekey_activated
	,mv.activation_date
	,case
		when after_move_launch = 0 then 'Legacy'
		else						 -- 'Non-Legacy'
			case
				when before_move_launch = 0 then 'New policy buyer'
				else "New converted from Legacy"
			end
	end cus_seg
	,b.after_activation after_activation									-- added 11/10/2022 KPI definition change
	,b.ape_after_activation ape_after_activation							-- added 11/10/2022 KPI definition change
	,case
		when new_policies > 0 and after_activation > 0 then 'Y' else 'N'
	end subsequent_ind							  							-- added 11/10/2022 KPI definition change
	,a.agency_ind agency_ind												-- added 11/10/2022 KPI definition change
	,a.banca_ind banca_ind													-- added 11/10/2022 KPI definition change
	,a.dmtm_ind dmtm_ind													-- added 11/10/2022 KPI definition change
	,'{rpt_mth}' reporting_date
from
	t_move_eligible_cutomers a
	inner join rs_b b on (a.customer_id = b.customer_id)
	left join move_users mv on (a.customer_id = mv.cli_num)          
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Store result to parquet</strong>

# COMMAND ----------

spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

move_monthly_ape.write.mode('overwrite').partitionBy('reporting_date').parquet(f'{out_path}MOVE_MONTHLY_APE/')
move_monthly_ape.coalesce(1).write.mode('overwrite').option('header', 'true').csv(f'{out_path}MOVE_MONTHLY_APE/CSV/')
