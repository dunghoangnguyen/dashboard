# Databricks notebook source
# MAGIC %md
# MAGIC # Create and store metrics for CX Dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load libl</strong>

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import date, datetime, timedelta

spark = SparkSession.builder.appName("CX").getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load params and tables</strong>

# COMMAND ----------

x = 7 # Change to number of months ago (0: last month-end, 1: last last month-end, ...)
today = datetime.now()
first_day_of_current_month = today.replace(day=1)
current_month = first_day_of_current_month

for i in range(x):
    first_day_of_previous_month = current_month - timedelta(days=1)
    first_day_of_previous_month = first_day_of_previous_month.replace(day=1)
    current_month = first_day_of_previous_month

last_day_of_x_months_ago = current_month - timedelta(days=1)
rpt_dt = last_day_of_x_months_ago.strftime('%Y-%m-%d')
rpt_num = rpt_dt[2:4]+rpt_dt[5:7]
print(f"{rpt_dt}, {rpt_num}")

source_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/scratch/ORC_ACID_to_Parquet_Conversion/apps/hive/warehouse/vn_processing_datamart_temp_db.db/customers_table_mthly/'
dest_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/dashboard/CX/'

cas_snapshot_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/'
ams_snapshot_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_AMS_SNAPSHOT_DB/'
prod_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
move_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_MOVE5_MONGO_DB/'
datamart_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_DATAMART_DB/'
campaign_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_CAMPAIGN_DB/'

tpol_mthend_path = 'TPOLICYS/'
tcli_pol_mthend_path = 'TCLIENT_POLICY_LINKS/'
tcus_mthend_path = 'TCUSTDM_MTHEND/'
tcov_mthend_path = 'TCOVERAGES/'
tex_rate_path = 'TEXCHANGE_RATES'
tagt_mthend_path = 'TAMS_AGENTS/'
manumember_path = 'MANULIFEMEMBER_FLAT/'
movekey_path = 'MOVEKEY_FLAT/'
muser_path = 'MUSER_FLAT/'
tcol_profiles_path = 'TCOL_PROFILES/'
plan_code_path = 'VN_PLAN_CODE_MAP/'
tfields_path = 'TFIELD_VALUES/'

customer_table = spark.read.parquet(f"{source_path}")
tpol_mthend = spark.read.parquet(f"{cas_snapshot_path}{tpol_mthend_path}")
tcli_pol_mthend = spark.read.parquet(f"{cas_snapshot_path}{tcli_pol_mthend_path}")
tcus_mthend = spark.read.parquet(f"{datamart_path}{tcus_mthend_path}")
tcov_mthend = spark.read.parquet(f"{cas_snapshot_path}{tcov_mthend_path}")
tex_rate = spark.read.parquet(f"{prod_path}{tex_rate_path}")
tagt_mthend = spark.read.parquet(f"{ams_snapshot_path}{tagt_mthend_path}")
manumember = spark.read.parquet(f"{move_path}{manumember_path}")
movekey = spark.read.parquet(f"{move_path}{movekey_path}")
muser = spark.read.parquet(f"{move_path}{muser_path}")
tcol_profiles = spark.read.parquet(f"{prod_path}{tcol_profiles_path}")
plan_code = spark.read.parquet(f"{campaign_path}{plan_code_path}")
tfields = spark.read.parquet(f"{cas_snapshot_path}{tfields_path}")

# Filter to select records for reporting month only
customer_table = customer_table.filter(col('reporting_date') == rpt_dt)
tpol_mthend = tpol_mthend.filter(col('image_date') == rpt_dt)
tcli_pol_mthend = tcli_pol_mthend.filter(col('image_date') == rpt_dt)
tcus_mthend = tcus_mthend.filter(col('image_date') == rpt_dt)
tcov_mthend = tcov_mthend.filter(col('image_date') == rpt_dt)
tagt_mthend = tagt_mthend.filter(col('image_date') == rpt_dt)
tfields = tfields.filter(col('image_date') == rpt_dt)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate temp tables

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>t_cust_dtl</strong>

# COMMAND ----------

customer_table.createOrReplaceTempView('customers_table_mthly')
tpol_mthend.createOrReplaceTempView('tpolicys')
tcli_pol_mthend.createOrReplaceTempView('tclient_policy_links')
tcov_mthend.createOrReplaceTempView('tcoverages')
tex_rate.createOrReplaceTempView('texchange_rates')

t_cust_dtl = spark.sql(f"""
select
	a.po_num
	,a.channel	
	,a.reporting_date
	,b.pol_cnt
	,b.base_ape
	,b.rid_cnt
	,b.rid_ape
	,case
		when nt.cli_num is null then 'N'
		else
			case
				when year(nt.first_eff_pol) = year(a.reporting_date) then 'Y'
				else 'N'
			end
	end new_cust_ytd_ind
from
	customers_table_mthly a
	inner join (
		select
			pol.image_date
			,cpl.cli_num
			,count(distinct pol.pol_num) as pol_cnt			
			,sum(case when cvg.cvg_typ = 'B' then cvg.cvg_prem*12/pol.pmt_mode end)/xrt.exr_usd base_ape
			,count(case when cvg.cvg_typ <> 'B' then cvg.pol_num end) rid_cnt
			,sum(case when cvg.cvg_typ <> 'B' then cvg.cvg_prem*12/pol.pmt_mode end)/xrt.exr_usd rid_ape
		from
			tpolicys pol			
			inner join tclient_policy_links cpl on (pol.image_date = cpl.image_date and pol.pol_num = cpl.pol_num and cpl.link_typ = 'O')
			inner join tcoverages cvg on (pol.image_date = cvg.image_date and pol.pol_num = cvg.pol_num and cvg.cvg_reasn = 'O')			
			left join (
				select
					aa.xchng_rate AS exr_usd
					,aa.fr_crcy_code
					,aa.to_crcy_code			
				FROM
					(
						SELECT
							xchng_rate
							, to_eff_dt
							,fr_crcy_code
							, to_crcy_code					
						FROM
							texchange_rates
						WHERE
							fr_crcy_code = '78'
							AND to_crcy_code = '78'
						AND xchng_rate_typ = 'U'				
						ORDER BY to_eff_dt DESC
					) aa
				LIMIT 1
			) xrt ON xrt.fr_crcy_code = pol.crcy_code AND xrt.to_crcy_code = pol.crcy_code
		where
			pol.pol_stat_cd in ('1','2','3','5')
			and cvg.cvg_stat_cd in ('1','2','3','5')
		group by
			pol.image_date
			,cpl.cli_num
			,xrt.exr_usd
	) b on (a.reporting_date = b.image_date and a.po_num = b.cli_num)
	left join (
		select
			pol.image_date
			,cpl.cli_num
			,min(pol.pol_eff_dt) first_eff_pol
		from
			tpolicys pol
			inner join tclient_policy_links cpl on (pol.image_date = cpl.image_date and pol.pol_num = cpl.pol_num and cpl.link_typ = 'O')
		group by
			pol.image_date
			,cpl.cli_num
	) nt on (a.reporting_date = nt.image_date and a.po_num = nt.cli_num)
where
	a.reporting_date = '{rpt_dt}'                       
""")
#print("#'s t_cust_dtl records:", t_cust_dtl.count())

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>t_cust_dtl_bnk</strong>

# COMMAND ----------

tagt_mthend.createOrReplaceTempView('tams_agents')

t_cust_dtl_bnk = spark.sql(f"""
select
	a.po_num
	,bnk.bank_cd	
	,a.reporting_date
	,b.pol_cnt
	,b.base_ape
	,b.rid_cnt
	,b.rid_ape
from
	customers_table_mthly a
	inner join (
		select
			pol.image_date
			,cpl.cli_num
			,count(distinct pol.pol_num) as pol_cnt			
			,sum(case when cvg.cvg_typ = 'B' then cvg.cvg_prem*12/pol.pmt_mode end)/xrt.exr_usd base_ape
			,count(case when cvg.cvg_typ <> 'B' then cvg.pol_num end) rid_cnt
			,sum(case when cvg.cvg_typ <> 'B' then cvg.cvg_prem*12/pol.pmt_mode end)/xrt.exr_usd rid_ape
		from
			tpolicys pol			
			inner join tclient_policy_links cpl on (pol.image_date = cpl.image_date and pol.pol_num = cpl.pol_num and cpl.link_typ = 'O')
			inner join tcoverages cvg on (pol.image_date = cvg.image_date and pol.pol_num = cvg.pol_num and cvg.cvg_reasn = 'O')			
			left join (
				select
					aa.xchng_rate AS exr_usd
					,aa.fr_crcy_code
					,aa.to_crcy_code			
				FROM
					(
						SELECT
							xchng_rate
							, to_eff_dt
							,fr_crcy_code
							, to_crcy_code					
						FROM
							texchange_rates
						WHERE
							fr_crcy_code = '78'
							AND to_crcy_code = '78'
						AND xchng_rate_typ = 'U'				
						ORDER BY to_eff_dt DESC
					) aa
				LIMIT 1
			) xrt ON xrt.fr_crcy_code = pol.crcy_code AND xrt.to_crcy_code = pol.crcy_code
		where
			pol.pol_stat_cd in ('1','2','3','5')
			and cvg.cvg_stat_cd in ('1','2','3','5')
		group by
			pol.image_date
			,cpl.cli_num
			,xrt.exr_usd
	) b on (a.reporting_date = b.image_date and a.po_num = b.cli_num)
	inner join (		
		select distinct
			cpl.cli_num
			,substr(agt.loc_code,1,3) as bank_cd				  
		from
			tpolicys pol
			inner join tclient_policy_links cpl on (pol.pol_num = cpl.pol_num and cpl.link_typ = 'O')
			inner join tams_agents agt on (pol.agt_code = agt.agt_code)				  	  
		where
			agt.chnl_cd = '03'
			and agt.stat_cd = '01'
			and pol.pol_stat_cd in ('1','2','3','5')
			and cpl.rec_status = 'A'	
	) bnk on (a.po_num = bnk.cli_num)
where
	a.reporting_date = '{rpt_dt}'     
""")
#print("#'s t_cust_dtl_bnk records:", t_cust_dtl_bnk.count())

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>unassigned customers<br></strong>
# MAGIC t_ucm_grp

# COMMAND ----------

tcol_profiles.createOrReplaceTempView('tcol_profiles')

t_ucm_grp = spark.sql(f"""
select
	cus.reporting_date
	,cus.po_num
	,cus.channel
	,cas.def
from
	customers_table_mthly cus
	left join (
		select
			*
		from
			(
				select
					pol.pol_num			
					,cpl.cli_num
					,pol.wa_cd_1
					,pol.agt_code
					,agt_srv.stat_cd
					,case
						when agt_srv.comp_prvd_num = '04' then			
							case								
								when substr(col_grp,1,3) = 'PSA' then 'PSA'
								else 'Collector'
							end
						when agt_srv.comp_prvd_num = '98' then 'SM'
						else
							case
								when agt_srv.stat_cd = '99' then 'Unassigned'
								when agt_srv.stat_cd = '01' and pol.wa_cd_1 = pol.agt_code then 'Original'
								when agt_srv.stat_cd = '01' and pol.wa_cd_1 <> pol.agt_code then 'Reassigned'						
								else 'Unknown'
							end
					end def
					,row_number() over(partition by pol.image_date,cpl.cli_num order by pol.pol_iss_dt desc) as rw_num
					,pol.image_date
				from
					tpolicys pol
					inner join tclient_policy_links cpl on (pol.image_date = cpl.image_date and pol.pol_num = cpl.pol_num and cpl.link_typ = 'O')
					inner join tams_agents agt_srv on (pol.image_date = agt_srv.image_date and pol.agt_code = agt_srv.agt_code)
					left join tcol_profiles col on (agt_srv.agt_code = col.col_code)
				where
					pol.image_date = '{rpt_dt}'
					and pol.pol_stat_cd in ('1','2','3','5')
			) rs
		where
			rs.rw_num = 1
	) cas on (cus.reporting_date = cas.image_date and cus.po_num = cas.cli_num)
where
	reporting_date = '{rpt_dt}'
""")

#print("#'s of t_ucm_grp records:", t_ucm_grp.count())

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>MOVE<br></strong>
# MAGIC t_move

# COMMAND ----------

manumember.createOrReplaceTempView('manulifemember_flat')
movekey.createOrReplaceTempView('movekey_flat')
muser.createOrReplaceTempView('muser_flat')

t_move = spark.sql(f"""
select
	'move_registration' kpi
	,'' sub_kpi
	,cus.channel
	,count(distinct cli_num) kpi_val	
	,last_day(cr_dt) reporting_date
from
	(
		select
			substr(mk.value,2,length(mk.value)-1) cli_num
			,mnl.userid
			,case
				when instr(usr.created,'-') > 0 then to_date(substr(usr.created,1,10))
				else
					case
						when cast(usr.created as bigint) is null then null
						else to_date(from_unixtime(cast(cast(usr.created as bigint)/1000 as bigint)))
					end	
			end cr_dt
		from
			muser_flat usr
			inner join manulifemember_flat mnl on (usr.`_id` = mnl.userid)
			inner join movekey_flat mk on (mnl.keyid = mk.`_id`)
	) mve
	inner join customers_table_mthly cus on (mve.cli_num = cus.po_num)
where	
	last_day(mve.cr_dt) = '{rpt_dt}'
	and last_day(mve.cr_dt) = cus.reporting_date
group by
	channel
	,last_day(cr_dt)     
""")

#print("#'s t_move records:", t_move.count())

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>insert<br></strong>
# MAGIC rs1

# COMMAND ----------

rs1 = spark.sql(f"""
select
	'unique_customers' kpi
	,case when cust_new_ytd = 'Y' then 'new_customer' else 'existing_customer' end as kpi_sub
	,channel chnl
	,count(po_num) kpi_val
	,reporting_date
from
	customers_table_mthly
where
	reporting_date = '{rpt_dt}'
group by
	case when cust_new_ytd = 'Y' then 'new_customer' else 'existing_customer' end
	,channel
	,reporting_date
union
select
	'customers_more_than_1_product' kpi
	,null as kpi_sub
	,rst.channel chnl
	,(no_cust_2_IFP + no_cust_3_IFP + no_cust_4_IFP + no_cust_5more_IFP) val
	,reporting_date
from
	(		
		select
			channel
			,count(case when prod_cnt=2 then po_num end) as no_cust_2_IFP
			,count(case when prod_cnt=3 then po_num end) as no_cust_3_IFP
			,count(case when prod_cnt=4 then po_num end) as no_cust_4_IFP
			,count(case when prod_cnt>4 then po_num end) as no_cust_5more_IFP
			,reporting_date
		from
			customers_table_mthly
		where
			reporting_date = '{rpt_dt}'
		group by
			channel
			,reporting_date
			
	) rst
union
select
	'existing_customers' kpi
	,null as kpi_sub
	,channel chnl
	,count(case when customer_type='Existing Customer' then po_num end) kpi_val
	,reporting_date
from
	customers_table_mthly
where
	reporting_date = '{rpt_dt}'
group by
	channel
	,reporting_date
union
select
	'new_customers' kpi
	,null as kpi_sub
	,channel chnl
	,count(case when customer_type='New Customer' then po_num end) kpi_val
	,reporting_date
from
	customers_table_mthly
where
	reporting_date = '{rpt_dt}'
group by
	channel
	,reporting_date
union
select
	'customer_segmentation' as kpi
	,vip_seg as kpi_sub
	,channel chnl
	,count(po_num) kpi_val
	,reporting_date
from
	customers_table_mthly
where
	reporting_date = '{rpt_dt}'
group by
	vip_seg
	,channel
	,reporting_date          
                
""")

#rs1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC rs2

# COMMAND ----------

tcus_mthend.createOrReplaceTempView('tcustdm_mthend')

rs2 = spark.sql(f"""
select
	'contact_availability_mobl' as kpi
	,case when sms_stat_cd = '01' then 'Y' else 'N' end kpi_sub
	,a.channel chnl
	,count(po_num) kpi_val
	,reporting_date
from
	customers_table_mthly a
	inner join tcustdm_mthend b on (a.po_num = b.cli_num and a.reporting_date = b.image_date)
where
	reporting_date = '{rpt_dt}'
	and mobl_phon_num is not null	
group by
	case when sms_stat_cd = '01' then 'Y' else 'N' end
	,a.channel
	,reporting_date
union
select
	'contact_availability_email' as kpi
	,case when service_method_recv = '2' then 'Y' else 'N' end kpi_sub
	,a.channel chnl
	,count(po_num) kpi_val
	,reporting_date
from
	customers_table_mthly a
	inner join tcustdm_mthend b on (a.po_num = b.cli_num and a.reporting_date = b.image_date)
where
	reporting_date = '{rpt_dt}'
	and email_addr is not null
group by
	case when service_method_recv = '2' then 'Y' else 'N' end
	,a.channel
	,reporting_date
union
select
	'cities' kpi
	,city as kpi_sub
	,channel chnl
	,count(po_num) kpi_val
	,reporting_date
from
	customers_table_mthly
where
	reporting_date = '{rpt_dt}'
group by
	city
	,channel
	,reporting_date
union
select
	'customers_issue_age' kpi
	,age_iss_group kpi_sub
	,channel chnl
	,count(po_num) kpi_val
	,reporting_date
from
	customers_table_mthly
where
	reporting_date = '{rpt_dt}'
group by
	age_iss_group
	,channel
	,reporting_date
union
select
	'customers_curr_age' kpi
	,age_curr_group kpi_sub
	,channel chnl
	,count(po_num) kpi_val
	,reporting_date
from
	customers_table_mthly
where
	reporting_date = '{rpt_dt}'
group by
	age_curr_group
	,channel
	,reporting_date
union
select
	'customers_gender' kpi
	,case gender
		when 'M' then 'Male'
		when 'F' then 'Female'
		else 'Unknown'
	end kpi_sub
	,channel chnl
	,count(po_num) kpi_val
	,reporting_date
from
	customers_table_mthly
where
	reporting_date = '{rpt_dt}'
group by
	gender
	,channel
	,reporting_date
union
select
	'customers_tenure' kpi
	,case
		when tenure < 1 then '<1 year'
		when tenure >= 1 and tenure < 2 then '1<= year <2'
		when tenure >= 2 and tenure < 3 then '2<= year <3'
		when tenure >= 3 and tenure < 4 then '3<= year <4'
		when tenure >= 4 and tenure < 5 then '4<= year <5'
		when tenure >= 5 and tenure < 10 then '5<= year <10'
		else '>10 years'
	end kpi_sub
	,channel chnl
	,count(po_num) kpi_val
	,reporting_date
from
	customers_table_mthly
where
	reporting_date = '{rpt_dt}'
group by
	tenure
	,channel
	,reporting_date
union
select
	'sustomers_income' kpi
	,case
		when mthly_incm < 10000 then '<10 mil'
		when mthly_incm >= 10000 and mthly_incm < 20000 then '10mil - <20 mil'
		when mthly_incm >= 20000 and mthly_incm < 40000 then '20mil - <40 mil'
		when mthly_incm >= 40000 and mthly_incm < 60000 then '40mil - <60 mil'
		when mthly_incm >= 60000 and mthly_incm < 80000 then '60mil - <80 mil'
		else '>80 mil'
	end	kpi_sub
	,channel chnl
	,count(po_num) kpi_val
	,reporting_date
from
	customers_table_mthly
where
	reporting_date = '{rpt_dt}'
group by
	mthly_incm
	,channel
	,reporting_date
""")

#rs2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC rs3

# COMMAND ----------

t_cust_dtl.createOrReplaceTempView('t_cust_dtl')

rs3 = spark.sql(f"""
select
	'CWS_registration' kpi
	,cws_reg as kpi_sub
	,cust.channel chnl
	,count(po_num) kpi_val
	,cust.reporting_date 
from
	customers_table_mthly cust
where
	reporting_date = '{rpt_dt}'
group by
	cws_reg
	,cust.channel
	,cust.reporting_date
union all
select
	'digital_active_customers' kpi
	,null as kpi_sub
	,null chnl
	,0 kpi_val
	,a.reporting_date
from
	(		
		select
			reporting_date			
			,count(case when digital_reg_ind = 'Y' then po_num end) val
		from
			customers_table_mthly
		where
			reporting_date = '{rpt_dt}'
		group by
			reporting_date			
	) a
union all
select
	'digital_payment' kpi
	,pmt_onl_ind kpi_sub
	,channel chnl
	,count(po_num) kpi_val
	,reporting_date
from
	customers_table_mthly
where
	reporting_date = '{rpt_dt}'
	and pmt_ind = 'Y'
group by
	pmt_onl_ind
	,channel
	,reporting_date
union all
select
	'claim_online' kpi
	,case when clm_cws_ind = 'Y' or clm_ezc_ind = 'Y' then 'Y' else 'N' end kpi_sub
	,channel chnl
	,count(po_num) kpi_val
	,reporting_date
from
	customers_table_mthly
where
	reporting_date = '{rpt_dt}'
	and clm_ind = 'Y'
group by
	case when clm_cws_ind = 'Y' or clm_ezc_ind = 'Y' then 'Y' else 'N' end
	,channel
	,reporting_date
union all
select
	'Digital_product_volume' kpi
	,b.nb_user_id kpi_sub
	,a.channel chnl
	,count(b.pol_num) kpi_val
	,a.reporting_date
from
	customers_table_mthly a
	inner join (
		select distinct			
			cpl.cli_num
			,pol.pol_num
			,pol.nb_user_id
			,pol.image_date
		from
			tpolicys pol
			inner join tclient_policy_links cpl on (pol.image_date = cpl.image_date and pol.pol_num = cpl.pol_num and cpl.link_typ = 'O')
		where
			pol.nb_user_id in ('SHOPEE','ECOMMERCE')
			and last_day(pol.pol_iss_dt) = pol.image_date
			and pol.pol_stat_cd in ('1','2','3','5')
	) b on (a.reporting_date = b.image_date and a.po_num = b.cli_num)
where
	a.reporting_date = '{rpt_dt}'
group by
	b.nb_user_id
	,a.channel
	,a.reporting_date
union all
select
	'policies_per_cust_existing' kpi
	,case
		when pol_cnt = 1 then '1 policy'
		when pol_cnt < 5 then concat(pol_cnt,' policies')
		else '>= 5 policies'
	end kpi_sub
	,channel chnl	
	,count(po_num) kpi_val
	,reporting_date
from
	t_cust_dtl
where
	new_cust_ytd_ind = 'N'
group by
	case
		when pol_cnt = 1 then '1 policy'
		when pol_cnt < 5 then concat(pol_cnt,' policies')
		else '>= 5 policies'
	end
	,channel		
	,reporting_date
union all
select
	'policies_per_cust_new_ytd' kpi
	,case
		when pol_cnt = 1 then '1 policy'
		when pol_cnt < 5 then concat(pol_cnt,' policies')
		else '>= 5 policies'
	end kpi_sub
	,channel chnl	
	,count(po_num) kpi_val
	,reporting_date
from
	t_cust_dtl
where
	new_cust_ytd_ind = 'Y'
group by
	case
		when pol_cnt = 1 then '1 policy'
		when pol_cnt < 5 then concat(pol_cnt,' policies')
		else '>= 5 policies'
	end
	,channel		
	,reporting_date
union all
select
	'coverages_per_cust' kpi
	,case
		when rid_cnt = 1 then '1 rider'
		when rid_cnt = 2 then '2 riders'
		when rid_cnt >= 3 and rid_cnt <= 5 then '3 - 5 riders'
		when rid_cnt > 5 and rid_cnt <= 10 then '6 - 10 riders'
		else '> 10 riders'
	end kpi_sub
	,channel chnl	
	,count(po_num) kpi_val
	,reporting_date
from
	t_cust_dtl
group by
	case
		when rid_cnt = 1 then '1 rider'
		when rid_cnt = 2 then '2 riders'
		when rid_cnt >= 3 and rid_cnt <= 5 then '3 - 5 riders'
		when rid_cnt > 5 and rid_cnt <= 10 then '6 - 10 riders'
		else '> 10 riders'
	end
	,channel		
	,reporting_date              
""")

#rs3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC rs4

# COMMAND ----------

plan_code.createOrReplaceTempView('vn_plan_code_map')

rs4 = spark.sql(f"""
select
	'product_distribution' kpi
	,b.product_group kpi_sub
	,a.channel chnl
	,count(distinct a.po_num) kpi_val
	,a.reporting_date
from
	customers_table_mthly a
	inner join (
		select distinct
			pol.image_date
			,case
				when instr('EDB10EDB11EDB12EDB13EDB14EDB15EDB16EDB17EDB18EDB19EDB20EDB21EDB22EDB23EDB24EDB25EDB26EDB27EDF23EDF24EDF25EDF26EDF27',pol.plan_code_base) > 0 then 'Education'
				when instr('ENC12ENC15ENC20ENF12ENF15ENF20ENM12ENM15ENM20',pol.plan_code_base) > 0 then '2018 CI Endowment'		
				when instr('EIC12EIC15EIC20EIF12EIF15EIF20EIM12EIM15EIM20',pol.plan_code_base) > 0 then 'CIE'
				when instr('RUV01RUV02RUV03',pol.plan_code_base) > 0 then 'RPVL (Unit-linked)'
				when instr('UL038UL039UL040UL041',pol.plan_code_base) > 0 then 'Universal Life 2019'
				when pol.plan_code_base = 'CA360' then 'Cancer360'
				when instr('RPA03, RPA05, RPA10, RPB03, RPB05, RPB10',pol.plan_code_base)>0 then 'Term ROP'
				when instr('UL035UL036UL037',pol.plan_code_base) > 0 then 'AUL'
				when pol.plan_code_base = 'UL007' then '2015 Universal Life'
				when pol.nb_user_id = 'SHOPEE' then 'Shopee'
				when pol.plan_code_base = 'FDB01' and cvg.vers_num in ('07','08','09') then 'Shopee'
				else
					case
						when need.plan_code = 'UL007' then '2015 Universal Life'
						when instr(lcase(need.product_name),'universal life') > 0 then 'Universal Life'				
						when instr(lcase(need.product_name),'education') > 0 then 'Education'
						when instr(lcase(need.product_name),'my life') > 0 then 'My Life'
						when instr(lcase(need.product_name),'my family') > 0 then 'My Family'
						when instr(lcase(need.product_name),'premier care') > 0 then 'Premier Care'
						when instr(lcase(need.product_name),'premier lady') > 0 then 'Premier Lady'
						when instr(lcase(need.product_name),'savings') > 0 then 'Savings'
						when instr(lcase(need.product_name),'endowment') > 0 then 'Endowment'
						when instr(lcase(need.product_name),'flexible en') > 0 then substr(need.product_name,1,length(need.product_name)-3)
						when instr(lcase(need.product_name),'accident') > 0 then 'Accident'
						when instr(lcase(need.product_name),'term premium') > 0 then 'Term Premium'
						else 'Others'
					end		
			end product_group
			,cpl.cli_num
		from
			tpolicys pol
			inner join tcoverages cvg on (pol.image_date = cvg.image_date and pol.pol_num = cvg.pol_num and cvg.cvg_typ = 'B')
			inner join tclient_policy_links cpl on (pol.image_date = cpl.image_date and pol.pol_num = cpl.pol_num and cpl.link_typ = 'O')
			left join (
				select
					*
				from
					(
						select
							plan_code
							,product_name
							,customer_needs
							,row_number() over(partition by PLAN_CODE order by EFFECTIVE_DATE desc) rw_num
						from
							vn_plan_code_map a				
					) rs where rw_num = 1
			) need on (pol.plan_code_base = need.plan_code)
		where
			pol.pol_stat_cd in ('1','2','3','5')
	) b on (a.reporting_date = b.image_date and a.po_num = b.cli_num)
where
	a.reporting_date = '{rpt_dt}'
group by
	b.product_group
	,a.channel
	,a.reporting_date              
""")

#rs_4.display()

# COMMAND ----------

# MAGIC %md
# MAGIC rs5

# COMMAND ----------

tfields.createOrReplaceTempView('tfield_values')
t_ucm_grp.createOrReplaceTempView('t_ucm_grp')
t_move.createOrReplaceTempView('t_move')

rs5 = spark.sql(f"""
select
	'avg_pol_per_cust' kpi
	,null as kpi_sub
	,channel chnl
	,sum(pol_cnt)/count(po_num) kpi_val
	,reporting_date
from
	t_cust_dtl
group by
	channel
	,reporting_date
union all
select
	'avg_rid_per_cust' kpi
	,null as kpi_sub
	,channel chnl
	,sum(rid_cnt)/count(po_num) kpi_val
	,reporting_date
from
	t_cust_dtl
group by
	channel
	,reporting_date
union all
select
	'avg_ape_per_cust' kpi
	,null as kpi_sub
	,channel chnl
	,sum(base_ape + rid_ape)*1000/count(po_num) kpi_val
	,reporting_date
from
	t_cust_dtl
group by
	channel
	,reporting_date
union all
select
	'avg_case_size' kpi
	,null as kpi_sub
	,channel chnl
	,sum(base_ape + rid_ape)*1000/sum(pol_cnt) kpi_val
	,reporting_date
from
	t_cust_dtl
group by
	channel
	,reporting_date
union all
select
	'avg_ape_base' kpi
	,null as kpi_sub
	,channel chnl
	,sum(base_ape)*1000/sum(pol_cnt) kpi_val
	,reporting_date
from
	t_cust_dtl
group by
	channel
	,reporting_date
union
select all
	'avg_ape_rider' kpi
	,null as kpi_sub
	,channel chnl
	,sum(rid_ape)*1000/sum(rid_cnt) kpi_val
	,reporting_date
from
	t_cust_dtl
group by
	channel
	,reporting_date
union all
select
	'customer_status' as kpi
	,def as kpi_sub
	,channel chnl
	,count(po_num) as kpi_val
	,reporting_date
from
	t_ucm_grp
group by
	reporting_date
	,def
	,channel
union all
select
	'policy_payment_mode' kpi
	,tv.fld_valu_desc_eng sub_kpi
	,cus.channel chnl
	,count(distinct pol.pol_num) kpi_val
	,cus.reporting_date
from
	tpolicys pol
	inner join tclient_policy_links cpl on (pol.image_date = cpl.image_date and pol.pol_num = cpl.pol_num and cpl.link_typ = 'O')
	inner join tfield_values tv on (pol.image_date = tv.image_date and pol.pmt_mode = tv.fld_valu and tv.fld_nm = 'PMT_MODE')
	inner join customers_table_mthly cus on (cpl.image_date = cus.reporting_date and cpl.cli_num = cus.po_num)
where
	pol.image_date = '{rpt_dt})'
	and pol.pol_stat_cd in ('1','2','3','5')
	and pol.plan_code_base <> 'MI007'
group by
	tv.fld_valu_desc_eng
	,cus.channel
	,cus.reporting_date
union all
select
	kpi
	,sub_kpi
	,channel
	,kpi_val	
	,reporting_date
from
	t_move              
""")

#rs_5.display()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Merge all metrics</strong><br>
# MAGIC and write to file

# COMMAND ----------

cx_db_company_kpis = rs1.union(rs2).union(rs3).union(rs4).union(rs5)

cx_db_company_kpis.write.mode("overwrite").partitionBy('reporting_date').parquet(f"{dest_path}cx_db_company_kpis/")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Metrics for Banks

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>brs1</strong>

# COMMAND ----------

t_cust_dtl_bnk.createOrReplaceTempView('t_cust_dtl_bnk')

brs1 = spark.sql(f"""
select
	'unique_customers_bnk' kpi
	,null as kpi_sub
	,bank_cd
	,count(po_num) kpi_val
	,reporting_date
from
	t_cust_dtl_bnk
group by
	reporting_date
	,bank_cd
union
select
	'policies_per_cust_bnk' kpi
	,case
		when pol_cnt = 1 then '1 policy'
		when pol_cnt < 5 then concat(pol_cnt,' policies')
		else '>= 5 policies'
	end kpi_sub
	,bank_cd	
	,count(po_num) kpi_val
	,reporting_date
from
	t_cust_dtl_bnk
group by
	case
		when pol_cnt = 1 then '1 policy'
		when pol_cnt < 5 then concat(pol_cnt,' policies')
		else '>= 5 policies'
	end
	,bank_cd		
	,reporting_date
union
select
	'coverages_per_cust_bnk' kpi
	,case
		when rid_cnt = 1 then '1 rider'
		when rid_cnt = 2 then '2 riders'
		when rid_cnt >= 3 and rid_cnt <= 5 then '3 - 5 riders'
		when rid_cnt > 5 and rid_cnt <= 10 then '6 - 10 riders'
		else '> 10 riders'
	end kpi_sub
	,bank_cd	
	,count(po_num) kpi_val
	,reporting_date
from
	t_cust_dtl_bnk
group by
	case
		when rid_cnt = 1 then '1 rider'
		when rid_cnt = 2 then '2 riders'
		when rid_cnt >= 3 and rid_cnt <= 5 then '3 - 5 riders'
		when rid_cnt > 5 and rid_cnt <= 10 then '6 - 10 riders'
		else '> 10 riders'
	end
	,bank_cd		
	,reporting_date
union
select
	'product_distribution_bnk' kpi
	,b.product_group kpi_sub
	,bnk.bank_cd
	,count(distinct a.po_num) kpi_val
	,a.reporting_date
from
	customers_table_mthly a
	inner join (		
		select distinct
			cli_num
			,substr(agt.loc_code,1,3) bank_cd
		from
			tpolicys pol
			inner join tclient_policy_links cpl on (pol.pol_num = cpl.pol_num and cpl.link_typ = 'O')
			inner join tams_agents agt on (pol.agt_code = agt.agt_code)		
		where
			agt.chnl_cd = '03'
			and agt.stat_cd = '01'
			and cpl.rec_status = 'A'
			and pol.pol_stat_cd in ('1','2','3','5')
	) bnk on (a.po_num = bnk.cli_num)
	inner join (
		select distinct
			pol.image_date
			,case
				when instr('EDB10EDB11EDB12EDB13EDB14EDB15EDB16EDB17EDB18EDB19EDB20EDB21EDB22EDB23EDB24EDB25EDB26EDB27EDF23EDF24EDF25EDF26EDF27',pol.plan_code_base) > 0 then 'Education'
				when instr('ENC12ENC15ENC20ENF12ENF15ENF20ENM12ENM15ENM20',pol.plan_code_base) > 0 then '2018 CI Endowment'		
				when instr('EIC12EIC15EIC20EIF12EIF15EIF20EIM12EIM15EIM20',pol.plan_code_base) > 0 then 'CIE'
				when instr('RUV01RUV02RUV03',pol.plan_code_base) > 0 then 'RPVL (Unit-linked)'
				when instr('UL038UL039UL040UL041',pol.plan_code_base) > 0 then 'Universal Life 2019'
				when pol.plan_code_base = 'CA360' then 'Cancer360'
				when instr('RPA03, RPA05, RPA10, RPB03, RPB05, RPB10',pol.plan_code_base)>0 then 'Term ROP'
				when instr('UL035UL036UL037',pol.plan_code_base) > 0 then 'AUL'
				when pol.plan_code_base = 'UL007' then '2015 Universal Life'
				when pol.nb_user_id = 'SHOPEE' then 'Shopee'
				when pol.plan_code_base = 'FDB01' and cvg.vers_num in ('07','08','09') then 'Shopee'
				else
					case
						when need.plan_code = 'UL007' then '2015 Universal Life'
						when instr(lcase(need.product_name),'universal life') > 0 then 'Universal Life'				
						when instr(lcase(need.product_name),'education') > 0 then 'Education'
						when instr(lcase(need.product_name),'my life') > 0 then 'My Life'
						when instr(lcase(need.product_name),'my family') > 0 then 'My Family'
						when instr(lcase(need.product_name),'premier care') > 0 then 'Premier Care'
						when instr(lcase(need.product_name),'premier lady') > 0 then 'Premier Lady'
						when instr(lcase(need.product_name),'savings') > 0 then 'Savings'
						when instr(lcase(need.product_name),'endowment') > 0 then 'Endowment'
						when instr(lcase(need.product_name),'flexible en') > 0 then substr(need.product_name,1,length(need.product_name)-3)
						when instr(lcase(need.product_name),'accident') > 0 then 'Accident'
						when instr(lcase(need.product_name),'term premium') > 0 then 'Term Premium'
						else 'Others'
					end		
			end product_group
			,cpl.cli_num
		from
			tpolicys pol
			inner join tcoverages cvg on (pol.image_date = cvg.image_date and pol.pol_num = cvg.pol_num and cvg.cvg_typ = 'B')
			inner join tclient_policy_links cpl on (pol.image_date = cpl.image_date and pol.pol_num = cpl.pol_num and cpl.link_typ = 'O')
			left join (
				select
					*
				from
					(
						select
							plan_code
							,product_name
							,customer_needs
							,row_number() over(partition by PLAN_CODE order by EFFECTIVE_DATE desc) rw_num
						from
							vn_plan_code_map a				
					) rs where rw_num = 1
			) need on (pol.plan_code_base = need.plan_code)
		where
			pol.pol_stat_cd in ('1','2','3','5')
	) b on (a.reporting_date = b.image_date and a.po_num = b.cli_num)
where
	a.reporting_date = '{rpt_dt}'
group by
	b.product_group
	,bnk.bank_cd
	,a.reporting_date
""")

#brs1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>brs2</strong>

# COMMAND ----------

brs2 = spark.sql(f"""
select
	'avg_pol_per_cust_bnk' kpi
	,'' as kpi_sub
	,bank_cd
	,sum(pol_cnt)/count(po_num) kpi_val
	,reporting_date
from
	t_cust_dtl_bnk
group by
	bank_cd
	,reporting_date
union
select
	'avg_rid_per_cust_bnk' kpi
	,'' as kpi_sub
	,bank_cd
	,sum(rid_cnt)/count(po_num) kpi_val
	,reporting_date
from
	t_cust_dtl_bnk
group by
	bank_cd
	,reporting_date
union
select
	'avg_ape_per_cust_bnk' kpi
	,'' as kpi_sub
	,bank_cd
	,sum(base_ape + rid_ape)*1000/count(po_num) kpi_val
	,reporting_date
from
	t_cust_dtl_bnk
group by
	bank_cd
	,reporting_date
union
select
	'avg_case_size_bnk' kpi
	,'' as kpi_sub
	,bank_cd
	,sum(base_ape + rid_ape)*1000/sum(pol_cnt) kpi_val
	,reporting_date
from
	t_cust_dtl_bnk
group by
	bank_cd
	,reporting_date
union
select
	'avg_ape_base_bnk' kpi
	,'' as kpi_sub
	,bank_cd chnl
	,sum(base_ape)*1000/sum(pol_cnt) kpi_val
	,reporting_date
from
	t_cust_dtl_bnk
group by
	bank_cd
	,reporting_date
union
select
	'avg_ape_rider_bnk' kpi
	,'' as kpi_sub
	,bank_cd
	,sum(rid_ape)*1000/sum(rid_cnt) kpi_val
	,reporting_date
from
	t_cust_dtl_bnk
group by
	bank_cd
	,reporting_date                 
""")

#brs2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>brs3</strong>

# COMMAND ----------

brs3 = spark.sql(f"""
select
	'policy_payment_mode_bnk' kpi
	,tv.fld_valu_desc_eng sub_kpi
	,bnk.bank_cd
	,count(distinct pol.pol_num) kpi_val
	,cus.reporting_date
from
	tpolicys pol
	inner join tclient_policy_links cpl on (pol.image_date = cpl.image_date and pol.pol_num = cpl.pol_num and cpl.link_typ = 'O')
	inner join tfield_values tv on (pol.image_date = tv.image_date and pol.pmt_mode = tv.fld_valu and tv.fld_nm = 'PMT_MODE')
	inner join customers_table_mthly cus on (cpl.image_date = cus.reporting_date and cpl.cli_num = cus.po_num)
	inner join (		
		select distinct
			cli_num
			,substr(agt.loc_code,1,3) bank_cd			
		from			  
			tpolicys pol
			inner join tclient_policy_links cpl on (pol.pol_num = cpl.pol_num and cpl.link_typ = 'O')
			inner join tams_agents agt on (pol.agt_code = agt.agt_code)
		where
			pol.pol_stat_cd in ('1','2','3','5')				  
			and cpl.rec_status = 'A'
			and agt.chnl_cd = '03'
			and agt.stat_cd = '01'		
	) bnk on (cus.po_num = bnk.cli_num)
where
	pol.image_date = '{rpt_dt}'
	and pol.pol_stat_cd in ('1','2','3','5')
	and pol.plan_code_base <> 'MI007'
group by
	tv.fld_valu_desc_eng
	,bnk.bank_cd
	,cus.reporting_date              
""")

#brs3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Merge all metrics</strong><br>
# MAGIC and write to file

# COMMAND ----------

cx_db_company_kpis_bnk = brs1.union(brs2).union(brs3)

cx_db_company_kpis_bnk.write.mode("overwrite").partitionBy('reporting_date').parquet(f"{dest_path}cx_db_company_kpis_bnk/")
