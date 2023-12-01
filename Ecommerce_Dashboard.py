# Databricks notebook source
# MAGIC %md
# MAGIC # ECOMMERCE DASHBOARD

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load functions</strong>

# COMMAND ----------

# MAGIC %run /Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1. Generate data for daily Ecommerce

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load libl, paths and params</strong>

# COMMAND ----------

from pyspark.sql.functions import *

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

lab_path = '/mnt/lab/vn/project/'
ams_path = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_AMS_DB/'
stg_path = '/mnt/prod/Staging/Incremental/VN_PUBLISHED_CAMPAIGN_FILEBASED_DB/'
cas_path = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
cpm_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_CAMPAIGN_DB/'
rpt_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_REPORTS_DB/'
sc_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/'
dm_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/'

dest_source = 'dashboard/ECOMMERCE_DASHBOARD/'
tmkt_source = 'TMKT_SUBMISSION/'
pol_source = 'TPOLICYS/'
lnk_source = 'TCLIENT_POLICY_LINKS/'
agt_source = 'TAMS_AGENTS/'
sc_source = 'AGENT_SCORECARD/'
nbv_source = 'NBV_MARGIN_HISTORIES/'
pln_source = 'VN_PLAN_CODE_MAP/'
loc_source = 'LOC_TO_SM_MAPPING/'
tpoli_source = 'TPOLIDM_DAILY/'
tcust_source = 'TCUSTDM_DAILY/'
tagt_source = 'TAGTDM_DAILY/'

abfss_paths = [ams_path,stg_path,cas_path,cpm_path,rpt_path,sc_path,dm_path]
parquet_files = [dest_source,tmkt_source,pol_source,lnk_source,agt_source,sc_source,
                 nbv_source,pln_source,loc_source,tpoli_source,tcust_source,tagt_source]

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load all working tables into Spark dfs</strong>

# COMMAND ----------

list_df = load_parquet_files(abfss_paths, parquet_files)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Generate table temp views</strong>

# COMMAND ----------

generate_temp_view(list_df)

# COMMAND ----------

ecomDF = spark.sql("""
select	ecom.source as source,
		ecom.api_import_time as api_import_time,
		ecom.cli_num as cli_num,
		ecom.pol_num as pol_num,
		ecom.agt_code as agent_code,
		ecom.agent_tier as agent_tier,
		ecom.agent_location_code as agent_location_code,
		ecom.batch_trxn_dt as batch_trxn_dt,
		ecom.batch_trxn_amt as batch_trxn_amt,
		ecom.NBV as nbv,
		ecom.shopee_order_id as shopee_order_id,
		ecom.plan_code_base as plan_code,
		ecom.vers_num_base as vers_num,
		ecom.pol_iss_dt as pol_iss_dt,
		ecom.pol_eff_dt as policy_effective_date,
		ecom.pol_stat_cd as policy_status_code,
		ecom.case_status as case_status,
		ecom.agent_code_level_6 as agent_code_level_6,
		ecom.agent_name_level_6 as agent_name_level_6,
		ecom.case_count as case_count,
		ecom.reporting_date as reporting_date, 
        noe.pol_num as cc_upsell,
        noe.plan_code_base as cc_plancode,
		noe.nbv_factor_group as cc_productgroup,
        TO_DATE(noe.pol_iss_dt) as iss_dt_upsell, 
        NVL(noe.APE_Upsell,0) as ape_upsell,
		CASE when org.cli_num is null then 'Y' else 'N' END as new_ind,
		NVL(noe.NBV_Upsell,0) as nbv_upsell
from	(with mkt as (
select  source, pol_num, batch_trxn_amt, batch_trxn_dt, max(shopee_order_id) shopee_order_id,
  		case_status, max(case_count) case_count, 
		last_day(max(batch_trxn_dt)) reporting_date, max(api_import_time) api_import_time
from	tmkt_submission 
where   shopee_order_id is not null
group by
  		source, pol_num, batch_trxn_amt, batch_trxn_dt, case_status
),
sc as (
select	agt_code, agent_tier, monthend_dt,
        row_number() over (partition by agt_code, monthend_dt order by agt_code, monthend_dt desc) rn
from	agent_scorecard 
)
select	distinct
		mkt.source,
		TO_DATE(mkt.api_import_time) api_import_time,
		tb2.cli_num,
		tb1.pol_num,
		tb1.agt_code,
		NVL(sc.agent_tier,'Unknown') agent_tier,
		tb3.loc_code agent_location_code,
		mkt.batch_trxn_dt,
		ROUND(NVL(mkt.batch_trxn_amt,mode_prem*12/cast(pmt_mode as int)),2) batch_trxn_amt,
		mkt.shopee_order_id,
		tb1.plan_code_base,
		tb1.vers_num_base,
		tb1.dist_chnl_cd,
		TO_DATE(tb1.pol_iss_dt) as pol_iss_dt,
		TO_DATE(tb1.pol_eff_dt) as pol_eff_dt,
		tb1.pol_stat_cd,
		mkt.case_status,
		loc.rh_code agent_code_level_6,
		loc.rh_name agent_name_level_6,
		NVL(mkt.case_count,1) as case_count,
		LAST_DAY(TO_DATE(tb1.pol_iss_dt)) as reporting_date,
		CASE when tb3.loc_code is null then 
				(case when tb1.dist_chnl_cd in ('03','10','14','16','17','18','19','22','23','24','25','29','30','31','32','33','39','41','44','47','49','51','52','53') then ROUND(NVL(mkt.batch_trxn_amt,mode_prem*12/cast(pmt_mode as int))*pln.nbv_margin_banca_other_banks,2) -- 'Banca'
					  when tb1.dist_chnl_cd in ('48') then ROUND(NVL(mkt.batch_trxn_amt,mode_prem*12/cast(pmt_mode as int))*pln.nbv_margin_other_channel_affinity,2)--'Affinity'
					  when tb1.dist_chnl_cd in ('01', '02', '08', '50', '*') then ROUND(NVL(mkt.batch_trxn_amt,mode_prem*12/cast(pmt_mode as int))*pln.nbv_margin_agency,2)--'Agency'
					  when tb1.dist_chnl_cd in ('05','06','07','34','36') then ROUND(NVL(mkt.batch_trxn_amt,mode_prem*12/cast(pmt_mode as int))*pln.nbv_margin_dmtm,2)--'DMTM'
					  when tb1.dist_chnl_cd in ('09') then ROUND(NVL(mkt.batch_trxn_amt,mode_prem*12/cast(pmt_mode as int))*-1.34,2)--'MI'
				 else ROUND(NVL(mkt.batch_trxn_amt,mode_prem*12/cast(pmt_mode as int))*pln.nbv_margin_other_channel,2) END) --'Unknown'
			 when tb1.dist_chnl_cd in ('*') then ROUND(NVL(mkt.batch_trxn_amt,mode_prem*12/cast(pmt_mode as int))*pln.nbv_margin_agency,2)--'Agency'
			 when tb3.loc_code like 'TCB%' then ROUND(NVL(mkt.batch_trxn_amt,mode_prem*12/cast(pmt_mode as int))*pln.nbv_margin_banca_tcb,2) --'TCB'
			 when tb3.loc_code like 'SAG%' then ROUND(NVL(mkt.batch_trxn_amt,mode_prem*12/cast(pmt_mode as int))*pln.nbv_margin_banca_scb,2) --'SCB'
			 else ROUND(NVL(mkt.batch_trxn_amt,mode_prem*12/cast(pmt_mode as int))*pln.nbv_margin_other_channel,2)
		END as NBV --'Unknown'
		from	tpolicys tb1 inner join
				tclient_policy_links tb2 on	tb1.pol_num=tb2.pol_num and	tb2.link_typ='O' left join
				mkt on tb1.pol_num=mkt.pol_num left join
				nbv_margin_histories pln on pln.plan_code=tb1.plan_code_base and floor(months_between(tb1.pol_iss_dt,pln.effective_date)) between 0 and 2 left join
				sc on tb1.agt_code=sc.agt_code and LAST_DAY(tb1.pol_iss_dt)=sc.monthend_dt and sc.rn=1 left join
				tams_agents tb3 on tb1.wa_cd_1=tb3.agt_code	left join 
				loc_to_sm_mapping loc on tb3.loc_code=loc.loc_cd
		where	YEAR(TO_DATE(tb1.pol_iss_dt)) >= YEAR(CURRENT_DATE)-2
			and	tb1.plan_code_base in ('FDB01','BIC01','BIC02','BIC03','BIC04','PN001')
			and tb1.pol_stat_cd not in ('8','A','N','R','X')
		) ecom
left join
		(select	tb2.cli_num, min(to_date(frst_iss_dt)) frst_iss_dt
		 from	tpolicys tb1 inner join 
				tclient_policy_links tb2 on tb1.pol_num=tb2.pol_num   
		 where	tb1.pol_stat_cd in ('1','2','3','5','7','9')
			and	tb1.plan_code_base not in ('FDB01','BIC01','BIC02','BIC03','BIC04','PN001')
		 group by tb2.cli_num
		) org
	 on	ecom.cli_num=org.cli_num AND ecom.pol_iss_dt>org.frst_iss_dt
left join 
		(select	tb2.cli_num,
				tb1.pol_num,
				tb1.plan_code_base,
				NVL(pln.nbv_factor_group,nbv.nbv_factor_group) as nbv_factor_group,
				TO_DATE(tb1.pol_iss_dt) as pol_iss_dt,
				tb1.agt_code,
				CASE when tb1.pmt_mode = '12' then 1 * tb1.mode_prem * 1000 
				  when tb1.pmt_mode = '06' then 2 * tb1.mode_prem * 1000 
				  when tb1.pmt_mode = '03' then 4 * tb1.mode_prem * 1000 
				  else 12 * tb1.mode_prem * 1000 
				END as APE_Upsell,
				CASE when agt.loc_code is null then 
						(case when tb1.dist_chnl_cd in ('03','10','14','16','17','18','19','22','23','24','25','29','30','31','32','33','39','41','44','47','49','51','52','53') then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_banca_other_banks,nbv.nbv_margin_banca_other_banks),2) -- 'Banca'
							  when tb1.dist_chnl_cd in ('48') then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_other_channel_affinity,nbv.nbv_margin_other_channel_affinity),2)--'Affinity'
							  when tb1.dist_chnl_cd in ('01', '02', '08', '50', '*') then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_agency,nbv.nbv_margin_agency),2)--'Agency'
							  when tb1.dist_chnl_cd in ('05','06','07','34','36') then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_dmtm,nbv.nbv_margin_dmtm),2)--'DMTM'
							  when tb1.dist_chnl_cd in ('09') then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*-1.34,2)--'MI'
						 else ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_other_channel,nbv.nbv_margin_other_channel),2) END) --'Unknown'
						 when tb1.dist_chnl_cd in ('01', '02', '08', '50', '*') then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_agency,nbv.nbv_margin_agency),2)--'Agency'
						 when agt.loc_code like 'TCB%' then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_banca_tcb,nbv.nbv_margin_banca_tcb),2) --'TCB'
						 when agt.loc_code like 'SAG%' then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_banca_scb,nbv.nbv_margin_banca_scb),2) --'SCB'
				    else ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_other_channel,nbv.nbv_margin_other_channel),2) END as NBV_Upsell		
		 from  tpolicys tb1
			inner join 
				tclient_policy_links tb2 
			 on tb1.pol_num=tb2.pol_num
			left join
				tams_agents agt
			 on	tb1.wa_cd_1=agt.agt_code
			left join
				nbv_margin_histories pln 
			 on pln.plan_code=tb1.plan_code_base and 
			    floor(months_between(tb1.pol_iss_dt,pln.effective_date)) between 0 and 2
			left join
				vn_plan_code_map nbv
			 on nbv.plan_code=tb1.plan_code_base
		 where  tb1.plan_code_base not in ('FDB01','BIC01','BIC02','BIC03','BIC04','PN001')
			and tb2.link_typ = 'O'
			and tb2.rec_status='A'
			and tb1.pol_stat_cd not in ('8','A','N','R','X')
		) noe
	 on	ecom.cli_num=noe.cli_num and ecom.pol_iss_dt<noe.pol_iss_dt and datediff(noe.pol_iss_dt,ecom.pol_iss_dt)<=180
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Write back to parquet</strong>

# COMMAND ----------

ecomDF.write.mode('overwrite').parquet(f'{lab_path}{dest_source}{tmkt_source}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2. Generate additional data for Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Generate ecomm_cat and ecomm_hld</strong>

# COMMAND ----------

ecom_cat = spark.sql("""
select	tb1.cli_num, 'Y' agt_ind
from	tcustdm_daily tb1 inner join
		tagtdm_daily tb2 on tb1.id_num=tb2.id_num         
""")

ecom_hld = spark.sql("""
select	po_num, COUNT(DISTINCT pol_num) no_pols, SUM(tot_ape) tot_ape
from	tpolidm_daily
where	pol_stat_cd in ('1','2','3','5')
group by
		po_num             
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Consolidate Ecommerce data with additional data</strong>

# COMMAND ----------

ecomDF.createOrReplaceTempView('ecom_tbl')
ecom_cat.createOrReplaceTempView('ecom_cat')
ecom_hld.createOrReplaceTempView('ecom_hld')

ecomm_digital_analysis = spark.sql("""
select	ecom.source,
		ecom.api_import_time,
		ecom.cli_num,
		cus.sex_code as gender,
		cus.cur_age as age_curr,
		case
        when hld.tot_ape >= 20000
            and hld.tot_ape < 65000
            and cus.cur_age-cus.frst_iss_age >= 10
										THEN '4.Silver'
        when hld.tot_ape >= 65000
            and hld.tot_ape < 150000    THEN '3.Gold'
        when hld.tot_ape >= 150000
            and hld.tot_ape < 300000    THEN '2.Platinum'
        when hld.tot_ape >= 300000      THEN '1.Platinum Elite'
										ELSE '5.Not VIP'
		end as vip_cat,
		cus.city,
		cus.cur_age-cus.frst_iss_age as tenure,
		hld.tot_ape,
		NVL(cat.agt_ind,'N') as agent_ind,
		ecom.pol_num,
		ecom.agent_code,
		ecom.agent_tier,
		ecom.agent_location_code,
		ecom.batch_trxn_dt,
		ecom.batch_trxn_amt,
		ecom.nbv,
		ecom.shopee_order_id,
		ecom.plan_code, --ecom.plan_name,
		ecom.vers_num,
		ecom.pol_iss_dt,
		ecom.policy_effective_date,
		ecom.policy_status_code,
		ecom.case_status,
		ecom.agent_code_level_6,
		ecom.agent_name_level_6,
		ecom.case_count,
		ecom.reporting_date, 
		hld.no_pols,
        ecom.cc_upsell, 
		ecom.cc_plancode,
		ecom.cc_productgroup,
        ecom.iss_dt_upsell, 
        ecom.ape_upsell,
		ecom.new_ind,
		ecom.nbv_upsell,
		row_number() over (partition by ecom.pol_num order by ecom.pol_num, ecom.pol_iss_dt) rn
from	ecom_tbl ecom inner join
		tcustdm_daily cus on ecom.cli_num=cus.cli_num left join
		ecom_cat cat on ecom.cli_num=cat.cli_num left join
		ecom_hld hld on	ecom.cli_num=hld.po_num
""")

# COMMAND ----------

ecom_analysisDF = ecomm_digital_analysis.select(
    "source",
    "api_import_time",
    "cli_num",
    "gender",
    "age_curr",
    "vip_cat",
    "city",
    "tenure",
    "tot_ape",
    "agent_ind",
    "pol_num",
    "agent_code",
    "agent_tier",
    "agent_location_code",
    "batch_trxn_dt",
    "batch_trxn_amt",
    "nbv",
    "shopee_order_id",
    "plan_code",
    "vers_num",
    "pol_iss_dt",
    "policy_effective_date",
    "policy_status_code",
    "case_status",
    "agent_code_level_6",
    "agent_name_level_6",
    "case_count",
    "reporting_date", 
    "no_pols",
    "cc_upsell", 
    "cc_plancode",
    "cc_productgroup",
    "iss_dt_upsell", 
    "ape_upsell",
    "new_ind",
    "nbv_upsell"
).where(
    (col("cc_upsell").isNotNull() & (col("rn") == 1)) | col("cc_upsell").isNull()
)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Write back to parquet</strong>

# COMMAND ----------

ecom_analysisDF.write.mode('overwrite').parquet(f'{lab_path}{dest_source}ECOMM_DIGITAL_ANALYSIS/')
