# Databricks notebook source
# MAGIC %md
# MAGIC ### Apend data from ADEC table

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load libl</strong>

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import date, timedelta

spark = SparkSession.builder.appName("CX").getOrCreate()
spark = spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load tables and params</strong>

# COMMAND ----------

today = date.today()
last_mthend = today.replace(day=1) - timedelta(days=1)
rpt_dt = last_mthend.strftime('%Y-%m-%d')
rpt_num = 2307
print(rpt_dt)

source_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/cpm/ADEC/WorkingData/'
dest_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/scratch/ORC_ACID_to_Parquet_Conversion/apps/hive/warehouse/vn_processing_datamart_temp_db.db/customers_table_mthly/'

snapshot_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/'

tpol_mthend_path = 'TPOLICYS/'
tcli_pol_mthend_path = 'TCLIENT_POLICY_LINKS/'
tcli_mthend_path = 'TCLIENT_DETAILS/'

customer_table = spark.read.parquet(f"{source_path}customer_table_{rpt_num}")
tpol_mthend = spark.read.parquet(f"{snapshot_path}{tpol_mthend_path}")
tcli_pol_mthend = spark.read.parquet(f"{snapshot_path}{tcli_pol_mthend_path}")
tcli_mthend = spark.read.parquet(f"{snapshot_path}{tcli_mthend_path}")

# Filter to select records for reporting month only
tpol_mthend = tpol_mthend.filter(col('image_date') == rpt_dt)
tcli_pol_mthend = tcli_pol_mthend.filter(col('image_date') == rpt_dt)
tcli_mthend = tcli_mthend.filter(col('image_date') == rpt_dt)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Append data into existing dataset</strong>

# COMMAND ----------

tpol_mthend.createOrReplaceTempView('tpolicys')
tcli_pol_mthend.createOrReplaceTempView('tclient_policy_links')
tcli_mthend.createOrReplaceTempView('tclient_details')
customer_table.createOrReplaceTempView(f'customer_table_{rpt_num}')

customers_table_mthly = spark.sql(f"""                                  
select distinct
	rst.po_num
	,gender
	,birth_dt
	,age_curr
	,age_iss_group
	,age_curr_group
	,customer_type
	,loc_code_agt
	,mobile_contact
	,email_contact
	,occp_code
	,mthly_incm
	,full_address
	,city
	,pol_iss_dt
	,pol_eff_dt
	,frst_iss_dt
	,agt_code
	,segment_type
	,max_no_dpnd
	,channel
	,tenure
	,cws_reg
	,cws_180_act -- replace by cws_lst_login_dt from Mar-2022
	,clm_ind
	,clm_cws_ind
	,clm_ezc_ind
	,pmt_cnt
	,pmt_onl_cnt
	,tot_ape
	,ape_mtd
	,nbv
	,nbv_mtd
	,pmt_ind
	,pmt_onl_ind
	,prod_cnt
	,prod_cnt_mtd
	,need_cnt
	,need_type
	,sms_ind
	,digital_reg_ind	
	,digital_reg_ind_v2
	,claim_reg
	,move_reg
	,e_pmt_reg
	,move_180_act -- replace by move_lst_login_dt from Mar-2022
	,e_pmt_180_act -- replace by e_pmt_lst_trxn_dt from Mar-2022
	,e_clm_180_act -- replace by e_clm_lst_trxn_dt from Mar-2022
	,case
        when rst.tot_ape >= 20000 and rst.tot_ape < 65000 and sq1.10yr_pol_cnt >= 1 then 'Silver'
        when rst.tot_ape >= 65000 and rst.tot_ape < 150000 then 'Gold'        
        when rst.tot_ape >= 150000 and rst.tot_ape < 300000 then 'Platinum'
        when rst.tot_ape >= 300000 then 'Platinum Elite'
        else 'Not VIP'
    end vip_seg
	,case when (year(reporting_date) = year(cn.first_eff_pol) and month(reporting_date) = month(cn.first_eff_pol)) then 'Y' else 'N' end cust_new_mtd
	,case when year(reporting_date) = year(cn.first_eff_pol) then 'Y' else 'N' end cust_new_ytd
	,ph.pol_cnt policies_holding
	,cws_lst_login_dt
	,e_clm_lst_trxn_dt
	,e_pmt_lst_trxn_dt
	,move_lst_login_dt
	,reporting_date
from
	(
		select
			po_num
			,gender
			,birth_dt
			,age_curr
			,age_iss_group
			,age_curr_group
			,customer_type
			,loc_code_agt
			,mobile_contact
			,email_contact
			,occp_code
			,mthly_incm
			,full_address
			,city
			,pol_iss_dt
			,pol_eff_dt
			,frst_iss_dt
			,agt_code
			,segment_type
			,max_no_dpnd
			,channel
			,tenure
			,cws_reg
			,'NaN' as cws_180_act
			,clm_ind
			,clm_cws_ind
			,clm_ezc_ind
			,pmt_cnt
			,pmt_onl_cnt
			,tot_ape
			,ape_mtd
			,nbv
			,nbv_mtd
			,pmt_ind
			,pmt_onl_ind
			,prod_cnt
			,prod_cnt_mtd
			,need_cnt
			,need_type
			,sms_ind
			,digital_reg_ind -- not in use
			,digital_reg_ind_v2
			,claim_reg
			,move_reg
			,e_pmt_reg
			,'NaN' as move_180_act
			,'NaN' as e_pmt_180_act
			,'NaN' as e_clm_180_act
			,cws_lst_login_dt
			,e_clm_lst_trxn_dt
			,e_pmt_lst_trxn_dt
			,move_lst_login_dt
			,'{rpt_dt}' reporting_date
		from
			customer_table_{rpt_num}
	) rst
	left join (
		select
			cpl.cli_num			
			,count(case when (floor(months_between(current_date, pol.pol_iss_dt)/12) >= 10 and pol.pol_stat_cd in ('1','2','3','5')) then pol.pol_num end) as 10yr_pol_cnt
		from
			tpolicys pol
			inner join tclient_policy_links cpl on (pol.pol_num = cpl.pol_num and cpl.link_typ = 'O')
			inner join tclient_details cli on cpl.cli_num = cli.cli_num
		group by
			cpl.cli_num
	) sq1 on rst.po_num = sq1.cli_num
	left join (
		select
			cpl.cli_num
			,min(pol.pol_eff_dt) first_eff_pol
		from
			tpolicys pol
			inner join tclient_policy_links cpl on (pol.pol_num = cpl.pol_num and cpl.link_typ = 'O')
		group by
			cpl.cli_num
	) cn on (rst.po_num = cn.cli_num)
	left join (
		select
			cpl.cli_num
			,count(pol.pol_num) pol_cnt
		from
			tpolicys pol
			inner join tclient_policy_links cpl on (pol.pol_num = cpl.pol_num and cpl.link_typ = 'O')
		where
			pol.pol_stat_cd in ('1','2','3','5')
		group by
			cpl.cli_num
	) ph on (rst.po_num = ph.cli_num)
""")

#print("No. of records:", customers_table_mthly.count())

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Write data back into parquet folder</strong>

# COMMAND ----------

customers_table_mthly.write.mode("overwrite").partitionBy("reporting_date").parquet(f"{dest_path}")
