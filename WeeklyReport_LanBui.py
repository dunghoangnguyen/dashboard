# Databricks notebook source
# MAGIC %md
# MAGIC # Weekly Report for Lan Bui (Ops Metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load libls, params and paths</strong>

# COMMAND ----------

cas_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
ams_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_AMS_DB/'
rpt_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_REPORTS_DB/'
lab_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/dashboard/'

tpex_source = 'TPEXNB02VN/'
tpol_source = 'TPOLICYS/'
tclm_source = 'TCLAIMS_CONSO_ALL/'
tpos_source = 'TPOS_COLLECTION/'

output_dest = 'WEEKLYREPORT_LANBUI/'

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load working tables</strong>

# COMMAND ----------

tpexnb02vn = spark.read.parquet(f'{rpt_path}{tpex_source}')
tpolicys = spark.read.parquet(f'{cas_path}{tpol_source}')
tclaims_conso_all = spark.read.parquet(f'{rpt_path}{tclm_source}')
tpos_collection = spark.read.parquet(f'{rpt_path}{tpos_source}')

tpexnb02vn.createOrReplaceTempView('tpexnb02vn')
tpolicys.createOrReplaceTempView('tpolicys')
tclaims_conso_all.createOrReplaceTempView('tclaims_conso_all')
tpos_collection.createOrReplaceTempView('tpos_collection')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate metrics

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>digital metrics</strong>

# COMMAND ----------

digital_metrics = spark.sql("""
Select
	'Digital first premium payment' as type
	,transaction_date as event_date
	,count(case when substr(payment_method,1,4) <> 'CASH' then policy_number end) val
	,count(policy_number) total
from
	tpos_collection
where
	transaction_date >= date_add(current_date,-90)
	and premium_type = 'I'
	and plan_code <> 'MI007'
group by	
	'Digital first premium payment'
	,transaction_date
union
select
	'Digital submission for NB' as type
	,submit_date as event_date
	,count(case when (nb_user in ('EPOS','SHOPEE','ECOMMERCE') or (plan_code in ('CA360','CX360') and dist_chnl_cd = '24')) then tpexnb02vn.pol_num end) val
	,count(tpexnb02vn.pol_num) total	
from 
	tpexnb02vn inner join
 	tpolicys on (tpexnb02vn.pol_num = tpolicys.pol_num)
where
	type = 'SUBMIT'
	and submit_date >= date_add(current_date,-90)
group by
	'Digital submission for NB'
	,submit_date                       
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>volume metrics</strong>

# COMMAND ----------

volume_metrics = spark.sql("""
select	
	case type
		when 'SUBMIT' then "New business - Submitted volume"
		else "New business - Issued volume"
	end as type
	,case type
		when 'SUBMIT' then submit_date
		else issue_date
	end  as event_date
	,count(pol_num) as val	
from
	tpexnb02vn
where
	(
		submit_date >= date_add(current_date,-90)
		or issue_date >= date_add(current_date,-90)
	)
group by
	case type
		when 'SUBMIT' then "New business - Submitted volume"
		else "New business - Issued volume"
	end
	,case type
		when 'SUBMIT' then submit_date
		else issue_date
	end
union
SELECT
	'Claim - Submitted volume' as type
	,claim_received_date as event_date
	,COUNT(claim_id) val		
FROM
	tclaims_conso_all
WHERE
	claim_received_date between date_add(current_date,-90) and current_date
GROUP BY
	'Claim - Submitted volume'
	,claim_received_date
union
select
	'Claim - Paid volume' as type
	,claim_approved_date as event_date
	,COUNT(claim_id) val
from
	tclaims_conso_all
where
	claim_approved_date >= date_add(current_date,-90)
	and claim_status = 'A'
group by
	'Claim - Paid volume'
	,claim_approved_date                       
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>tat metrics</strong>

# COMMAND ----------

tat_metrics = spark.sql("""
SELECT	
	'NB Ops TAT (Tier 2)' as type
	,issue_date as event_date
	,SUM(tat_days) as tat_days
	,COUNT(pol_num) trxn_cnt	
FROM
	tpexnb02vn
WHERE
	type='ISSUE'
	AND	status_code not in ('N','R','T','6','8')
	AND	issue_date >= date_add(current_date,-90)
GROUP BY
	'NB Ops TAT (Tier 2)'
	,issue_date
union	
SELECT
	'Claims Ops TAT (Tier 2)' as type
	,clm.claim_approved_date as event_date
	,SUM(clm.tat_tier2) tat_days
	,COUNT(clm.claim_id) trxn_cnt		
FROM
	(SELECT
		a.*,
		ABS(DATEDIFF(a.payment_date_tier2,a.submit_date_tier2)) TAT_Tier2
	FROM
		(SELECT
			clm.claim_id
			,CASE				
				WHEN clm.direct_billing_ind='Y' 
    				THEN TO_DATE(clm.claim_received_date)
				WHEN clm.source_code NOT IN ('CWS','EZC') AND clm.completed_doc_date IS NULL 
    				THEN TO_DATE( clm.claim_received_date)
				WHEN clm.completed_doc_date > clm.claim_received_date 
    				THEN TO_DATE(clm.completed_doc_date)
				ELSE TO_DATE(clm.claim_received_date)
			END AS submit_date_tier2
			,CASE
				WHEN clm.direct_billing_ind='Y' THEN TO_DATE(clm.claim_received_date)
				WHEN clm.payout_type IN ('TF','PO') THEN DATE_ADD(clm.claim_approved_date,2)
				ELSE TO_DATE(clm.claim_approved_date)
			END AS payment_date_tier2
			,clm.claim_approved_date
		FROM
			tclaims_conso_all clm
		WHERE			
			clm.claim_status = 'A'
			AND	clm.claim_approved_date >= date_add(current_date,-90)
			AND	(YEAR(clm.completed_doc_date) <= YEAR(clm.claim_approved_date) OR clm.completed_doc_date IS NULL)
		) a
	) clm
GROUP BY
	'Claims Ops TAT (Tier 2)'
	,claim_approved_date;                  
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>nb submit dtls</strong>

# COMMAND ----------

nb_submit_dtls = spark.sql("""
select	
	submit_date
	,sbmt_typ
	,count(*) as val
from (
		select
			submit_date
			,case nb_user
				when 'EPOS' then 'ePOS'
				when 'SHOPEE' then 'Shopee'
				else
					case
						when (plan_code in ('CA360','CX360') and dist_chnl_cd = '24') then 'Cancer360'
						else 'Paper'
					end
			end sbmt_typ			
		from
			tpexnb02vn inner join
   			tpolicys on (tpexnb02vn.pol_num = tpolicys.pol_num)
		where
			type = 'SUBMIT'
			and submit_date >= date_add(current_date,-90)
	) rs1
group by
	submit_date
	,sbmt_typ                        
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>nb issue dtls</strong>

# COMMAND ----------

nb_issue_dtls = spark.sql("""
select	
	issue_date
	,sbmt_typ
	,count(*) as val
from (
		select
			issue_date
			,case nb_user
				when 'EPOS' then 'ePOS'
				when 'SHOPEE' then 'Shopee'
				else
					case
						when (plan_code in ('CA360','CX360') and dist_chnl_cd = '24') then 'Cancer360'
						else 'Paper'
					end
			end sbmt_typ			
		from
			tpexnb02vn inner join 
   			tpolicys on (tpexnb02vn.pol_num = tpolicys.pol_num)
		where
			type = 'ISSUE'
			and issue_date >= date_add(current_date,-90)
	) rs1
group by
	issue_date
	,sbmt_typ                       
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>nb dtls</strong>

# COMMAND ----------

nb_dtls = spark.sql("""
select	
	event_date
	,'SUBMIT' event_type
	,sbmt_typ
	,count(pol_num) as ptn_cnt
	,sum(ape) tot_ape
from (
		select
			pex.pol_num
			,submit_date as event_date
			,case nb_user
				when 'EPOS' then 'ePOS'
				when 'SHOPEE' then 'Shopee'
				when 'ECOMMERCE' then 'Ecommerce'
				else
					case
						when (plan_code in ('CA360','CX360') and dist_chnl_cd = '24') then 'Cancer360'
						else 'Paper'
					end
			end sbmt_typ
			,pol.mode_prem*12/pol.pmt_mode ape
		from
			tpexnb02vn pex inner join 
   			tpolicys pol on (pex.pol_num = pol.pol_num)
		where
			type = 'SUBMIT'
			and submit_date >= date_add(last_day(add_months(current_date,-4)),1)
	) rs1
group by
	event_date
	,sbmt_typ
union all
-- Digital submission for NB - detail - ISSUE
select	
	event_date
	,'ISSUE' as event_type
	,sbmt_typ
	,count(pol_num) as pol_cnt
	,sum(ape) tot_ape
from (
		select
			tpexnb02vn.pol_num
			,issue_date as event_date
			,case nb_user
				when 'EPOS' then 'ePOS'
				when 'SHOPEE' then 'Shopee'
				when 'ECOMMERCE' then 'Ecommerce'
				else
					case
						when (plan_code in ('CA360','CX360') and dist_chnl_cd = '24') then 'Cancer360'
						else 'Paper'
					end
			end sbmt_typ
			,mode_prem*12/pol.pmt_mode ape	
		from
			tpexnb02vn inner join 
   			tpolicys pol on (tpexnb02vn.pol_num = pol.pol_num)
		where
			type = 'ISSUE'
			and status_code in ('1','2','3','5')
			and issue_date >= date_add(last_day(add_months(current_date,-4)),1)
	) rs1
group by
	event_date
	,sbmt_typ                 
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Store results to parquet

# COMMAND ----------

digital_metrics.write.mode('overwrite').parquet(f'{lab_path}{output_dest}digital_metrics/')
volume_metrics.write.mode('overwrite').parquet(f'{lab_path}{output_dest}volume_metrics/')
tat_metrics.write.mode('overwrite').parquet(f'{lab_path}{output_dest}tat_metrics/')
nb_submit_dtls.write.mode('overwrite').parquet(f'{lab_path}{output_dest}nb_submit_dtls/')
nb_issue_dtls.write.mode('overwrite').parquet(f'{lab_path}{output_dest}nb_issue_dtls/')
nb_dtls.write.mode('overwrite').parquet(f'{lab_path}{output_dest}nb_dtls/')
