# Databricks notebook source
# MAGIC %md
# MAGIC ### DAILY RECONCILIATION REPORT

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load libl, paths and params</strong>

# COMMAND ----------

from pyspark.sql.functions import *

pro_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
stg_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_POSSTG_DB/'
lab_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/dashboard/'

batch_headers_source = 'TCASH_BATCH_HEADERS/'
batch_details_source = 'TCASH_BATCH_DETAILS/'
outstand_details_source = 'TCASH_OUTSTAND_DETAILS/'
online_payments_source = 'TONLINE_PAYMENTS/'
applications_source = 'TAP_APPLICATIONS/'

output_dest = 'DAILY_RECONCILIATION/'

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load working tables </strong>

# COMMAND ----------

batch_headers = spark.read.parquet(f'{pro_path}{batch_headers_source}')
batch_details = spark.read.parquet(f'{pro_path}{batch_details_source}')
outstand_details = spark.read.parquet(f'{pro_path}{outstand_details_source}')
online_payments = spark.read.parquet(f'{pro_path}{online_payments_source}')
applications = spark.read.parquet(f'{stg_path}{applications_source}')

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Generate table</strong>

# COMMAND ----------

batch_headers.createOrReplaceTempView('tcash_batch_headers')
batch_details.createOrReplaceTempView('tcash_batch_details')
outstand_details.createOrReplaceTempView('tcash_outstand_details')
online_payments.createOrReplaceTempView('tonline_payments')
applications.createOrReplaceTempView('tap_applications')

resultDF = spark.sql("""
select
	cbh.trxn_dt trxn_dt
	,cbh.user_id user_id
	,cbd.rep_num rep_num
	,cbd.pol_num pol_num
	,cbd.prod_cat product_type
	,cbd.rep_typ rep_typ
	,cbd.prem_stat prem_stat
	,cbd.trxn_amt trxn_amt
	,cbd.reasn_code reasn_code
	,cod.bank_cd bank_cd
	,cod.bank_ref bank_ref
	,cod.service_type service_type
	,case
		when cbd.bank_cd = 'SHOPEEVC' then op.receipt_num_vendor
		else
			case
				when cbh.trxn_dt <= '2021-04-14' then op.receipt_num_vendor
				else cbd.rep_num 
			end
	end	trxn_id
	,case when cbd.bank_cd = 'SHOPEEVC' then 'Shopee' else 'SCB' end trxn_typ
from
	tcash_batch_headers cbh
	inner join tcash_batch_details cbd on (cbh.btch_num = cbd.btch_num and cbh.btch_role = cbd.btch_role and cbh.trxn_dt = cbd.trxn_dt)
	inner join tcash_outstand_details cod on (cbd.trxn_dt = cod.input_dt and cbd.pol_num = cod.pol_num and cbd.rep_num = cod.receipt_num)
	left join (
		select
			nvl(ap.pol_num,op.pol_num) pol_num
			,op.receipt_num_vendor
			,op.trxn_num
		from
			tonline_payments op
			left join tap_applications ap on (op.pol_num = ap.proposal_num)
		where
			op.status = 'A'
	) op on (cod.pol_num = op.pol_num and cod.trxn_num = op.trxn_num)	
where
	cbh.btch_role = 'C'
	and cbh.btch_typ = cod.pmt_type
	and cbd.bank_cd in ('SHOPEEVC','ECOMSACB','SCBHCMC')
	and cod.service_type in ('6','8')
	and cbh.trxn_dt >= date_add(current_date,-181)                  
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Write output to parquet</strong>

# COMMAND ----------

resultDF.write.mode('overwrite').parquet(f'{lab_path}{output_dest}')
