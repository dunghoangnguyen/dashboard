# Databricks notebook source
# MAGIC %md
# MAGIC # VOC INSURANCE (Quarterly)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load defined functions</strong>

# COMMAND ----------

# MAGIC %run "/Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions"

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load libls, params and paths</strong>

# COMMAND ----------

from pyspark.sql.functions import col, count, countDistinct, sum
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta

x = 0 # Change to number of months ago (0: last month-end, 1: last last month-end, ...)
today = datetime.now()
first_day_of_current_month = today.replace(day=1)
current_month = first_day_of_current_month

for i in range(x):
    first_day_of_previous_month = current_month - timedelta(days=1)
    first_day_of_previous_month = first_day_of_previous_month.replace(day=1)
    current_month = first_day_of_previous_month

last_day_of_x_months_ago = current_month - timedelta(days=1)
rpt_dt = last_day_of_x_months_ago.strftime('%Y-%m-%d')
rpt_mth = rpt_dt[2:4]+rpt_dt[5:7]
print(rpt_dt, rpt_mth)

# List of ABFSS paths
in_path = '/mnt/lab/vn/project/scratch/ORC_ACID_to_Parquet_Conversion/apps/hive/warehouse/vn_processing_datamart_temp_db.db/'
lab_path = '/mnt/lab/vn/project/cpm/ADEC/WorkingData/'
cpm_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_CAMPAIGN_DB/'
out_path = '/mnt/lab/vn/project/dashboard/VOC_INSURANCE/'

# List of tables
tbl_src1 = 'customers_table_mthly/'
tbl_src2 = 'product_table_summary_'+rpt_mth+'/'
tbl_src3 = 'LOC_CODE_MAPPING/'

#daily_paths = [in_path,lab_path,cpm_path]
#daily_files = [tbl_src1,tbl_src2,tbl_src3]

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load working tables</strong>

# COMMAND ----------

df1 = spark.read.parquet(f'{in_path}{tbl_src1}reporting_date={rpt_dt}/')
df2 = spark.read.parquet(f'{lab_path}{tbl_src2}')
df3 = spark.read.parquet(f'{cpm_path}{tbl_src3}')
#df = df1.filter(col('reporting_date')==rpt_dt)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate temp views and result

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Generate temp view</strong>

# COMMAND ----------

df1.createOrReplaceTempView(f'customer_table_{rpt_mth}')
df2.createOrReplaceTempView(f'product_table_summary_{rpt_mth}')
df3.createOrReplaceTempView('loc_code_mapping')

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Generate result</strong>

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>voc_cust_size</strong>

# COMMAND ----------

sql_string = """
select
	cast(count(po_num) as double) val
	,to_date('{rpt_dt}') reporting_date
from
	customer_table_{rpt_mth}
"""
voc_cust_size = sql_to_df(sql_string, 1, spark)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>voc_gender</strong>

# COMMAND ----------

sql_string = """
select 
	gender
	,cast(count(po_num) as float) val
	,to_date('{rpt_dt}') reporting_date
from
    customer_table_{rpt_mth}
group by
	gender
having
	gender is not null
"""
voc_gender = sql_to_df(sql_string, 1, spark)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>voc_age_band</strong>

# COMMAND ----------

sql_string = """
select
	age_band
	,cast(count(po_num) as float) val
	,to_date('{rpt_dt}') reporting_date
from
	(
		select
			po_num
			,case
				when age_curr between 18 and 24 then '1.18-24'
				when age_curr between 25 and 34 then '2.25-34'
				when age_curr between 35 and 44	then '3.35-44'
				when age_curr between 45 and 54 then '4.45-54'
				when age_curr between 55 and 64	then '5.55-64'		
			end age_band
		from
			customer_table_{rpt_mth}
	) sum
group by
	age_band
having  age_band is not null
"""
voc_age_band = sql_to_df(sql_string, 1, spark)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>voc_ape</strong>

# COMMAND ----------

sql_string = """
select
	ape_band
	,count(po_num) val
	,to_date('{rpt_dt}') reporting_date
from
		(select	po_num,
				case
					when tot_ape < 10000
					then '1.less than 10'
					when tot_ape >= 10000 and tot_ape < 20000
					then '2.10 - less than 20'
					when tot_ape >= 20000 and tot_ape < 40000
					then '3.20 - less than 40'
					when tot_ape >= 40000 and tot_ape < 65000
					then '4.40 - less than 65'                    
					when tot_ape >= 65000 and tot_ape < 80000
					then '5.65 - less than 80'
					when tot_ape >= 80000 and tot_ape < 150000
					then '6.80 - less than 150'
					when tot_ape >= 150000 and tot_ape < 250000
					then '7.150 - less than 250'                    
					when tot_ape >= 250000
					then '8.250 and above'      	
				end ape_band
		from	customer_table_{rpt_mth}
  	) sum
group by
	ape_band
having
	ape_band is not null
"""
voc_ape = sql_to_df(sql_string, 1, spark)
#voc_ape.display()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>temp_tenure_and_recency</strong>

# COMMAND ----------

sql_string = """
select
	po_num
	,case
		when tenure < 1 then '1.less than 1 year'
		when tenure >= 1 and tenure < 2 then '2.1 year to less than 2 years'
		when tenure >= 2 and tenure < 3 then '3.2 years to less than 3 years'
		when tenure >= 3 and tenure < 4 then '4.3 years to less than 4 years'                    
		when tenure >= 4 and tenure < 5 then '5.4 years to less than 5 years'                    
		when tenure >= 5 and tenure < 6 then '6.5 years to less than 6 years'                    
		when tenure >= 6 and tenure < 10 then '7.6 years to less than 10 years'                    
		when tenure >= 10 then '8.10 years or more'    	
	end tenure_band
	,case
		when recency < 1 then '1.less than 1 year'
		when recency >= 1 and recency < 2 then '2.1 year to less than 2 years'
		when recency >= 2 and recency < 3 then '3.2 years to less than 3 years'
		when recency >= 3 and recency < 4 then '4.3 years to less than 4 years'                    
		when recency >= 4 and recency < 5 then '5.4 years to less than 5 years'
		when recency >= 5 and recency < 6 then '6.5 years to less than 6 years'
		when recency >= 6 and recency < 7 then '7.6 years to less than 7 years'
		when recency >= 7 and recency < 10 then '8.7 years to less than 10 years'
		else '9.10 years or above'    	
	end recency_band
from
	(
		select
			po_num
			,floor(months_between(last_day(add_months('{rpt_dt}',-1)), pol_iss_dt) / 12) recency
			,tenure
		from
			customer_table_{rpt_mth}
	) rs
"""
t_tenure_recency = sql_to_df(sql_string, 1, spark)
t_tenure_recency.createOrReplaceTempView('t_tenure_recency')

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>voc_tenure</strong>

# COMMAND ----------

sql_string = """
select
	tenure_band
	,cast(count(distinct po_num) as float) val
	,to_date('{rpt_dt}') reporting_date
from
	t_tenure_recency
group by
	tenure_band
having
	tenure_band is not null
order by
	tenure_band
"""

voc_tenure = sql_to_df(sql_string, 1, spark)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>voc_rencency</strong>

# COMMAND ----------

sql_string = """
select
	recency_band recency
	,cast(count(distinct po_num) as float) val
	,to_date('{rpt_dt}') reporting_date
from
	t_tenure_recency
group by
	recency_band
having
	recency_band is not null 
order by
	recency
"""
voc_rencency = sql_to_df(sql_string, 1, spark)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>voc_tenure_vs_recency</strong>

# COMMAND ----------

sql_string = """
select
	tenure_band
	,recency_band
	,cast(count(distinct po_num) as float) val
	,to_date('{rpt_dt}') reporting_date
from
	t_tenure_recency
where
	tenure_band is not null
	and recency_band is not null
group by
	tenure_band
	,recency_band
order by
	tenure_band
	,recency_band
"""

voc_tenure_vs_recency = sql_to_df(sql_string, 1, spark)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>voc_needs</strong>

# COMMAND ----------

sql_string = """
select
	need_typ
	,cnt as val
	,to_date('{rpt_dt}') reporting_date
from
	(
		select 'accident' as need_typ, cast(count(po_num) as float) cnt from product_table_summary_{rpt_mth} where need_acc > 0
		union
		select 'critical_illness' as need_typ, cast(count(po_num) as float) cnt from product_table_summary_{rpt_mth} where need_ci > 0
		union
		select 'disability' as need_typ, cast(count(po_num) as float) cnt from product_table_summary_{rpt_mth} where need_dis > 0
		union
		select 'group_life_health' as need_typ, cast(count(po_num) as float) cnt from product_table_summary_{rpt_mth} where need_glh > 0
		union
		select 'investment_linked' as need_typ, cast(count(po_num) as float) cnt from product_table_summary_{rpt_mth} where need_inv > 0
		union
		select 'life_protection' as need_typ, cast(count(po_num) as float) cnt from product_table_summary_{rpt_mth} where need_lp > 0
		union
		select 'long_term_savings' as need_typ, cast(count(po_num) as float) cnt from product_table_summary_{rpt_mth} where need_lts > 0
		union
		select 'medical' as need_typ, cast(count(po_num) as float) cnt from product_table_summary_{rpt_mth} where need_med > 0
	) rs
"""

voc_needs = sql_to_df(sql_string, 1, spark)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>voc_channel</strong>

# COMMAND ----------

sql_string = """
select
	channel
	,cast(count(*) as float) val
	,to_date('{rpt_dt}') reporting_date
from
	customer_table_{rpt_mth}
group by
	channel
"""

voc_channel = sql_to_df(sql_string, 1, spark)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>voc_cummunication</strong>

# COMMAND ----------

sql_string = """
select	
	cast(count(case when email_contact='Y' then po_num end) as float) with_email,
	cast(count(case when email_contact='N' then po_num end) as float) without_email,
	cast(count(case when mobile_contact='Y' then po_num end) as float) with_mbl_phone,
	cast(count(case when mobile_contact='N' then po_num end) as float) without_mbl_phone
	,to_date('{rpt_dt}') reporting_date
from
	customer_table_{rpt_mth}
"""

voc_cummunication = sql_to_df(sql_string, 1, spark)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>voc_penetration</strong>

# COMMAND ----------

sql_string = """
select
	a.city
	,cast(count(a.po_num) as float) val
	,to_date('{rpt_dt}') reporting_date
from 
	customer_table_{rpt_mth} a
	left join loc_code_mapping b on a.loc_code_agt=b.loc_code
group by
	a.city
having
	a.city is not null
"""

voc_penetration = sql_to_df(sql_string, 1, spark)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Store result to parquet</strong>

# COMMAND ----------

spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

voc_cust_size.write.mode('overwrite').partitionBy('reporting_date').parquet(f'{out_path}voc_cust_size/')
voc_gender.write.mode('overwrite').partitionBy('reporting_date').parquet(f'{out_path}voc_gender/')
voc_age_band.write.mode('overwrite').partitionBy('reporting_date').parquet(f'{out_path}voc_age_band/')
voc_ape.write.mode('overwrite').partitionBy('reporting_date').parquet(f'{out_path}voc_ape/')
voc_tenure.write.mode('overwrite').partitionBy('reporting_date').parquet(f'{out_path}voc_tenure/')
voc_rencency.write.mode('overwrite').partitionBy('reporting_date').parquet(f'{out_path}voc_rencency/')
voc_tenure_vs_recency.write.mode('overwrite').partitionBy('reporting_date').parquet(f'{out_path}voc_tenure_vs_recency/')
voc_needs.write.mode('overwrite').partitionBy('reporting_date').parquet(f'{out_path}voc_needs/')
voc_channel.write.mode('overwrite').partitionBy('reporting_date').parquet(f'{out_path}voc_channel/')
voc_cummunication.write.mode('overwrite').partitionBy('reporting_date').parquet(f'{out_path}voc_cummunication/')
voc_penetration.write.mode('overwrite').partitionBy('reporting_date').parquet(f'{out_path}voc_penetration/')
