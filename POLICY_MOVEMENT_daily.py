# Databricks notebook source
# MAGIC %md
# MAGIC #POLICY MOVEMENT REPORT (daily)

# COMMAND ----------

# MAGIC %run "/Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions"

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load libraries, params</strong>

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
from dateutil.relativedelta import relativedelta

lday = -1
lmth = -1
exclude_MI = 'MI007'

last_mthend = (datetime.today().replace(day=1) - relativedelta(days=1)).strftime('%Y-%m-%d')
print("last_mthend:", last_mthend)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load tables</strong>

# COMMAND ----------

# Declare paths
cdc_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/CDC/VN_PUBLISHED_CAS_DB_SYNCSORT/'
cas_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/'

# Declare table names
tblSrc1 = 'TPOLICYS/'

tpol_mthend = spark.read.format("parquet").load(f'{cas_path}{tblSrc1}')
tpol_mthend = tpol_mthend.toDF(*[col.lower() for col in tpol_mthend.columns])

tpol_daily = spark.read.format('delta').load(f'{cdc_path}{tblSrc1}delta/')
tpol_daily = tpol_daily.toDF(*[col.lower() for col in tpol_daily.columns])

tpol_mthend = tpol_mthend.filter(col('image_date') == last_mthend)\
    .select(
        'pol_num',
        'plan_code_base',
        'pol_stat_cd',
        'pol_iss_dt',
        'image_date'
    ).distinct()

tpol_daily = tpol_daily.select(
    'pol_num',
    'plan_code_base',
    'pol_stat_cd',
    'pol_iss_dt'
).distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Finalize table</strong>

# COMMAND ----------

print('tpol_daily:', tpol_daily.count(), ', tpol_mthend:', tpol_mthend.count())
tpol_daily.createOrReplaceTempView('tpol_daily')
tpol_mthend.createOrReplaceTempView('tpol_mthend')

# COMMAND ----------

pol_move_df = spark.sql(f"""
SELECT	t.TYPE,
		t.SUB_TYPE,
		t.COUNT_NUM,
		LAST_DAY(DATE_ADD(CURRENT_DATE,{lday})) IMAGE_DATE
FROM	(
select	'00.Beginning balance (Non MI)' type,
		'' sub_type,
		count(pol_num) count_num
from	tpolicys
where	image_date = last_day(add_months(current_date,{llst_mth}))
	and	plan_code_base <> '{exclude_MI}'
	and	pol_stat_cd in ('1','2','3','4','5','7','9')
union
select	'01.New Business Issuance' type,
		'' sub_type,
		count(cmo.pol_num) count_num
from
	  	(select	pol_num,
		 		pol_stat_cd,
		 		pol_iss_dt,
		 		image_date
	  	from	tpolicys
	  	where	image_date = last_day(add_months(current_date,{lst_mth}))
		 	and	plan_code_base <> '{exclude_MI}'
		  	--and	pol_stat_cd in ('6','8')		                 -- Select policies that are pending on the reporting date
		) cmo
left join
		(select	pol_num,
		 		pol_stat_cd,
		 		image_date
		 from	tpolicys
		 where	image_date = last_day(add_months(current_date,{llst_mth}))
		 	and	plan_code_base <> ('{exclude_MI}')
		 	--and	pol_stat_cd in ('1','2','3','4','5','7','9')    -- Select policies with "inforce" status the month before
		) lmo
	on	cmo.pol_num = lmo.pol_num
where	cmo.pol_stat_cd in ('1','2','3','4','5','7','9')
	and	(lmo.pol_num is null or
		lmo.pol_stat_cd in ('6','8'))
union
select	'02.Reinstatement' type,
		'' sub_type,
		count(cmo.pol_num) count_num
from
	  	(select	pol_num,
		 		pol_stat_cd,
		 		pol_iss_dt,
		 		image_date
	  	from	tpolicys
	  	where	image_date = last_day(add_months(current_date,{lst_mth}))
		 	and	plan_code_base <> '{exclude_MI}'
		  	and	pol_stat_cd in ('1','2','3','4','5','7','9')		     -- Select policies that are inforce on the reporting date
		) cmo
inner join
		(select	pol_num,
		 		pol_stat_cd,
		 		image_date
		 from	tpolicys
		 where	image_date = last_day(add_months(current_date,{llst_mth}))
		 	and	plan_code_base <> '{exclude_MI}'
		 	and	pol_stat_cd not in ('1','2','3','4','5','6','7','8','9')    -- Select policies with "not pending/not inforce" status the month before
		) lmo
	on	cmo.pol_num = lmo.pol_num
union	
select	'03.Unissue' type,
		'' sub_type,
		count(cmo.pol_num) count_num
from
	  	(select	pol_num,
		 		pol_stat_cd,
		 		pol_iss_dt,
		 		image_date
	  	from	tpolicys
	  	where	image_date = last_day(add_months(current_date,{lst_mth}))
		 	and	plan_code_base <> '{exclude_MI}'
		  	and	pol_stat_cd in ('6','8')							-- Select policies that are inforce on the reporting date
		) cmo
inner join
		(select	pol_num,
		 		pol_stat_cd,
		 		image_date
		 from	tpolicys
		 where	image_date = last_day(add_months(current_date,{llst_mth}))
		 	and	plan_code_base <> '{exclude_MI}'
		 	and	pol_stat_cd in ('1','2','3','4','5','7','9')		-- Select policies with pending status the month before
		) lmo
	on	cmo.pol_num = lmo.pol_num
union    
select	'04.Lapsed' type,
		issue_period sub_type,
		count(cmo.pol_num) count_num
from
	  	(select	pol_num,
		 		pol_stat_cd,
		 		pol_iss_dt,
		 		case 
		 			when datediff(last_day(add_months(current_date,{llst_mth})), pol_iss_dt)/365.25 <= 1    -- policies issued within 1 year
		 			then '1.Within 1 year'
		 			when datediff(last_day(add_months(current_date,{llst_mth})), pol_iss_dt)/365.25 > 1 and -- policies issued within 2 years
		 				 datediff(last_day(add_months(current_date,{llst_mth})), pol_iss_dt)/365.25 <= 2
		 			then '2.Within 2 years'
		 			else '3.>2 years'                                                               -- policies issued more than 2 years
		 		end as issue_period,
		 		image_date
	  	from	tpolicys
	  	where	image_date = last_day(add_months(current_date,{lst_mth}))
		 	and	plan_code_base <> '{exclude_MI}'
		  	and	pol_stat_cd = 'B'		                        -- Select policies that are lapsed on the reporting date
		) cmo
inner join
		(select	pol_num,
		 		pol_stat_cd,
		 		image_date
		 from	tpolicys
		 where	image_date = last_day(add_months(current_date,{llst_mth}))
		 	and	plan_code_base <> '{exclude_MI}'
		 	and	pol_stat_cd in ('1','2','3','4','5','7','9')    -- Select policies with "inforce" status the month before
		) lmo
	on	cmo.pol_num = lmo.pol_num
group by
		issue_period
union
select	'05.Surrender' type,
		'' sub_type,
		count(cmo.pol_num) count_num
from
	  	(select	pol_num,
		 		pol_stat_cd,
		 		pol_iss_dt,
		 		image_date
	  	from	tpolicys
	  	where	image_date = last_day(add_months(current_date,{lst_mth}))
		 	and	plan_code_base <> '{exclude_MI}'
		  	and	pol_stat_cd in ('E')							-- Select policies that are inforce on the reporting date
		) cmo
inner join
		(select	pol_num,
		 		pol_stat_cd,
		 		image_date
		 from	tpolicys
		 where	image_date = last_day(add_months(current_date,{llst_mth}))
		 	and	plan_code_base <> '{exclude_MI}'
		 	and	pol_stat_cd in ('1','2','3','4','5','7','9')		-- Select policies with pending status the month before
		) lmo
	on	cmo.pol_num = lmo.pol_num    
union
select	'06.Maturity' type,
		'' sub_type,
		count(cmo.pol_num) count_num
from
	  	(select	pol_num,
		 		pol_stat_cd,
		 		pol_iss_dt,
		 		image_date
	  	from	tpolicys
	  	where	image_date = last_day(add_months(current_date,{lst_mth}))
		 	and	plan_code_base <> '{exclude_MI}'
		  	and	pol_stat_cd in ('F','H')							-- Select policies that are inforce on the reporting date
		) cmo
inner join
		(select	pol_num,
		 		pol_stat_cd,
		 		image_date
		 from	tpolicys
		 where	image_date = last_day(add_months(current_date,{llst_mth}))
		 	and	plan_code_base <> '{exclude_MI}'
		 	and	pol_stat_cd in ('1','2','3','4','5','7','9')		-- Select policies with pending status the month before
		) lmo
	on	cmo.pol_num = lmo.pol_num   
union
select	'07.Death-Major Disease-TPD' type,
		'' sub_type,
		count(cmo.pol_num) count_num
from
	  	(select	pol_num,
		 		pol_stat_cd,
		 		pol_iss_dt,
		 		image_date
	  	from	tpolicys
	  	where	image_date = last_day(add_months(current_date,{lst_mth}))
		 	and	plan_code_base <> '{exclude_MI}'
		  	and	pol_stat_cd in ('D','M','T')						-- Select policies that are inforce on the reporting date
		) cmo
inner join
		(select	pol_num,
		 		pol_stat_cd,
		 		image_date
		 from	tpolicys
		 where	image_date = last_day(add_months(current_date,{llst_mth}))
		 	and	plan_code_base <> '{exclude_MI}'
		 	and	pol_stat_cd in ('1','2','3','4','5','7','9')		-- Select policies with pending status the month before
		) lmo
	on	cmo.pol_num = lmo.pol_num
union
select	'08.Nottaken' type,
		'' sub_type,
		count(cmo.pol_num) count_num
from
	  	(select	pol_num,
		 		pol_stat_cd,
		 		pol_iss_dt,
		 		image_date
	  	from	tpolicys
	  	where	image_date = last_day(add_months(current_date,{lst_mth}))
		 	and	plan_code_base <> '{exclude_MI}'
		  	and	pol_stat_cd in ('A')								-- Select policies that are inforce on the reporting date
		) cmo
inner join
		(select	pol_num,
		 		pol_stat_cd,
		 		image_date
		 from	tpolicys
		 where	image_date = last_day(add_months(current_date,{llst_mth}))
		 	and	plan_code_base <> ('{exclude_MI}')
		 	and	pol_stat_cd in ('1','2','3','4','5','7','9')		-- Select policies with pending status the month before
		) lmo
	on	cmo.pol_num = lmo.pol_num
union
select	'09.Other Terminated Status' type,
		'' sub_type,
		count(cmo.pol_num) count_num
from
	  	(select	pol_num,
		 		pol_stat_cd,
		 		pol_iss_dt,
		 		image_date
	  	from	tpolicys
	  	where	image_date = last_day(add_months(current_date,{lst_mth}))
		 	and	plan_code_base <> '{exclude_MI}'
		  	and	pol_stat_cd in ('C','L','N','R','X')				-- Select policies that are inforce on the reporting date
		) cmo
inner join
		(select	pol_num,
		 		pol_stat_cd,
		 		image_date
		 from	tpolicys
		 where	image_date = last_day(add_months(current_date,{llst_mth}))
		 	and	plan_code_base <> '{exclude_MI}'
		 	and	pol_stat_cd in ('1','2','3','4','5','7','9')		-- Select policies with pending status the month before
		) lmo
	on	cmo.pol_num = lmo.pol_num    
union
select	'10.Ending balance (Non MI)' type,
		'' sub_type,
		count(pol_num) count_num
from	tpolicys
where	image_date = last_day(add_months(current_date,{lst_mth}))
	and	plan_code_base <> '{exclude_MI}'
	and	pol_stat_cd in ('1','2','3','4','5','7','9')
union
select	'11.Ending balance (MI)' type,
		'' sub_type,
		count(pol_num) count_num
from	tpolicys
where	image_date = last_day(add_months(current_date,{lst_mth}))
	and	plan_code_base = '{exclude_MI}'
	and	pol_stat_cd in ('1','2','3','4','5','7','9')    
) t
""")
pol_move_df = pol_move_df.toDF(*[col.lower() for col in pol_move_df.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Store result</strong>

# COMMAND ----------

spark = SparkSession.builder.appName("POLICY_MOVEMENT").getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

pol_move_df.write.mode("overwrite").partitionBy("image_date").parquet("abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/dashboard/POLICY_MOVEMENT")
