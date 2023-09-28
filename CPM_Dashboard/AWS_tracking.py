# Databricks notebook source
# MAGIC %run "/Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

path1 = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_ADOBE_PWS_DB/'

out_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/cpm/'

tbl1 = 'AWS_CPM_DAILY/'

path_list = [path1]
tbl_list = [tbl1]

list_df = {}

# COMMAND ----------

df_list1 = load_parquet_files(path_list, tbl_list)
list_df.update(df_list1)

# COMMAND ----------

generate_temp_view(list_df)

# COMMAND ----------

resultDF = spark.sql("""
SELECT	a.cpmid cpmid,
		CASE WHEN SUBSTR(a.cpmid,5,3)='UCM' THEN CONCAT_WS('-',a.cpmid,CONCAT(SUBSTR(b.hit_date,3,2),SUBSTR(b.hit_date,6,2))) 
			ELSE CONCAT_WS('-',a.cpmid,SUBSTR(a.cpmid,11,4))
		END cmpgn_btch_id,
                a.agentid agentid,
		a.page page,
		a.pageview pageview,
		a.comid comid,
		a.compaignid compaignid,
		a.tgt_id tgt_id,
		a.cust_id cust_id,
		a.loc_code loc_code,
		a.br_code br_code,
		a.rh_name rh_name,
		a.manager_name_0 manager_name_0,
		a.hit_date hit_date
FROM 	(SELECT	cpmid,
		agent_id agentid,
		CASE SUBSTR(page,55,32)
			WHEN 'customerportfoliomanagement' THEN 'CPM Landing Page'
			WHEN 'newcustomerpolicys' THEN 'Lead List Page'
                        WHEN 'customerpotentiallist' THEN 'Lead List Page' 
			WHEN 'customerpolicysdetail' THEN 'Lead Info Page'
		END page,
		pageviews pageview,
		com_id comid,
		compaign_id compaignid,
		tgt_id,
		cust_id,
		loc_cd loc_code,
		br_code,
		rh_name,
		manager_name_0,
		hit_date
FROM	aws_cpm_daily
lateral view explode (cmpgn_id_list) cmpgn_id_list as cpmid ) a
	INNER JOIN
		(SELECT	cpmid,
		 		MIN(hit_date) hit_date
		 FROM	aws_cpm_daily
		 lateral view explode (cmpgn_id_list) cmpgn_id_list as cpmid
		 GROUP BY cpmid) b
	 ON	a.cpmid=b.cpmid		 
WHERE	CAST(SUBSTR(a.cpmid,9,4) AS INT)>=YEAR(CURRENT_DATE)-1            
""")

# COMMAND ----------

resultDF.write.mode('overwrite').parquet(f'{out_path}aws_tracking/')
