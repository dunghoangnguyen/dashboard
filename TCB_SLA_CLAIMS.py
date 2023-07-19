# Databricks notebook source
# MAGIC %md
# MAGIC # TCB SLA Claims

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load libl, tables and params</strong>

# COMMAND ----------

from pyspark.sql.functions import *

tclaims_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_REPORTS_DB/TCLAIMS_CONSO_ALL/'
claim_c_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_SFDC_EASYCLAIMS_DB/CLAIM__C/'
case_c_path =  'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_SFDC_EASYCLAIMS_DB/CASE/'

tclaims = spark.read.parquet(tclaims_path)
tclaim_c = spark.read.parquet(claim_c_path)
case_c = spark.read.parquet(case_c_path)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Intermediate tables</strong>

# COMMAND ----------

tclaims = tclaims.toDF(*[col.lower() for col in tclaims.columns])
tclaim_c = tclaim_c.toDF(*[col.lower() for col in tclaim_c.columns])
case_c = case_c.toDF(*[col.lower() for col in case_c.columns])

tclaims.createOrReplaceTempView("tclaims")
tclaim_c.createOrReplaceTempView("sf_clm")
case_c.createOrReplaceTempView("sf_case")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Finalize and store output</strong>

# COMMAND ----------

final = spark.sql("""
SELECT	t.policy_number as policy_number,		-- Policy Number
		t.claim_id as claim_id,
		t.claim_received_date as claim_received_date,-- Claim Received Date
		t.completed_doc_date as completed_doc_date,	-- Claim Re-open Date
		t.claim_approved_date as claim_approved_date,-- Claim Approved Date
		t.location_code as location_code,		-- Location Code
		t.claim_status as claim_status,		-- Claim Status		
		t.channel as channel,				-- Channel
		t.tat as tat,					-- TAT
        t.reporting_date as reporting_date
FROM	(
  		SELECT  clm.policy_number,		
				clm.claim_id,			
				clm.claim_received_date,	
				clm.completed_doc_date,	
				clm.claim_approved_date,	
				clm.location_code,		
				clm.claim_status,			
				CASE
					WHEN clm.direct_billing_ind = 'Y'
					THEN 'DirectBilling'
					WHEN clm.source_code = 'CWS'
					THEN 'CWS'           
					WHEN sf_case.origin = 'Easy Claims'
					THEN 'EasyClaims'
					WHEN sf_case.origin = 'Branch Submission'
					THEN 'BranchICE'
					WHEN sf_case.origin is null and clm.case_number is not null
					THEN 'EasyClaims'
					ELSE 'BranchCAS'
				END AS CHANNEL,				
				CASE
					WHEN clm.claim_status <> 'P'
					THEN
  						CASE
						  WHEN clm.completed_doc_date IS NULL
						  THEN DATEDIFF(clm.claim_approved_date, clm.claim_received_date)
						  WHEN clm.completed_doc_date > clm.claim_approved_date
  						  THEN DATEDIFF(clm.completed_doc_date, clm.claim_received_date)
  						  ELSE DATEDIFF(clm.claim_approved_date, clm.completed_doc_date)
  						END
					ELSE null  
				END AS TAT,					
				clm.reporting_date,
				ROW_NUMBER() OVER (PARTITION BY clm.claim_id, clm.policy_number ORDER BY clm.reporting_date DESC) RN
		FROM	tclaims clm
			LEFT JOIN
				sf_clm
			 ON	clm.claim_id = sf_clm.Name	
			LEFT JOIN
				sf_case
			 ON	sf_clm.case__c = sf_case.Id
		WHERE	clm.location_code LIKE 'TCB%'
			AND	clm.reporting_date > '2020-12-31'
		) t
WHERE	t.rn=1                 
""")

# COMMAND ----------

final.write.mode('overwrite').parquet('abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/dashboard/TCB/SLA_CLAIMS/')
