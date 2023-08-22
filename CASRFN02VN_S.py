# Databricks notebook source
# MAGIC %md
# MAGIC # CASRFN02VN_S

# COMMAND ----------

# MAGIC %md
# MAGIC <strong> Load path to tables </strong>

# COMMAND ----------

from pyspark.sql.functions import *

tacct_extracts_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TACCT_EXTRACTS/'

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load and intermediate tables</strong>

# COMMAND ----------

tacct_extracts = spark.read.format("parquet").load(tacct_extracts_path)

# Standardize column heads
tacct_extracts = tacct_extracts.toDF(*[col.lower() for col in tacct_extracts.columns])

tacct_extracts = tacct_extracts \
    .filter((col("extract_typ").isin(['N', 'X', 'A'])) & 
            (year(col("trxn_dt")) >= year(date_add(last_day(add_months(date_add(current_date(), -7), -1)), 1))))
 
#tacct_extracts_dedup = tacct_extracts.filter(
#    (col("trxn_dt").between("2023-06-01", "2023-06-30")) &
#    (col("acct_num") == "1111120")
#).dropDuplicates()

#tacct_extracts_dedup.show()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Finalize table</strong>

# COMMAND ----------

tacct_extracts_dedup.createOrReplaceTempView("tacct_extracts")

final = spark.sql("""
SELECT	substr(pol_num,1,3) POL_PREFIX,																
		sum(case when CR_OR_DR='D' then ACCT_GEN_AMT*1000 else 0 end) DEBIT,  								
		sum(case when CR_OR_DR='C' then ACCT_GEN_AMT*1000 else 0 end) CREDIT,								
		sum(case when CR_OR_DR='D' then ACCT_GEN_AMT*1000 else 0 end) - sum(case when CR_OR_DR='C' then ACCT_GEN_AMT*1000 else 0 end) NET,
		IF(CRCY_CODE='90','CNY',CRCY_CODE) CRCY_CODE,
		ACCT_NUM,
        LOB, 								
		WRK_AREA, 
		acct_mne_cd,
	 	trxn_dt,
        extract_typ,
		LAST_DAY(TRXN_DT) AS reporting_date
FROM 	tacct_extracts
GROUP BY acct_num, acct_mne_cd, lob, crcy_code,									
		substr(pol_num,1,3), wrk_area, trxn_dt, extract_typ								
ORDER BY acct_num, acct_mne_cd, lob, crcy_code,									
		pol_prefix, wrk_area, trxn_dt, extract_typ
""")


# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Store result</strong>

# COMMAND ----------

final.write.mode("overwrite").parquet("abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/dashboard/CASRFN02VN_S")

# COMMAND ----------


