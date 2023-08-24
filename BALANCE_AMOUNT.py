# Databricks notebook source
# MAGIC %md
# MAGIC # BALANCE AMOUNT REPORT

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load libls, params and paths</strong>

# COMMAND ----------

from pyspark.sql.functions import *

spark.conf.set('partitionOverwriteMode', 'dynamic')

pro_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
misc_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_MISC_DB/'
bak_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_BAK_OS_DB/'
lab_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/dashboard/'

tacct_source = 'TACCT_EXTRACTS/'
tacct_hist_source = 'TACCT_EXTRACTS_HISTORIES/'
tacct_os_source = 'TACCT_EXTRACTS_OS/'
tpol_source = 'TPOLICYS/'
tcvg_source = 'TCOVERAGES/'
output_source = 'BALANCE_AMOUNT/'

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load working tables</strong>

# COMMAND ----------

tacctDF = spark.read.parquet(f'{pro_path}{tacct_source}')
tacct_histDF = spark.read.parquet(f'{misc_path}{tacct_hist_source}')
tacct_osDF = spark.read.parquet(f'{bak_path}{tacct_os_source}')
tpolDF = spark.read.parquet(f'{pro_path}{tpol_source}')
tcvgDF = spark.read.parquet(f'{pro_path}{tcvg_source}')

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Generate result</strong>

# COMMAND ----------

tacctDF.createOrReplaceTempView('tacct_extracts')
tacct_histDF.createOrReplaceTempView('tacct_extracts_histories')
tacct_osDF.createOrReplaceTempView('tacct_extracts_os')
tpolDF.createOrReplaceTempView('tpolicys')
tcvgDF.createOrReplaceTempView('tcoverages')

resultDF = spark.sql("""
SELECT
                  acc.acct_num, 
                  acc.pol_num,
                  pol.plan_code_base,
                  cvg.face_amt,
                  pol.pol_eff_dt,
                  cvg.prem_dur,
                  cvg.xpry_dt,
                  pol.pd_to_dt,
                  pol.last_pd_to_dt,
                  pol.last_avy_dt,
                  pol.pmt_mode,
                  pol.mode_prem,
                  pol.pol_stat_cd,
                  sum(if(acc.cr_or_dr='C',1,-1)*acc.acct_gen_amt) balance_amt
                  ,pol.wrk_area
                  ,acc.extract_typ
                  ,max(acc.trxn_dt) as susp_dt
FROM
                   (select * 
                    from tacct_extracts 
                    where trxn_dt <= last_day(add_months(current_date,-1))
        union all
                    select *
                    from tacct_extracts_histories
        union all       
                    select *      
                    from tacct_extracts_os                                                                    
                ) acc
    left join tpolicys pol ON pol.pol_num = acc.pol_num 
    left join tcoverages cvg ON pol.plan_code_base = cvg.plan_code and pol.vers_num_base = cvg.vers_num and pol.pol_num = cvg.pol_num
GROUP BY 
        acc.acct_num,
        acc.pol_num,
        pol.plan_code_base,
        cvg.face_amt,
        pol.pol_eff_dt,
        cvg.prem_dur,
        cvg.xpry_dt,
        pol.pd_to_dt,
        pol.last_pd_to_dt,
        pol.last_avy_dt,
        pol.pmt_mode,
        pol.mode_prem,
        pol.pol_stat_cd, 
        pol.wrk_area,
        acc.extract_typ
HAVING balance_amt <> 0                
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Store result to parquet</strong>

# COMMAND ----------

resultDF.write.mode('overwrite').parquet(f'{lab_path}{output_source}')
