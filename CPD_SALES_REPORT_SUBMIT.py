# Databricks notebook source
# MAGIC %md
# MAGIC # THIS SECTION GIVES AN OVERVIEW ABOUT THE FUNCTIONS OF THIS CODE

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load paths and param</strong>

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DateType
import pandas as pd
from datetime import datetime, timedelta
import calendar

tams_agents_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_AMS_DB/TAMS_AGENTS/'
tcomm_trailers_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TCOMMISSION_TRAILERS/'
tcoverages_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TCOVERAGES/'
tpolicys_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TPOLICYS/'
tplans_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TPLANS/'
ttrxn_histories_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TTRXN_HISTORIES/'
tams_agt_rpt_rels_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_AMS_DB/TAMS_AGT_RPT_RELS/'
twrk_fyp_ryp_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TWRK_FYP_RYP/'
tams_crr_txn_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_AMS_DB/TAMS_CRR_TRANSACTIONS/'
tams_crr_txn_bk_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_AMS_BAK_DB/TAMS_CRR_TRANSACTIONS_BK/'
tcsh_btch_dtl_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TCASH_BATCH_DETAILS/'
tpt_chg_ctl_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TPT_CHG_CTL/'
tcsh_os_dtl_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TCASH_OUTSTAND_DETAILS/'

# COMMAND ----------

# MAGIC %md
# MAGIC <strong> Load tables and convert to DF</strong>

# COMMAND ----------

tams_agents = spark.read.format("parquet").load(tams_agents_path)
tcomm_trailers = spark.read.format("parquet").load(tcomm_trailers_path)
tcoverages = spark.read.format("parquet").load(tcoverages_path)
tpolicys = spark.read.format("parquet").load(tpolicys_path)
tplans = spark.read.format("parquet").load(tplans_path)
ttrxn_histories = spark.read.format("parquet").load(ttrxn_histories_path)
tams_agt_rpt_rels = spark.read.format("parquet").load(tams_agt_rpt_rels_path)
twrk_fyp_ryp = spark.read.format("parquet").load(twrk_fyp_ryp_path)
tams_crr_txn = spark.read.format("parquet").load(tams_crr_txn_path)
tams_crr_txn_bk = spark.read.format("parquet").load(tams_crr_txn_bk_path)
tcsh_btch_dtl = spark.read.format("parquet").load(tcsh_btch_dtl_path)
tpt_chg_ctl = spark.read.format("parquet").load(tpt_chg_ctl_path)
tcsh_os_dtl = spark.read.format("parquet").load(tcsh_os_dtl_path)

tams_agents = tams_agents.toDF(*[col.lower() for col in tams_agents.columns])
tcomm_trailers = tcomm_trailers.toDF(*[col.lower() for col in tcomm_trailers.columns])
tcoverages = tcoverages.toDF(*[col.lower() for col in tcoverages.columns])
tpolicys = tpolicys.toDF(*[col.lower() for col in tpolicys.columns])
tplans = tplans.toDF(*[col.lower() for col in tplans.columns])
ttrxn_histories = ttrxn_histories.toDF(*[col.lower() for col in ttrxn_histories.columns])
tams_agt_rpt_rels = tams_agt_rpt_rels.toDF(*[col.lower() for col in tams_agt_rpt_rels.columns])
twrk_fyp_ryp = twrk_fyp_ryp.toDF(*[col.lower() for col in twrk_fyp_ryp.columns])
tams_crr_txn = tams_crr_txn.toDF(*[col.lower() for col in tams_crr_txn.columns])
tams_crr_txn_bk = tams_crr_txn_bk.toDF(*[col.lower() for col in tams_crr_txn_bk.columns])
tcsh_btch_dtl = tcsh_btch_dtl.toDF(*[col.lower() for col in tcsh_btch_dtl.columns])
tpt_chg_ctl = tpt_chg_ctl.toDF(*[col.lower() for col in tpt_chg_ctl.columns])
tcsh_os_dtl = tcsh_os_dtl.toDF(*[col.lower() for col in tcsh_os_dtl.columns])


# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Intermediate tables</strong>

# COMMAND ----------

ttrxn_df = ttrxn_histories.filter((ttrxn_histories.trxn_cd == "NEWBUS") & 
                                  (ttrxn_histories.reasn_code == "111")) \
    .groupBy("pol_num") \
    .agg(min("trxn_dt").alias("cnfrm_acpt_dt"))

wrk_df = twrk_fyp_ryp.groupBy("pol_num", "plan_code", "typ", "agt_code") \
    .agg(sum(
        when(twrk_fyp_ryp.typ == "FYP", twrk_fyp_ryp.amt)
        .when(twrk_fyp_ryp.typ.isin(["FYP-TOPUP", "SP"]), 0.06 * twrk_fyp_ryp.amt)
        .otherwise(0)
    ).alias("fyp_amt"))

crr_df = tams_crr_txn.filter((tams_crr_txn.crr_typ == "COM") & 
                             (tams_crr_txn.reasn_cd == "115")) \
    .groupBy("pol_num", "plan_cd", "agt_cd") \
    .agg(sum(coalesce(tams_crr_txn.crr_valu, lit(0))).alias("amt"))

crr_bk_df = tams_crr_txn_bk.filter((tams_crr_txn_bk.crr_typ == "COM") & 
                             (tams_crr_txn_bk.reasn_cd == "115")) \
    .groupBy("pol_num", "plan_cd", "agt_cd") \
    .agg(sum(coalesce(tams_crr_txn_bk.crr_valu, lit(0))).alias("amt"))

tbd_df = tcsh_btch_dtl.filter((tcsh_btch_dtl.btch_role == "C") & 
                              (tcsh_btch_dtl.btch_dtl_stat_cd == "A")) \
    .select(tcsh_btch_dtl.pol_num, 
            tcsh_btch_dtl.trxn_dt, 
            lit("1").alias("rowtype"))

tc_df = tpt_chg_ctl.filter(tpt_chg_ctl.dept_typ == "NB") \
    .select(tpt_chg_ctl.pol_num,
            tpt_chg_ctl.sbmt_date.alias("trxn_dt"),
            lit("2").alias("rowtype"))

tod_df = tcsh_os_dtl.select(
    tcsh_os_dtl.pol_num,
    tcsh_os_dtl.input_dt.alias("trxn_dt"),
    lit("3").alias("rowtype")
)

groupdt_df = tbd_df.unionByName(tc_df).unionByName(tod_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong> Generate final raw table </strong>

# COMMAND ----------

tams_agents.createOrReplaceTempView("tams_agents")
tcomm_trailers.createOrReplaceTempView("tcomm_trailers")
tcoverages.createOrReplaceTempView("tcoverages")
tpolicys.createOrReplaceTempView("tpolicys")
tplans.createOrReplaceTempView("tplans")
tams_agt_rpt_rels.createOrReplaceTempView("tams_agt_rpt_rels")
ttrxn_df.createOrReplaceTempView("ttrxn_df")
wrk_df.createOrReplaceTempView("wrk_df")
crr_df.createOrReplaceTempView("crr_df")
crr_bk_df.createOrReplaceTempView("crr_bk_df")
groupdt_df.createOrReplaceTempView("groupdt_df")

final_df = spark.sql("""
    SELECT distinct 
            agt.agt_code,
            agt.agt_join_dt agt_join_dt,
            cov.pol_num,
            pol.pol_stat_cd,
            wrk_df.fyp_amt,
            case when date_sub(CURRENT_DATE,1) > cov.cvg_eff_dt  and date_sub(CURRENT_DATE,1) < add_months(cov.cvg_eff_dt, 12) then nvl(crr_df.amt,0)+nvl(crr_bk_df.amt,0)
            else wrk_df.fyp_amt
            end as fyc_amt,
            (cov.cvg_prem/pol.pmt_mode * 12) ape,
            case WHEN groupdt_df.rowtype in ('1','2','3') then groupdt_df.trxn_dt else 'NULL' END as subms_dt,
            frst_iss_dt IssueDate,
            ttrxn_df.cnfrm_acpt_dt,
            cov.cvg_typ,
            pol.plan_code_base plan_cd_base,
            cov.plan_code plan_code,
            p.plan_lng_desc,
            agt.loc_code loc_cd,
            rel.agt_cd dr_mgr_cd,
            trl.pct_splt,
            CASE WHEN groupdt_df.rowtype in ('1','2') then groupdt_df.trxn_dt else 'NULL' END as sbmt_at_cashier,
            pol.wa_cd_1, pol.wa_cd_2
    FROM    tams_agents agt left join 
            tcomm_trailers trl on agt.agt_code = trl.agt_code inner join 
            tcoverages cov on trl.Pol_Num = cov.pol_num inner join 
            tpolicys pol on pol.Pol_Num=cov.pol_num inner join 
            tplans p on p.plan_code = cov.plan_code and p.vers_num = cov.vers_num left join 
            ttrxn_df on ttrxn_df.pol_num = cov.pol_num left join 
            tams_agt_rpt_rels rel on agt.agt_code = rel.sub_agt_cd and rel.rpt_level = 1 left join 
            wrk_df on wrk_df.pol_num = cov.pol_num and wrk_df.plan_code = cov.plan_code and wrk_df.typ = 'FYP' and wrk_df.agt_code = trl.agt_code left join 
            crr_df on cov.pol_num = crr_df.pol_num  and cov.plan_code = crr_df.plan_cd and trl.agt_code = crr_df.agt_cd left join 
            crr_bk_df on cov.pol_num = crr_bk_df.pol_num  and cov.plan_code = crr_bk_df.plan_cd and trl.agt_code = crr_bk_df.agt_cd left join 
            groupdt_df on groupdt_df.pol_num = cov.pol_num
    WHERE   agt.comp_prvd_num = '01'
        AND pol.sbmt_dt between date_sub(CURRENT_DATE, 365) and CURRENT_DATE
        AND cov.pol_num = trl.pol_num
        AND cov.plan_code = trl.plan_code
        AND cov.vers_num = trl.vers_num
        AND cov.cvg_eff_dt = trl.cvg_eff_dt
""")
print("Final details:", final_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC <strong> Finalize</strong>

# COMMAND ----------

final_df.createOrReplaceTempView("final")

final = spark.sql("""
SELECT  final.agt_code agt_code,
        TO_DATE(final.agt_join_dt) agt_join_dt,
        final.pol_num pol_num,
        final.pol_stat_cd pol_stat_cd,
        final.fyp_amt fyp_amt,
        final.fyc_amt fyc_amt,
        final.ape ape,
        TO_DATE(MIN(final.Subms_dt)) AS Submis_dt,
        TO_DATE(final.IssueDate) IssueDate,
        TO_DATE(final.cnfrm_acpt_dt) cnfrm_acpt_dt,
        final.cvg_typ cvg_typ,
        final.plan_cd_base plan_cd_base,
        final.plan_code plan_code,
        final.plan_lng_desc plan_lng_desc,
        final.loc_cd loc_cd,
        final.dr_mgr_cd dr_mgr_cd,
        final.pct_splt pct_splt,
        TO_DATE(MIN(final.sbmt_at_cashier)) as sbmts_at_cashier,
        final.wa_cd_1 wa_cd_1, 
        final.wa_cd_2 wa_cd_2
FROM    final
GROUP BY
        final.agt_code,
        final.agt_join_dt,
        final.pol_num,
        final.pol_stat_cd,
        final.fyp_amt,
        final.fyc_amt,
        final.ape,
        final.IssueDate,
        final.cnfrm_acpt_dt,
        final.cvg_typ,
        final.plan_cd_base,
        final.plan_code,
        final.plan_lng_desc,
        final.loc_cd,
        final.dr_mgr_cd,
        final.pct_splt,
        final.wa_cd_1, final.wa_cd_2
""")
print("Final:", final.count())

# COMMAND ----------

final.write.mode("overwrite").parquet("abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/dashboard/CPD_SALES_REPORT_SUBMIT")

# COMMAND ----------


