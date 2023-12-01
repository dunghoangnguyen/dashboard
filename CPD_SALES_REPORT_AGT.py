# Databricks notebook source
# MAGIC %md
# MAGIC # CPD SALES REPORT AGT

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load defined functions</strong>

# COMMAND ----------

# MAGIC %run /Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load libls, params and paths</strong>

# COMMAND ----------

from pyspark.sql.functions import *

# List of ABFSS paths
cas_path = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
ams_path = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_AMS_DB/'
ams_bk_path = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_AMS_BAK_DB/'
out_path = '/mnt/lab/vn/project/dashboard/'

# List of tables
tbl_src1 = 'TAMS_AGENTS/'
tbl_src2 = 'TCOMMISSION_TRAILERS/'
tbl_src3 = 'TAMS_AGT_RPT_RELS/'
tbl_src4 = 'TWRK_FYP_RYP/'
tbl_src5 = 'TAMS_CRR_TRANSACTIONS/'
tbl_src6 = 'TAMS_CRR_TRANSACTIONS_BK/'
tbl_src7 = 'TCASH_BATCH_DETAILS/'
tbl_src8 = 'TTRXN_HISTORIES/'
tbl_src9 = 'TPLANS/'
tbl_src10 = 'TPOLICYS/'
tbl_src11 = 'TCOVERAGES/'
tbl_src12 = 'TPT_CHG_CTL/'
tbl_src13 = 'TCASH_OUTSTAND_DETAILS/'

#snapshot_paths = [lab_path,casm_path,asm_path]
daily_paths = [ams_path,cas_path,ams_bk_path]
daily_files = [tbl_src1,tbl_src2,tbl_src3,tbl_src4,tbl_src5,tbl_src6,tbl_src7,
               tbl_src8,tbl_src9,tbl_src10,tbl_src11,tbl_src12,tbl_src13]

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load working tables</strong>

# COMMAND ----------

daily_df = load_parquet_files(daily_paths,daily_files)

#list_df.update(snapshot_df)
#list_df.update(daily_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate temp views and result

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Generate temp view</strong>

# COMMAND ----------

generate_temp_view(daily_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Generate result</strong>

# COMMAND ----------

resultDF = spark.sql(f"""
SELECT	final.agt_code agt_code,
		final.agt_join_dt agt_join_dt,
		final.pol_num pol_num,
		final.pol_stat_cd pol_stat_cd,
		final.fyp_amt fyp_amt,
		final.fyc_amt fyc_amt,
		final.ape ape,
		MIN(TO_DATE(final.Subms_dt)) AS Submis_dt,
		final.IssueDate IssueDate,
		final.cnfrm_acpt_dt cnfrm_acpt_dt,
		final.cvg_typ cvg_typ,
		final.plan_cd_base plan_cd_base,
		final.plan_code plan_code,
		final.plan_lng_desc plan_lng_desc,
		final.loc_cd loc_cd,
		final.dr_mgr_cd dr_mgr_cd,
		final.pct_splt pct_splt,
		MIN(final.sbmt_at_cashier) as sbmts_at_cashier,
                final.wa_cd_1 wa_cd_1,
                final.wa_cd_2 wa_cd_2
FROM
		(SELECT distinct 
				agt.agt_code,
				agt.agt_join_dt agt_join_dt,
				cov.pol_num,
				pol.pol_stat_cd,
				wrk.fyp_amt,
				case when date_sub(CURRENT_DATE,1) > cov.cvg_eff_dt  and date_sub(CURRENT_DATE,1) < add_months(cov.cvg_eff_dt, 12) then nvl(crr.amt,0)+nvl(crr_bk.amt,0)
				else wrk.fyp_amt
				end as fyc_amt,
				(cov.cvg_prem/pol.pmt_mode * 12) ape,
				case WHEN groupdt.rowtype in ('1','2','3') then groupdt.trxn_dt else NULL END as subms_dt,
				frst_iss_dt IssueDate,
				b.cnfrm_acpt_dt,
				cov.cvg_typ,
				pol.plan_code_base plan_cd_base,
				cov.plan_code plan_code,
				p.plan_lng_desc,
				agt.loc_code loc_cd,
				rel.agt_cd dr_mgr_cd,
				trl.pct_splt,
				CASE WHEN groupdt.rowtype in ('1','2') then groupdt.trxn_dt else 'NULL' END as sbmt_at_cashier,
                                pol.wa_cd_1, pol.wa_cd_2
		FROM tams_agents agt
		left join tcommission_trailers trl on agt.agt_code = trl.agt_code
		inner join tcoverages cov on trl.Pol_Num = cov.pol_num
		inner join tpolicys pol on pol.Pol_Num=cov.pol_num
		inner join tplans p on p.plan_code = cov.plan_code and p.vers_num = cov.vers_num
		left join 
			(SELECT pol_num,MIN(b.trxn_dt) as cnfrm_acpt_dt
			FROM ttrxn_histories b
			WHERE b.trxn_cd = 'NEWBUS'
			AND b.reasn_code = '111'
			group by pol_num
			) b on b.pol_num = cov.pol_num
		left join tams_agt_rpt_rels rel on agt.agt_code = rel.sub_agt_cd and rel.rpt_level = 1
		left join (select pol_num,plan_code,typ,agt_code, sum(case when typ = 'FYP' then amt
		when typ in ('FYP-TOPUP','SP') then 0.06 * amt
		else 0 end) fyp_amt
		from twrk_fyp_ryp
		group by pol_num,plan_code,typ,agt_code) wrk
		on wrk.pol_num = cov.pol_num
		AND wrk.plan_code = cov.plan_code
		AND wrk.typ = 'FYP'
		AND wrk.agt_code = trl.agt_code
		left join (select pol_num,plan_cd,agt_cd,SUM(nvl(crr_valu, 0)) as amt
		from tams_crr_transactions t
		where crr_typ = 'COM' and t.reasn_cd = '115'
		group by pol_num,plan_cd,agt_cd) crr on cov.pol_num = crr.pol_num  and cov.plan_code = crr.plan_cd and trl.agt_code = crr.agt_cd
		left join (select pol_num,plan_cd,agt_cd,SUM(nvl(crr_valu, 0)) as amt
		from tams_crr_transactions_bk t
		where crr_typ = 'COM' and t.reasn_cd = '115'
		group by pol_num,plan_cd,agt_cd) crr_bk on cov.pol_num = crr_bk.pol_num  and cov.plan_code = crr_bk.plan_cd and trl.agt_code = crr_bk.agt_cd
		left join (select tbd.pol_num,tbd.trxn_dt, '1' as rowtype from tcash_batch_details tbd where tbd.btch_role = 'C' and tbd.btch_dtl_stat_cd = 'A'
		union select tc.pol_num,tc.sbmt_date as trxn_dt, '2' as rowtype from tpt_chg_ctl tc where  tc.dept_typ='NB'
		union select tod.pol_num,tod.input_dt as trxn_dt, '3' as rowtype from tcash_outstand_details tod) groupdt on groupdt.pol_num = cov.pol_num
		WHERE agt.comp_prvd_num = '01'
		AND agt.agt_join_dt between date_sub(CURRENT_DATE, 365) and CURRENT_DATE
		AND cov.pol_num = trl.pol_num
		AND cov.plan_code = trl.plan_code
		AND cov.vers_num = trl.vers_num
		AND cov.cvg_eff_dt = trl.cvg_eff_dt
		) final
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

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Store result to parquet</strong>

# COMMAND ----------

#spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

resultDF.write.mode('overwrite').parquet(f'{out_path}CPD_SALES_REPORT_AGT/')
