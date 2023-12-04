# Databricks notebook source
# MAGIC %md
# MAGIC # PEXFN11VN

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load defined functions</strong>

# COMMAND ----------

# MAGIC %run /Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load libls, params and paths</strong>

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta

# List of ABFSS paths
cas_path = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
ams_path = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_AMS_DB/'
out_path = '/mnt/lab/vn/project/dashboard/'

# List of tables
tbl_src1 = 'tcash_batch_headers/'
tbl_src2 = 'tcash_batch_details/'
tbl_src3 = 'tcoverages/'
tbl_src4 = 'tams_agents/'
tbl_src5 = 'tcheque_banks/'
tbl_src6 = 'tbank_details/'
tbl_src7 = 'tchart_of_accounts/'
tbl_src8 = 'tfield_values/'
tbl_src9 = 'tpolicys/'
tbl_src10 = 'tdnr_pickup_track_sh/'
tbl_src11 = 'tdnr_pickup_return_track'

daily_paths = [ams_path,cas_path]
daily_files = [tbl_src1,tbl_src2,tbl_src3,tbl_src4,tbl_src5,tbl_src6,
               tbl_src7,tbl_src8,tbl_src9,tbl_src10,tbl_src11]

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load working tables</strong>

# COMMAND ----------

list_df = {}

daily_df = load_parquet_files(daily_paths,daily_files)

list_df.update(daily_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate temp views and result

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Generate temp view</strong>

# COMMAND ----------

generate_temp_view(list_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Generate result</strong>

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>collection</strong>

# COMMAND ----------

collectionDF = spark.sql(f"""
with pexfn11vn_collection_tmp_2 as (
select
	a.trxn_dt
	,a.rep_num
	,a.pol_num
	,a.nhothu_user
	,a.thu
	,a.btch_typ	
	,a.bank_cd
	,case
		when c_lkup_valu.fld_lkup_valu = 'CS' then
			case
				when a.bank_cd is not null then
					case
						when c_bank_tf.acct_mne_cd is null then null
						else c_bank_tf.acct_mne_cd
					end
				else  'PCCASH'
			end
		when c_lkup_valu.fld_lkup_valu = 'TF' and a.bank_cd is not null then			
			case
				when c_bank_tf.bank_cd is null then null
				else c_bank_tf.acct_mne_cd
			end
		when c_lkup_valu.fld_lkup_valu = 'AUTO' and a.bank_cd is not null then
			case
				when c_bank_auto.acct_mne_cd is null then null
				else c_bank_auto.acct_mne_cd
			end
		else null
	end l_acct_mne_cd
	,a.prod_code
	,a.team_code
	,a.amt
	,a.sophieu
	,a.par_code_0
	,a.par_code_1
	,a.par_code_3
	,a.par_code_4		    
from
	(
		with pexfn11vn_collection_tmp_1 as (
			Select
				hd.trxn_dt
				,dt.rep_num
				,dt.pol_num
				,hd.user_id nhothu_user
				,hd.br_code thu
				,hd.btch_typ
				,nvl(hd.bank_code,dt.bank_cd) bank_cd	
				,nvl(cvg.plan_code,'55550') prod_code
				,agt.team_code
				,sum(dt.trxn_amt) amt
				,count(dt.pol_num) sophieu
				,sum(case when cvg.par_code = '0' then dt.trxn_amt else 0 end) par_code_0
				,sum(case when cvg.par_code = '1' then dt.trxn_amt else 0 end) par_code_1
				,sum(case when nvl(cvg.par_code,'3') = '3' then dt.trxn_amt else 0 end) par_code_3
				,sum(case when cvg.par_code = '4' then dt.trxn_amt else 0 end) par_code_4		    
			From
				tcash_batch_headers hd	
				inner join tcash_batch_details dt on (hd.trxn_dt = dt.trxn_dt and hd.btch_num = dt.btch_num and hd.btch_role = dt.btch_role)	
				left join tcoverages cvg on (dt.pol_num = cvg.pol_num and cvg.cvg_typ = 'B' and cvg.cvg_reasn = 'O')
				left join tpolicys pol on (dt.pol_num = pol.pol_num)
				left join tams_agents agt on (pol.agt_code = agt.agt_code)	
			where 
				hd.trxn_dt >= last_day(date_add(add_months(current_date,-4),1))
				and hd.btch_role = 'C'		    
				and dt.trxn_cd ='COLECT'
			group by
					hd.trxn_dt
					,dt.rep_num
					,dt.pol_num
					,hd.user_id
					,hd.br_code
					,hd.btch_typ		
					,nvl(hd.bank_code,dt.bank_cd)
					,nvl(cvg.plan_code,'55550')
					,agt.team_code
		) select * from pexfn11vn_collection_tmp_1
	) a
	left join (
		with c_lkup_valu as(
			select
				fld_lkup_valu
				,fld_valu
			from
				tfield_values
			where
				fld_nm = 'BTCH_TYP'
		) select * from c_lkup_valu
	) c_lkup_valu on (a.btch_typ = c_lkup_valu.fld_valu)
	left join (
		with c_bank_tf as (
			select distinct
				acct_mne_cd
				,bank_cd
			from
				tcheque_banks
			where
				acct_mne_cd is not null
		) select * from c_bank_tf
	) c_bank_tf on (a.bank_cd = c_bank_tf.bank_cd)
	left join (
		with c_bank_auto as (
			select distinct
				acct_mne_cd
				,bank_cd
			from
				tbank_details
			where
				acct_mne_cd is not null
		) select * from c_bank_auto
	) c_bank_auto on (a.bank_cd = c_bank_auto.bank_cd)
)
select
	a.trxn_dt trxn_dt
	,a.rep_num rep_num
	,a.pol_num pol_num
	,a.nhothu_user nhothu_user
	,a.thu thu
	,a.btch_typ	btch_typ
	,a.bank_cd
	,nvl(b1.acct_num,b2.acct_num) acct_num
	,a.prod_code prod_code
	,a.team_code team_code
	,a.amt amt
	,a.sophieu sophieu
	,a.par_code_0 par_code_0
	,a.par_code_1 par_code_1
	,a.par_code_3 par_code_3
	,a.par_code_4 par_code_4
from
	pexfn11vn_collection_tmp_2 a
	left join (
		select
				acct_num
				,acct_mne_cd bank_cd
			from
				tchart_of_accounts		
			union all
			select
				b.acct_num
				,a.bank_cd
			from
				tbank_details a
				inner join tchart_of_accounts b on (a.acct_mne_cd = b.acct_mne_cd)
	) b1 on (a.l_acct_mne_cd = b1.bank_cd)
	left join (
		select
			acct_num
			,acct_mne_cd bank_cd
		from
			tchart_of_accounts		
		union all
		select
			b.acct_num
			,a.bank_cd
		from
			tbank_details a
			inner join tchart_of_accounts b on (a.acct_mne_cd = b.acct_mne_cd)
	) b2 on (a.bank_cd = b2.bank_cd)      
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>dnr</strong>

# COMMAND ----------

dnrDF = spark.sql("""
select
	trxn_dt trxn_dt
	,a.br_code br_code
	,agt.team_code team_code
	,cvg.pol_num pol_num
	,cvg.plan_code plan_code
	,sum(trxn_amt) amt
	,sum(case when cvg.par_code = '0' then trxn_amt else 0 end) par_code_0
	,sum(case when cvg.par_code = '1' then trxn_amt else 0 end) par_code_1
	,sum(case when nvl(cvg.par_code,'3') = '3' then trxn_amt else 0 end) par_code_3
	,sum(case when cvg.par_code = '4' then trxn_amt else 0 end) par_code_4	    
from
	(
		select
			dt.trxn_dt
			,dt.br_code
			,dt.pol_num
			,dt.trxn_amt trxn_amt
		from
			tdnr_pickup_track_sh dt
		where
			dt.rec_typ='D'
			and dt.trxn_dt >= last_day(date_add(add_months(current_date,-4),1))
			and dt.BTCH_TYP in ('CS','AVCS')
			and dt.REASN_CODE in ('01','02')
			and dt.btch_stat_code='C'
		union all
		select
			dt.trxn_dt
			,pck.br_code
			,dt.pol_num
			,- dt.trxn_amt trxn_amt
		from
			tdnr_pickup_return_track dt
			inner join (
				select distinct
					br_code
					,dnr_num
				from
					tdnr_pickup_track_sh a
				where
					a.btch_typ in ('CS','AVCS')
					and a.reasn_code in ('01','02')
			) pck on (dt.dnr_num=pck.dnr_num)
		where
			dt.rec_typ='D'
		    and dt.trxn_dt >= last_day(date_add(add_months(current_date,-4),1))
		    and dt.BTCH_TYP in ('CS','AVCS')
		    and dt.REASN_CODE in ('01','02')
		    and dt.btch_stat_code='C'		       
	) a
	inner join tcoverages cvg on (a.pol_num = cvg.pol_num and cvg_typ='B' and cvg_reasn='O')
	inner join tpolicys pol on (a.pol_num = pol.pol_num)
	inner join vn_published_ams_db.tams_agents agt on (pol.agt_code = agt.agt_code)	
group by
	trxn_dt
	,a.br_code
	,agt.team_code
	,cvg.plan_code
	,cvg.pol_num      
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Store result to parquet</strong>

# COMMAND ----------

#spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

collectionDF.write.mode('overwrite').parquet(f'{out_path}PEXFN11VN/collection/')
dnrDF.write.mode('overwrite').parquet(f'{out_path}PEXFN11VN/dnr/')
