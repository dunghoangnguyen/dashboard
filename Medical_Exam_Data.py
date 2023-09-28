# Databricks notebook source
# MAGIC %md
# MAGIC # Medical Exam Data

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load UDFs</strong>

# COMMAND ----------

# MAGIC %run "/Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions"

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load all paths and params</strong>

# COMMAND ----------

cas_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
lab_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/'

tbl_src1 = 'tmed_details/'
tbl_src2 = 'tmed_client_details/'
tbl_src3 = 'tclient_policy_links/'
tbl_src4 = 'tclient_details/'
tbl_src5 = 'tmed_dtl_exam/'
tbl_src6 = 'tmed_supp_exam/'
tbl_src7 = 'tmed_field_values/'

abfss_paths = [cas_path]
parquet_files = [tbl_src1,tbl_src2,tbl_src3,tbl_src4,
                 tbl_src5,tbl_src6,tbl_src7]

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load datasets and create temp views</strong>

# COMMAND ----------

list_df = load_parquet_files(abfss_paths, parquet_files)

# COMMAND ----------

generate_temp_view(list_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Generate result</strong>

# COMMAND ----------

sql_string = """
Select distinct
	p.POL_NUM as policy_no
	,p.CLI_NM as client_name
	,p.MED_DATE as medic_date
	,p.MED_EXAM as medic_check_items
	,p.BT as blood_test
	,p.IN_EXPS as fee
	,p.fld_valu_desc as hospital
	,p.fld_cd as med_hosp
from
	(
		select
			mdl.pol_num
			,cli.cli_nm
			,mdl.MED_DATE		
			,collect_set(mse.med_supp_exam) as med_exam
			,mdl.IN_EXPS*1000 in_exps
			,fld.fld_valu_desc
			,mdl.med_hosp fld_cd
			,case 
				when mde.pol_num is not null then 'Y'
				else null 
			end as BT
		from
			tmed_details mdl
			left join (
				select distinct
					case 
						when mcd.ins_nm is not null then mcd.ins_nm
						else null 
					end as cli_nm 
					,mcd.pol_num 
					,mcd.med_seq
				from
					tmed_client_details mcd
					left join tclient_policy_links cpl on (mcd.pol_num = cpl.pol_num and cpl.link_typ = 'I')
					left join  tclient_details cd on (cpl.cli_num = cd.cli_num)
					left join tmed_dtl_exam de on (mcd.pol_num = de.pol_num)				
			) cli on (mdl.pol_num = cli.pol_num and mdl.med_seq = cli.med_seq)
			left join tmed_supp_exam mse on (
																	mdl.pol_num = mse.pol_num
																	and mdl.med_seq = mse.med_seq
																	and mdl.med_date = mse.med_date
																	and mdl.med_hosp = mse.med_hosp
																)
			left join tmed_dtl_exam mde on (
																	mdl.pol_num = mde.pol_num
																	and mdl.med_seq = mde.med_seq
																	and mdl.med_hosp = mde.med_hosp
																	and mdl.med_date = mde.med_date
																	and mde.exam_cd = '04'
																)
			left join tmed_field_values fld on (fld.fld_valu= mdl.med_hosp)
		where 
			mdl.med_comp_dt is not null
			and fld.fld_nm = 'med_hosp'				
		group by
			fld.fld_valu_desc
			,mdl.med_hosp
			,mdl.pol_num
			,cli.cli_nm
			,mdl.med_date			
			,mdl.in_exps	
			,mde.pol_num
		order by
			fld.fld_valu_desc
			,mdl.med_date
			,mdl.pol_num
	) p
where
	p.med_date between date_sub(current_date,365) and current_date
"""

resultDF = sql_to_df(sql_string, 0, spark)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Store the result back to ADLS2</strong>

# COMMAND ----------

#spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
resultDF.write.mode("overwrite").parquet(f'{lab_path}dashboard/MEDICAL_EXAM_DATA/')
