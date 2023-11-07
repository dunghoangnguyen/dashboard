# Databricks notebook source
# MAGIC %md
# MAGIC ### Step1. One-off initial lead from Cloudera
# MAGIC ### Step2. Re-create the master_digital_leads_gen_hist and store in ADLS
# MAGIC ### Step3. Schedule daily/weekly run and append data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load libl, params and paths</strong>

# COMMAND ----------

from pyspark.sql.functions import *

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

lab_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/'
sf_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_SFDC_EASYCLAIMS_DB/'
dm_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_DATAMART_DB/'
cpm_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Staging/Incremental/VN_PUBLISHED_CAMPAIGN_FILEBASED_DB/'
cas_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/'

dest_source = 'dashboard/DIGITAL_LEADS/master_digital_leads_gen_hist/'
lead_source = 'LEAD/'
user_source = 'USER/'
cli_source = 'TCLIENT_DETAILS/'
oth_source = 'TCLIENT_OTHER_DETAILS/'
pol_source = 'TPOLIDM_DAILY/'
cvg_source = 'TPORIDM_DAILY/'
agt_source = 'TAGTDM_DAILY/'
nbv_source = 'NBV_MARGIN_HISTORIES/'

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>One-off load from vn_processing_datamart_temp_db</strong>

# COMMAND ----------

#cloudera_source = 'scratch/mdlgh.csv'

#masterDF = spark.read.format('csv').option('header', True).load(f'{lab_path}{cloudera_source}')
#masterDF = masterDF.withColumn('createddate', to_date(masterDF['createddate'], 'yyyy-MM-dd'))
#masterDF = masterDF.withColumn('pol_eff_dt', to_date(masterDF['pol_eff_dt'], 'yyyy-MM-dd'))
#masterDF = masterDF.withColumn('image_date', to_date(masterDF['image_date'], 'yyy-MM-dd'))
#masterDF = masterDF.withColumn('tot_ape', masterDF['tot_ape'].cast('decimal(38,15)'))
#masterDF = masterDF.withColumn('nbv', masterDF['nbv'].cast('double'))

# Show the schema of the DataFrame
#filteredDF = masterDF.filter(col('pol_num').isNotNull())
#filteredDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load all working tables</strong>

# COMMAND ----------

leadDF = spark.read.parquet(f'{sf_path}{lead_source}')
userDF = spark.read.parquet(f'{sf_path}{user_source}')
cliDF = spark.read.parquet(f'{cas_path}{cli_source}')
othDF = spark.read.parquet(f'{cas_path}{oth_source}')
polDF = spark.read.parquet(f'{dm_path}{pol_source}')
cvgDF = spark.read.parquet(f'{dm_path}{cvg_source}')
agtDF = spark.read.parquet(f'{dm_path}{agt_source}')
nbvDF = spark.read.parquet(f'{cpm_path}{nbv_source}')
histDF = spark.read.parquet(f'{lab_path}{dest_source}')

histDF = histDF.toDF(*[col.lower() for col in histDF.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Create DIGITAL_LEADS_GEN</strong>

# COMMAND ----------

leadDF.createOrReplaceTempView('lead')
userDF.createOrReplaceTempView('user')

leadexclDF = spark.sql("""
select	distinct id, website
from	lead
where	LOWER(website) LIKE '%move%uu-dai%'
		OR LOWER(website) LIKE '%lead-form-experience-fragment-move%'	
		OR LOWER(website) LIKE '%thong-tin-dang-ky-nhan-medal-manulifemove.html%'
		OR LOWER(website) LIKE '%vi-move-redemption-manulearn.content.html%'
		OR LOWER(website) LIKE '%/move/dieu-khoan-su-dung.html%'
		OR LOWER(website) LIKE '%/lead-form-experience-fragment-move/vi-move-offer-page-nike.html%'                  
""")

leadexclDF.createOrReplaceTempView('digital_leads_gen_excl')

digital_leads_gen = spark.sql("""
SELECT  DISTINCT 
		TO_DATE(lead.CREATEDDATE) AS CREATEDDATE,
		lead.LASTNAME LASTNAME,
		SUBSTR(REGEXP_REPLACE(TRIM(lead.PHONE),'[^0-9A-Za-z]',''),-9) PHONE,
		lead.EMAIL EMAIL,
		lead.STATE STATE,
		lead.LEADSOURCE LEADSOURCE,
		lead.DESCRIPTION DESCRIPTION,
		IF(lead.ISCUSTOMER__C='false','N','Y') ISCUSTOMER,
		lead.STATUS STATUS,
		lead.WEBSITE WEBSITE,
		lead.WEBTRACKINGID__C WEBTRACKINGID,
		lead.UTMCAMPAIGNSOURCE__C UTMCAMPAIGNSOURCE,
		lead.UTMCAMPAIGNMEDIUM__C UTMCAMPAIGNMEDIUM,
		lead.UTMCAMPAIGNNAME__C UTMCAMPAIGNNAME,
		lead.UTMCAMPAIGNCONTENT__C UTMCAMPAIGNCONTENT,
		lead.ID ID,
		SUBSTR(lead.ID,1,15) LEADID,
		lead.OWNERID OWNERID,
		own.ALIAS OWNER_ALIAS,
		lead.CREATEDBYID CREATEDBYID,
		crt.NAME CREATED_BY,
		lead.AGENT_ACCOUNT_AGENT_CODE__C AGENT_CODE
FROM	lead INNER JOIN
		user own ON lead.OWNERID=own.ID INNER JOIN
		user crt ON	lead.CREATEDBYID=crt.ID LEFT JOIN
		digital_leads_gen_excl excl ON lead.ID=excl.ID	
WHERE	(lead.WEBSITE IS NOT NULL OR
		lead.UTMCAMPAIGNSOURCE__C IS NOT NULL OR
		lead.UTMCAMPAIGNMEDIUM__C IS NOT NULL OR
		lead.UTMCAMPAIGNNAME__C IS NOT NULL OR
		lead.UTMCAMPAIGNCONTENT__C IS NOT NULL
		)
	AND	excl.ID IS NULL
	AND	lead.STATUS<>'New'                       
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Create MASTER_DIGITAL_LEADS_GEN</strong>

# COMMAND ----------

digital_leads_gen.createOrReplaceTempView('digital_leads_gen')
polDF.createOrReplaceTempView('tpolidm_daily')
cvgDF.createOrReplaceTempView('tporidm_daily')
agtDF.createOrReplaceTempView('tagtdm_daily')
nbvDF.createOrReplaceTempView('nbv_margin_histories')

digital_cvg = spark.sql("""
SELECT	pol.POL_NUM,
		pol.PO_NUM,
		pol.PLAN_CODE,
		pol.PLAN_NM,
		to_date(pol.POL_ISS_DT) AS pol_iss_dt,
		pol.POL_STAT_DESC,
		pol.TOT_APE,
		pol.WA_CODE,
		agt.LOC_CD,
		case when agt.loc_cd is null then 
			(case when DIST_CHNL_CD in ('03','10','14','16','17','18','19','22','23','24','25','29','30','31','32','33','39','41','44','47','49','51','52','53') then round(cvg.CVG_PREM*NVL(pln.nbv_margin_banca_other_banks,nbv.nbv_margin_banca_other_banks),2) -- 'Banca'
				when DIST_CHNL_CD in ('48') then round(cvg.CVG_PREM*NVL(pln.nbv_margin_other_channel_affinity,nbv.nbv_margin_other_channel_affinity),2)--'Affinity'
				when DIST_CHNL_CD in ('01', '02', '08', '50', '*') then round(cvg.CVG_PREM*NVL(pln.nbv_margin_agency,nbv.nbv_margin_agency),2)--'Agency'
				when DIST_CHNL_CD in ('05','06','07','34','36') then round(cvg.CVG_PREM*NVL(pln.nbv_margin_dmtm,nbv.nbv_margin_dmtm),2)--'DMTM'
				when DIST_CHNL_CD in ('09') then round(cvg.CVG_PREM*-1.34041044648343,2)--'MI'
			else round(cvg.CVG_PREM*NVL(pln.nbv_margin_other_channel,nbv.nbv_margin_other_channel),2) end) --'Unknown'
			when DIST_CHNL_CD in ('*') then round(cvg.CVG_PREM*NVL(pln.nbv_margin_agency,nbv.nbv_margin_agency),2)--'Agency'
			when agt.loc_cd like 'TCB%' then round(cvg.CVG_PREM*NVL(pln.nbv_margin_banca_tcb,nbv.nbv_margin_banca_tcb),2) --'TCB'
			when agt.loc_cd like 'SAG%' then round(cvg.CVG_PREM*NVL(pln.nbv_margin_banca_scb,nbv.nbv_margin_banca_scb),2) --'SCB'
   			when agt.loc_cd like 'VTI%' then round(cvg.CVG_PREM*NVL(pln.nbv_margin_banca_vti,nbv.nbv_margin_banca_vti),2) --'VTB'
		else round(cvg.CVG_PREM*NVL(pln.nbv_margin_other_channel,nbv.nbv_margin_other_channel),2) end as NBV, --'Unknown'
		case
			when DIST_CHNL_CD in ('01','02','08','50','*')
			then 'Agency'
			when DIST_CHNL_CD IN ('05','06','07','34','36')
			then 'DMTM'
			else 'Banca'
		end as CHANNEL				 
FROM	tpolidm_daily pol
	INNER JOIN
		tporidm_daily cvg
	 ON	pol.POL_NUM = cvg.POL_NUM
	LEFT JOIN
		tagtdm_daily agt
	 ON pol.WA_CODE = agt.AGT_CODE
	LEFT JOIN
		nbv_margin_histories pln 
	 ON pol.PLAN_CODE = pln.PLAN_CODE and 
concat((case when FLOOR(month(pol.POL_ISS_DT)/3.1)=0 then -1
			 when FLOOR(month(pol.POL_ISS_DT)/3.1)=1 then -1
			 else 0 end) + year(pol.POL_ISS_DT), ' Q',
	   (case when FLOOR(month(pol.POL_ISS_DT)/3.1)=0 then 3
			 when FLOOR(month(pol.POL_ISS_DT)/3.1)=1 then 4
			 when FLOOR(month(pol.POL_ISS_DT)/3.1)=2 then 1
			 else 2 end))=pln.effective_qtr
	LEFT JOIN
		vn_published_campaign_db.vn_plan_code_map nbv
	 ON	pol.PLAN_CODE = nbv.plan_code
WHERE	pol.POL_STAT_CD NOT IN ('8','A','N','R','X')
	AND SUBSTR(pol.PLAN_CODE,1,3) NOT IN ('FDB','BIC','NP0')                   
""")

digital_cvg.createOrReplaceTempView('master_digital_cvg')
digital_pol = spark.sql("""
select	POL_NUM,
		PO_NUM,
		PLAN_CODE,
		PLAN_NM,
		POL_ISS_DT,
		POL_STAT_DESC,
		TOT_APE,
		WA_CODE,
		LOC_CD,
		SUM(NBV) NBV,
		CHANNEL
from	master_digital_cvg
group by
		POL_NUM,
		PO_NUM,
		PLAN_CODE,
		PLAN_NM,
		POL_ISS_DT,
		POL_STAT_DESC,
		TOT_APE,
		WA_CODE,
		LOC_CD,
		CHANNEL             
""")

cliDF.createOrReplaceTempView('tclient_details')
othDF.createOrReplaceTempView('tclient_other_details')
digital_pol.createOrReplaceTempView('master_digital_pols')

digital_leads_phone = spark.sql("""
SELECT	pol.POL_NUM,
		pol.PO_NUM,
		pol.PLAN_CODE,
		pol.PLAN_NM,
		pol.POL_ISS_DT,
		pol.POL_STAT_DESC,
		pol.TOT_APE,
		pol.WA_CODE,
		pol.LOC_CD,
		cli1.CLI_NM,
		IF(LENGTH(REGEXP_REPLACE(TRIM(MOBL_PHON_NUM),'[^0-9A-Za-z]',''))<9,REGEXP_REPLACE(TRIM(MOBL_PHON_NUM),'[^0-9A-Za-z]',''),SUBSTR(REGEXP_REPLACE(TRIM(MOBL_PHON_NUM),'[^0-9A-Za-z]',''),-9)) MOBL_PHON_NUM,
		cli2.EMAIL_ADDR,
		pol.NBV, 
		pol.CHANNEL
FROM	master_digital_pols pol
	INNER JOIN
		tclient_details cli1
	 ON	pol.PO_NUM = cli1.CLI_NUM		
	LEFT JOIN
		tclient_other_details cli2
	 ON cli1.CLI_NUM = cli2.CLI_NUM
WHERE	cli1.MOBL_PHON_NUM IS NOT NULL
UNION
SELECT	pol.POL_NUM,
		pol.PO_NUM,
		pol.PLAN_CODE,
		pol.PLAN_NM,
		pol.POL_ISS_DT,
		pol.POL_STAT_DESC,
		pol.TOT_APE,
		pol.WA_CODE,
		pol.LOC_CD,
		cli1.CLI_NM,
		IF(LENGTH(REGEXP_REPLACE(TRIM(PRIM_PHON_NUM),'[^0-9A-Za-z]',''))<9,REGEXP_REPLACE(TRIM(PRIM_PHON_NUM),'[^0-9A-Za-z]',''),SUBSTR(REGEXP_REPLACE(TRIM(PRIM_PHON_NUM),'[^0-9A-Za-z]',''),-9)) MOBL_PHON_NUM,
		cli2.EMAIL_ADDR,
		pol.NBV, 
		pol.CHANNEL
FROM	master_digital_pols pol
	INNER JOIN
		tclient_details cli1
	 ON	pol.PO_NUM = cli1.CLI_NUM		
	LEFT JOIN
		tclient_other_details cli2
	 ON cli1.CLI_NUM = cli2.CLI_NUM
WHERE	cli1.PRIM_PHON_NUM IS NOT NULL
UNION
SELECT	pol.POL_NUM,
		pol.PO_NUM,
		pol.PLAN_CODE,
		pol.PLAN_NM,
		pol.POL_ISS_DT,
		pol.POL_STAT_DESC,
		pol.TOT_APE,
		pol.WA_CODE,
		pol.LOC_CD,
		cli1.CLI_NM,
		IF(LENGTH(REGEXP_REPLACE(TRIM(OTHR_PHON_NUM),'[^0-9A-Za-z]',''))<9,REGEXP_REPLACE(TRIM(OTHR_PHON_NUM),'[^0-9A-Za-z]',''),SUBSTR(REGEXP_REPLACE(TRIM(OTHR_PHON_NUM),'[^0-9A-Za-z]',''),-9)) MOBL_PHON_NUM,
		cli2.EMAIL_ADDR,
		pol.NBV, 
		pol.CHANNEL
FROM	master_digital_pols pol
	INNER JOIN
		tclient_details cli1
	 ON	pol.PO_NUM = cli1.CLI_NUM		
	LEFT JOIN
		tclient_other_details cli2
	 ON cli1.CLI_NUM = cli2.CLI_NUM
WHERE	cli1.OTHR_PHON_NUM IS NOT NULL                     
""")

digital_leads_email = spark.sql("""
SELECT	pol.POL_NUM,
		pol.PO_NUM,
		pol.PLAN_CODE,
		pol.PLAN_NM,
		pol.POL_ISS_DT,
		pol.POL_STAT_DESC,
		pol.TOT_APE,
		pol.WA_CODE,
		pol.LOC_CD,
		cli1.CLI_NM,
		SUBSTR(REGEXP_REPLACE(TRIM(cli1.MOBL_PHON_NUM),'[^0-9A-Za-z]',''),-9) MOBL_PHON_NUM,
		cli2.EMAIL_ADDR,
		pol.NBV, 
		pol.CHANNEL
FROM	master_digital_pols pol
	INNER JOIN
		tclient_details cli1
	 ON	pol.PO_NUM = cli1.CLI_NUM		
	LEFT JOIN
		tclient_other_details cli2
	 ON cli1.CLI_NUM = cli2.CLI_NUM
WHERE	cli2.EMAIL_ADDR IS NOT NULL                      
""")

digital_leads_phone.createOrReplaceTempView('master_digital_leads_phone')
digital_leads_email.createOrReplaceTempView('master_digital_leads_email')

digital_leads_sales = spark.sql("""
SELECT	A.*
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',null,B.POL_NUM) POL_NUM
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',null,B.PO_NUM) PO_NUM
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',null,B.PLAN_CODE) PLAN_CODE
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',null,B.PLAN_NM) PLAN_NM
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',null,B.POL_ISS_DT) POL_ISS_DT
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',null,B.POL_STAT_DESC) POL_STAT_DESC
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',null,B.WA_CODE) WA_CODE
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',null,B.LOC_CD) LOC_CD
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',null,B.CLI_NM) CLI_NM
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',null,B.MOBL_PHON_NUM) MOBL_PHON_NUM
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',null,B.EMAIL_ADDR) EMAIL_ADDR
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',0,B.TOT_APE) TOT_APE
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',0,B.NBV) NBV
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',null,B.CHANNEL) CHANNEL
FROM	DIGITAL_LEADS_GEN A
	INNER JOIN
		MASTER_DIGITAL_LEADS_PHONE B
	 ON	A.PHONE = B.MOBL_PHON_NUM
WHERE	DATEDIFF(B.POL_ISS_DT, A.CREATEDDATE) BETWEEN 2 AND 180
	AND	B.MOBL_PHON_NUM IS NOT NULL
UNION
SELECT	A.*
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',null,B.POL_NUM) POL_NUM
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',null,B.PO_NUM) PO_NUM
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',null,B.PLAN_CODE) PLAN_CODE
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',null,B.PLAN_NM) PLAN_NM
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',null,B.POL_ISS_DT) POL_ISS_DT
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',null,B.POL_STAT_DESC) POL_STAT_DESC
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',null,B.WA_CODE) WA_CODE
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',null,B.LOC_CD) LOC_CD
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',null,B.CLI_NM) CLI_NM
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',null,B.MOBL_PHON_NUM) MOBL_PHON_NUM
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',null,B.EMAIL_ADDR) EMAIL_ADDR
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',0,B.TOT_APE) TOT_APE
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',0,B.NBV) NBV
		,IF(LOWER(TRIM(A.STATUS)) LIKE '%duplicate%',null,B.CHANNEL) CHANNEL
FROM	DIGITAL_LEADS_GEN A
	INNER JOIN
		MASTER_DIGITAL_LEADS_EMAIL B
	 ON	TRIM(UPPER(A.EMAIL)) = TRIM(UPPER(B.EMAIL_ADDR))
WHERE	DATEDIFF(B.POL_ISS_DT, A.CREATEDDATE) BETWEEN 2 AND 366                      
""")

digital_leads_sales.createOrReplaceTempView('master_digital_leads_sales')

master_digital_leads_gen = spark.sql("""
SELECT 	A.*,
		B.POL_NUM,
		B.PO_NUM,
		B.PLAN_CODE,
		B.PLAN_NM,
		B.POL_ISS_DT,
		B.POL_STAT_DESC,
		B.WA_CODE,
		B.LOC_CD,
		B.CLI_NM,
		B.MOBL_PHON_NUM,
		B.EMAIL_ADDR,
		B.TOT_APE,
		B.NBV,
		B.CHANNEL								  
FROM	DIGITAL_LEADS_GEN A
	LEFT JOIN
		MASTER_DIGITAL_LEADS_SALES B
	 ON	A.ID=B.ID                             
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Append to MASTER_DIGITAL_LEADS_GEN_HIST</strong>

# COMMAND ----------

master_digital_leads_gen.createOrReplaceTempView('master_digital_leads_gen')
histDF.createOrReplaceTempView('master_digital_leads_gen_hist')

tempDF = spark.sql("""
SELECT	distinct
		TO_DATE(createddate) createddate,
		lastname,
		phone,
		email,
		state,
		leadsource,
		description,
		iscustomer,
		status,
		website,
		webtrackingid,
		utmcampaignsource,
		utmcampaignmedium,
		utmcampaignname,
		utmcampaigncontent,
		id,
		leadid,
		ownerid,
		owner_alias,
		createdbyid,
		created_by,
		agent_code,
		pol_num,
		po_num,
		plan_code,
		plan_nm,
		pol_iss_dt,
		pol_stat_desc,
		wa_code,
		loc_cd,
		cli_nm,
		mobl_phon_num,
		email_addr,
		tot_ape,
		nbv,
		channel,
		IF(TRIM(UPPER(LASTNAME))=TRIM(UPPER(CLI_NM)),'Y','N') NAME_HIT_IND,
		IF(TRIM(UPPER(PHONE))=TRIM(UPPER(MOBL_PHON_NUM)),'Y','N') PHONE_HIT_IND,
		IF(TRIM(UPPER(EMAIL))=TRIM(UPPER(EMAIL_ADDR)),'Y','N') EMAIL_HIT_IND,
		CURRENT_DATE image_date
FROM	MASTER_DIGITAL_LEADS_GEN
""")

lastDF = spark.sql("""
select	id,
		leadid,
		createddate,
		pol_num,
		MAX(image_date) image_date
from	MASTER_DIGITAL_LEADS_GEN_HIST
where	pol_num is not null
group by
		id,
		leadid,
		createddate,
		pol_num        
""")

tempDF.createOrReplaceTempView('master_digital_leads_gen_hist_tmp')
lastDF.createOrReplaceTempView('master_digital_leads_gen_lst')

masterDF = spark.sql("""
SELECT	distinct
		t.createddate,
		t.lastname,
		t.phone,
		t.email,
		t.state,
		t.leadsource,
		t.description,
		t.iscustomer,
		t.status,
		t.website,
		t.webtrackingid,
		t.utmcampaignsource,
		t.utmcampaignmedium,
		t.utmcampaignname,
		t.utmcampaigncontent,
		t.id,
		t.leadid,
		t.ownerid,
		t.owner_alias,
		t.createdbyid,
		t.created_by,
		t.pol_num,
		t.po_num,
		t.plan_code,
		t.plan_nm,
		t.pol_iss_dt as pol_eff_dt,
		t.pol_stat_desc,
		t.wa_code,
		t.loc_cd,
		t.cli_nm,
		t.mobl_phon_num,
		t.email_addr,
		ROUND(t.tot_ape,2) tot_ape,
		ROUND(t.nbv,2) nbv,
		t.channel,
		t.name_hit_ind,
		t.phone_hit_ind,
		t.email_hit_ind,
		t.agent_code,
		COALESCE(lst.image_date,t.image_date) image_date
FROM	master_digital_leads_gen_hist_tmp t
	LEFT JOIN
		master_digital_leads_gen_lst lst
	 ON	t.id = lst.id AND
		t.leadid = lst.leadid AND
		t.createddate = lst.createddate           
""")

# COMMAND ----------

#masterSumDF = masterDF.groupBy('image_date')\
#    .agg(
#        count('id').alias('no_rows'),
#        countDistinct('leadid').alias('no_leads'),
#        countDistinct('pol_num').alias('no_pols')
#    ).toPandas()

#masterSumDF
#masterDF.filter(col('pol_num').isNotNull()).display()

# COMMAND ----------

masterDF.write.mode('overwrite').partitionBy('image_date').parquet(f'{lab_path}{dest_source}')

# COMMAND ----------


