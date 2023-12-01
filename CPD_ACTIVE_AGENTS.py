# Databricks notebook source
# MAGIC %run /Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions

# COMMAND ----------

ams_path = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_AMS_DB/'
cas_path = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
out_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/dashboard/'

src_tbl1 = 'tams_agents/'
src_tbl2 = 'tams_stru_groups/'
src_tbl3 = 'tams_agt_rpt_rels/'
src_tbl4 = 'tams_locations/'
src_tbl5 = 'tams_candidates/'
src_tbl6 = 'tams_comp_chnl_mappings/'
src_tbl7 = 'tams_exam_schds/'
src_tbl8 = 'tams_classes/'
src_tbl9 = 'tams_class_attd_regs/'
src_tbl10 = 'TAMS_TRAINING_RSLTS/'
src_tbl11 = 'tfield_values/'

path_list = [ams_path, cas_path]
tbl_list = [src_tbl1, src_tbl2, src_tbl3, src_tbl4, src_tbl5,
            src_tbl6, src_tbl7, src_tbl8, src_tbl9, src_tbl10, src_tbl11,]

# COMMAND ----------

df_list = load_parquet_files(path_list, tbl_list)
generate_temp_view(df_list)

# COMMAND ----------

result = spark.sql("""
SELECT DISTINCT
        a.agt_code          Agent_Code,
        a.agt_nm            Agent_Name,
        a.class_num         Class_Code,
        exam.exam_master_cd Exam_Code ,
        a.agt_join_dt       Agent_Load_Code,
        a.br_code                     ,
        a.loc_code Location_Code      ,
        a.stat_cd  status             ,
        a.rank_cd                     ,
        YEAR(a.agt_join_dt)                                              vYear,
        MONTH(a.agt_join_dt)                                             vMonth,
        exam.exam_dt                                                     Exam_Date,
        CASE WHEN b.mgr_cd=a.agt_code THEN c.agt_cd ELSE b.mgr_cd END AS Direct_Manager_Code,
        d.loc_desc           Location_Name,
        a.team_code                   ,
        a.comp_prvd_num               ,
        e.id_num  ID_Number           ,
        e.can_dob Agent_DOB           ,
        e.sex_code                    ,
        CASE WHEN chmap.chnl_cd='BK' THEN 'Bancassurance' WHEN chmap.chnl_cd='DR' THEN 'DMTM' WHEN chmap.chnl_cd='AGT' THEN 'Agency' END AS channel_code ,
        ex_cls.cls_num   	ex_cls_num   ,
        EX_cls.exam_master_cd Ex_exam_code ,
        ex_cls.exam_dt   	Ex_exam_dt   ,
        ex_cls.eff_dt    	ex_eff_dt    ,
        fva.fld_valu_desc  AS agent_group
FROM
        tams_agents a
INNER JOIN
        tams_stru_groups b
ON
        a.unit_code    = b.stru_grp_cd
AND     b.stru_grp_typ = 'UN'
INNER JOIN
        tams_agt_rpt_rels c
ON
        a.agt_code  = c.sub_agt_cd
AND     c.rpt_level = '1'
INNER JOIN
        tams_locations d
ON
        a.loc_code = d.loc_cd
INNER JOIN
        tams_candidates e
ON
        a.can_num = e.can_num
INNER JOIN
        tams_comp_chnl_mappings chmap
ON
        a.comp_prvd_num = chmap.comp_prvd_num
AND     chmap.chnl_cd IN ('AGT',
                          'BK' ,
                          'DR')
LEFT JOIN
        tams_exam_schds exam
ON
        a.class_num=exam.class_cd
LEFT JOIN
        (
                SELECT
                        reg.can_num       ,
                        cls.cls_num       ,
                        exs.exam_dt       ,
                        exs.exam_master_cd,
                        rslt.eff_dt
                FROM
                        tams_classes cls
                INNER JOIN
                        tams_class_attd_regs reg
                ON
                        reg.class_cd=cls.cls_num
                INNER JOIN
                        tams_exam_schds exs
                ON
                        exs.class_cd=cls.cls_num
                INNER JOIN
                        TAMS_TRAINING_RSLTS rslt
                ON
                        rslt.can_num =reg.can_num
                AND     rslt.class_cd=cls.cls_num
                INNER JOIN
                        (
                                SELECT
                                        reg.can_num,
                                        exs.exam_dt,
                                        row_number() over (PARTITION BY
                                                           reg.can_num order by exs.exam_dt) rn
                                FROM
                                        tams_classes cls
                                INNER JOIN
                                        tams_class_attd_regs reg
                                ON
                                        reg.class_cd=cls.cls_num
                                INNER JOIN
                                        tams_exam_schds exs
                                ON
                                        cls.cls_num=exs.class_cd
                                INNER JOIN
                                        TAMS_TRAINING_RSLTS rslt
                                ON
                                        (
                                                cls.cls_num         =rslt.class_cd
                                        AND     rslt.can_num        =reg.can_num
                                        AND     rslt.training_cat_cd='EX') ) tmp
                ON
                        (
                                tmp.can_num = rslt.can_num
                        AND     exs.exam_dt = tmp.exam_dt
                        AND     tmp.rn      = 1)
                WHERE
                        rslt.training_cat_cd='EX' ) EX_cls
ON
        EX_cls.can_num=a.can_num
LEFT JOIN
        tfield_values fva
ON
        e.agent_group = fva.fld_valu
AND     fva.fld_nm    = 'AGENT_GROUP'
WHERE
        a.stat_cd IN ('01',
                      '99')             
""")


# COMMAND ----------

#spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

result.write.mode('overwrite').parquet(f'{out_path}CPD_ACTIVE_AGENTS/')
