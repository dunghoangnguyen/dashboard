# Databricks notebook source
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import pyspark.sql.functions as F
from datetime import timedelta
from operator import attrgetter

pd.set_option('display.float_format', lambda x: '%.2f' % x)
pd.set_option('display.max_rows', 200)
pd.set_option('display.max_columns', 200)

# COMMAND ----------

# Step 1. Modify the config and paths for upcoming month

# COMMAND ----------

#change filter date and tier if needed every month
max_date_str = '2023-10-31'
min_last_mth = '2023-09-30'
min_last_3m = '2023-07-31' # last 3 month
min_date_str = '2022-10-31' # only last year
max_date = pd.to_datetime(max_date_str)
mth_partition = max_date_str[:7]
current_year = max_date.year

# offline data, manual work for manuprolist and VN model output. please control the format to align with previous month
path_manupro = 'vn_mp_202310'
path_allowance = 'manu_pro_tracking_20231031'
path_mdrt = 'vn_mdrt_20230920'
manupro_level = 'Platinum'
snapshot = '202310' # used to filter lapse socre and persistency socre

# model output tables, please change the date
multiclass_path = '/dbfs/mnt/lab/vn/project/scratch/gen_rep_2023/prod_existing/11_multiclass_scored_base/multiclass_scored_' + snapshot + '.csv'
leads_model_path = '/dbfs/mnt/lab/vn/project/scratch/gen_rep_2023/prod_existing/8_model_score_existing/'
lapse_path = '/dbfs/mnt/lab/vn/project/lapse/pre_lapse_deployment/lapse_mthly/lapse_score.parquet/'

# pre_lapse_revamp_path = '/dbfs/mnt/lab/vn/project/lapse/pre_lapse_revamp_3/snapshots/snapshots_202309/'

# manupro intro link
mp_link_url = 'https://manulife-mba.axonify.com/training/index.html#hub/search/community-1535/articles/1'
#2024 MDRT requirment
ape_benchmark = dict({'MDRT': 721626600, 'COT': 2164879800, 'TOT': 4329759600, '^.^': 4329759600})

# COMMAND ----------

# load dependency model output
multiclass = pd.read_csv(multiclass_path)
leads_existing_model = pd.read_parquet(leads_model_path)
#revamp snapshot path: '/dbfs/mnt/lab/vn/project/lapse/pre_lapse_revamp_3/snapshots/snapshots_202309/'
lapse = pd.read_parquet(lapse_path)

# COMMAND ----------

# step 2. load PAR related data

# COMMAND ----------

query = '''
select * 
from {0} 
where level = '{1}';
'''.format(path_manupro, manupro_level)
mp = spark.sql(query).toPandas()

query = '''
select * 
from {0} 
;
'''.format(path_allowance)
mth_allowance = spark.sql(query).toPandas()

query = '''
select * 
from {0} 
;
'''.format(path_mdrt)
mdrt = spark.sql(query).toPandas()


path = '/dbfs/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/TAGTDM_DAILY'
tagtdm_daily = pd.read_parquet(path)
#tagtdm_daily = tagtdm_daily[tagtdm_daily['agt_code'].isin(mp['agt_cd'])]


path = '/dbfs/mnt/prod/Published/VN/Master/VN_PUBLISHED_AMS_DB/TAMS_AGENTS/AMS_TAMS_AGENTS.parquet'
tams_agents = pd.read_parquet(path)


path = '/dbfs/mnt/prod/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/AGENT_RFM/monthend_dt=' + mth_partition
agent_rfm = pd.read_parquet(path)


query = '''
select pol_num, cli_num, cvg_eff_dt, plan_code, cvg_prem, cvg_typ, cvg_reasn, occp_clas, par_code, bnft_dur, prem_dur, rel_to_insrd, smkr_code, sex_code, 
cvg_eff_age, cvg_stat_cd, cvg_del_dt, face_amt 
from hive_metastore.vn_published_cas_db.tcoverages 
where pol_num in (
    select pol_num from hive_metastore.vn_published_cas_db.tpolicys
    where wa_cd_1 in (
        select agt_cd from {0} 
        where level = '{1}'
    )
) or cvg_eff_dt > '{2}'
;
'''.format(path_manupro, manupro_level, min_last_mth)
tcoverages = spark.sql(query).toPandas()


query = '''
select pol_num, pol_app_dt, pol_eff_dt, sbmt_dt, pol_trmn_dt, pmt_mode, agt_code, wa_cd_1, wa_cd_2, plan_code_base, plan_prem, dist_chnl_cd, pol_stat_cd, pd_to_dt 
from hive_metastore.vn_published_cas_db.tpolicys
where wa_cd_1 in (
    select agt_cd from {0} 
    where level = '{1}'
) or pol_num in (
    select pol_num from hive_metastore.vn_published_cas_db.tcoverages 
    where cvg_eff_dt > '{2}'
)
;
'''.format(path_manupro, manupro_level, min_last_mth)
tpolicys = spark.sql(query).toPandas()


path = '/dbfs/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/TCLIENT_POLICY_LINKS/CAS_TCLIENT_POLICY_LINKS.parquet'
cus_pol_link = pd.read_parquet(path,columns = ['POL_NUM', 'CLI_NUM', 'LINK_TYP', 'REC_STATUS', 'REL_TO_INSRD'])
cus_pol_link = cus_pol_link[cus_pol_link['POL_NUM'].isin(tpolicys['pol_num'])]


path = '/dbfs/mnt/prod/Published/VN/Master/VN_PUBLISHED_AMS_DB/TAMS_CANDIDATES/AMS_TAMS_CANDIDATES.parquet'
tams_candidates = pd.read_parquet(path, columns = ['ID_NUM', 'CAN_NUM'])


path = '/dbfs/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/TCLIENT_DETAILS/CAS_TCLIENT_DETAILS.parquet'
tclient_details = pd.read_parquet(path, columns = ['ID_NUM', 'CLI_NUM', 'BIRTH_DT', 'SEX_CODE', 'CLI_NM'])


path = '/dbfs/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAMPAIGN_FILEBASED_DB/NBV_MARGIN_HISTORIES/NBV_MARGIN_HISTORIES.parquet'
margin = pd.read_parquet(path)


query = '''
select distinct 
  tclaim.clm_id
, tclaim.event_dt
, tclaim.clm_recv_dt
, tclaim.clm_aprov_dt
, tclaim.clm_stat_code
, tclaim.clm_reasn_cd

, tclaim.plan_code
, tclaim.clm_code
, tclaim.clm_aprov_amt
, case 
    when tclaim.clm_stat_code = 'A' then tclaim.clm_aprov_amt 
    else 0 
  end as adj_aprov_amt 
, tclaim.clm_prvd_amt
, tclaim.pol_num
, tclaim.cli_num
from hive_metastore.vn_published_cas_db.tclaim_details as tclaim
where tclaim.pol_num in (
        select pol_num from hive_metastore.vn_published_cas_db.tpolicys
    where wa_cd_1 in (
        select agt_cd from {0} 
        where level = '{1}'
    )
)
;
'''.format(path_manupro, manupro_level)
tclaim = spark.sql(query).toPandas()


path = '/dbfs/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/TPLANS/CAS_TPLANS.parquet'
sp_plan = pd.read_parquet(path, columns = ['PLAN_CODE', 'SNGL_PREM_IND'])
sp_plan = sp_plan[sp_plan['SNGL_PREM_IND'] == 'Y']['PLAN_CODE'].to_list()


query = '''
select agt_cd, acum_bal as m19_per
from hive_metastore.VN_PUBLISHED_AMS_BAK_DB.TAMS_AGT_ACUMS_BK
where agt_cd in (
    select agt_cd from {0} 
    where level = '{1}'
) and run_num = '{2}'
and acum_cd = 'A214'
;
'''.format(path_manupro, manupro_level, snapshot)
m19_per_vn = spark.sql(query).toPandas()

# COMMAND ----------

path = '/dbfs/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAMPAIGN_FILEBASED_DB/NBV_MARGIN_HISTORIES/NBV_MARGIN_HISTORIES.parquet'
margin = pd.read_parquet(path)
margin.columns = [i.lower() for i in margin.columns]
margin.to_csv('/dbfs/mnt/lab/vn/project/temp_luobinr/margin_20231121.csv', index = False)

# COMMAND ----------

# Step 3. processing for related tables

# COMMAND ----------

# column name issue
tams_agents.columns = [i.lower() for i in tams_agents.columns]
tams_candidates.columns = [i.lower() for i in tams_candidates.columns]
tclient_details.columns = [i.lower() for i in tclient_details.columns]
cus_pol_link.columns = [i.lower() for i in cus_pol_link.columns]
margin.columns = [i.lower() for i in margin.columns]

# addtional agents' demographics

agent_rfm = agent_rfm.sort_values(by = ['mar_stat_cd', 'ins_exp_ind']).reset_index(drop = True)
agent_rfm = agent_rfm.drop_duplicates(subset = ['agt_code'], keep = 'first').reset_index(drop = True)

mar_stat_cd = dict(zip(agent_rfm['agt_code'], agent_rfm['mar_stat_cd']))
ins_exp_ind = dict(zip(agent_rfm['agt_code'], agent_rfm['ins_exp_ind']))
del agent_rfm

tagtdm_daily = tagtdm_daily[~tagtdm_daily['agt_code'].isnull()].reset_index(drop = True)
tagtdm_daily['mar_stat_cd'] = tagtdm_daily['agt_code'].map(lambda x: mar_stat_cd[x] if x in mar_stat_cd else np.nan)
tagtdm_daily['ins_exp_ind'] = tagtdm_daily['agt_code'].map(lambda x: ins_exp_ind[x] if x in ins_exp_ind else np.nan)

tagtdm_daily = tagtdm_daily.rename(columns = {'agt_code': 'wa_cd_1', 'sex_code': 'sex_code_agt'})
tagtdm_daily['agt_join_dt'] = pd.to_datetime(tagtdm_daily['agt_join_dt'])
tagtdm_daily['agt_term_dt'] = pd.to_datetime(tagtdm_daily['agt_term_dt'])

# keep only agents from candidates
agt_from_can = list(set(tams_agents['can_num']) & set(tams_candidates['can_num']))
tams_can_agt = tams_candidates[tams_candidates['can_num'].isin(pd.Series(agt_from_can))].reset_index(drop = True)
del tams_candidates

# create id_num to tams_agents
agt_can_id = dict(zip(tams_can_agt['can_num'],tams_can_agt['id_num']))
tams_agents['id_num'] = tams_agents['can_num'].map(lambda x: agt_can_id[x])

id_num_agt_as_cus = list(set(tclient_details['id_num'].to_list()) & set(tams_can_agt['id_num'].to_list()))

# create dict of id_num from cli and add to tcoverage
tclient_agt = tclient_details[tclient_details['id_num'].isin(id_num_agt_as_cus)].reset_index(drop = True)
cli_id_num = dict(zip(tclient_agt['cli_num'], tclient_agt['id_num']))

# create dict of id_num from wa_agt and add to tcoverage
agt_id_num = dict(zip(tams_agents['agt_code'], tams_agents['id_num']))
del tams_agents

#only for agency channel
tagtdm_daily = tagtdm_daily[(tagtdm_daily['channel'] == 'Agency')&
                            (tagtdm_daily['comp_prvd_num'].isin(['01', '98']))&
                            (tagtdm_daily['agt_stat_code'] == '1')&
                            (tagtdm_daily['agt_term_dt'].isnull())]

# COMMAND ----------

# processing for policy tables
# load coverage data
tcoverages['cvg_eff_dt'] = pd.to_datetime(tcoverages['cvg_eff_dt'])
tcoverages['cli_id_num_agt_buy'] = tcoverages['cli_num'].map(lambda x: cli_id_num[x] if x in cli_id_num else np.nan)
tcoverages['cli_id_num_agt_buy'].notnull().sum()

# keep policy from agency channel only
tpolicys = tpolicys[tpolicys['wa_cd_1'].isin(tagtdm_daily['wa_cd_1'])].reset_index(drop = True)
tpolicys['pol_trmn_dt'] = pd.to_datetime(tpolicys['pol_trmn_dt'])
tpolicys['pol_eff_dt'] = pd.to_datetime(tpolicys['pol_eff_dt'])
tpolicys['term_pol_last_days'] = (tpolicys['pol_trmn_dt'].dt.date - tpolicys['pol_eff_dt'].dt.date).dt.days
tpolicys['agt_id_num_agt_buy'] = tpolicys['wa_cd_1'].map(lambda x: agt_id_num[x] if x in agt_id_num else np.nan)

# get pol link all
cus_pol_link.rename(columns = {'cli_num':'po_num'}, inplace = True)
cus_pol_link = cus_pol_link[(cus_pol_link['link_typ'] == 'O')&(cus_pol_link['rec_status'] == 'A')].reset_index(drop = True)
pol_po_dict = dict(zip(cus_pol_link['pol_num'], cus_pol_link['po_num']))

pol_num = pd.Series(list(set(tpolicys['pol_num'].to_list()) & set(tcoverages['pol_num'].to_list())))

tcoverages = tcoverages[tcoverages['pol_num'].isin(pol_num)]
tpolicys = tpolicys[tpolicys['pol_num'].isin(pol_num)]

pol = pd.merge(tcoverages, tpolicys, on = 'pol_num', how = 'left')
pol['APE'] = pol['cvg_prem']*12/pol['pmt_mode'].astype(int)

#combine to df
df = pd.merge(pol, tagtdm_daily, on = 'wa_cd_1', how = 'left')
del tpolicys
del tcoverages
del pol

# COMMAND ----------

# add related columns to df for further calculation
df['cvg_prem'] = df['cvg_prem'].astype(float)
#create first buy date and repeat buy flag
df['po_num'] = df['pol_num'].map(lambda x: pol_po_dict[x] if x in pol_po_dict else np.nan)

# adjust APE for first year termination
def adjust_ape(eff_days, pmt, prem, ape):
    if eff_days <= 21:
        ape = 0
    else:
        if (eff_days is np.nan)|(eff_days >= 365):
            ape = ape
        elif (eff_days < 365)&(pmt == 12):
            ape = ape
        elif (eff_days < 365)&(pmt == 3):
            ape = prem*(int(eff_days/92)+1)
        elif (eff_days < 365)&(pmt == 6):
            ape = prem*(int(eff_days/183)+1)
        elif (eff_days < 365)&(pmt == 1):
            ape = prem*(int(eff_days/31)+1)
    return ape

df['cvg_del_dt'] = pd.to_datetime(df['cvg_del_dt'])
df['min_trmn_dt'] = df[['pol_trmn_dt', 'cvg_del_dt']].min(axis=1)
df['pol_eff_days'] = (df['min_trmn_dt'] - df['pol_eff_dt']).dt.days
df['adjust_ape'] = list(zip(df['pol_eff_days'], df['pmt_mode'], df['cvg_prem'], df['APE']))
df['adjust_ape'] = df['adjust_ape'].map(lambda x: adjust_ape(x[0], x[1], x[2], x[3]))

# adjust APE for single premium
def adjust_sp_ape(eff_days, prem, adj_ape, sp):
    if sp == 0:
        ape = adj_ape
    else:
        if eff_days <= 21:
            ape = 0
        else: #(eff_days > 21)&(sp != 0):
            ape = prem*0.1
    return ape

# SP list from DB (to be auto updated)
# select plan_code, sngl_prem_ind from vn_published_cas_db.tplans where sngl_prem_ind = 'Y'
SP = ['SPU01','SPU02','SPU03','SPU04','UL004','UL005','SCL01','UL035','UL036','UL037']
df['sp_ind'] = df['plan_code'].isin(SP).astype(int)

df['adjust_ape'] = list(zip(df['pol_eff_days'], df['cvg_prem'], df['adjust_ape'], df['sp_ind']))
df['adjust_ape'] = df['adjust_ape'].map(lambda x: adjust_sp_ape(x[0], x[1], x[2], x[3]))

df['adjust_ape'] = df['adjust_ape'].astype(float)
df['adjust_ape'] = (df['adjust_ape']*1000).astype(int)

#prepare columns for KPI calculation
df['cvg_eff_yr'] = df['cvg_eff_dt'].dt.year
df['cvg_eff_yr_mth'] = df['cvg_eff_dt'].dt.to_period('M')
df['cvg_eff_mth_from_pol_eff'] = (df['cvg_eff_dt'].dt.to_period('M').astype(int) - 
                                    df['pol_eff_dt'].dt.to_period('M').astype(int)
                                    ) + 1
df['cvg_eff_mth_from_agt_join'] = (df['cvg_eff_dt'].dt.to_period('M').astype(int) -
                                    df['agt_join_dt'].dt.to_period('M').astype(int)
                                    ) + 1
df['pol_eff_mth_from_agt_join'] = (df['pol_eff_dt'].dt.to_period('M').astype(int) - 
                                    df['agt_join_dt'].dt.to_period('M').astype(int)
                                    ) + 1
df['pol_eff_days_from_agt_join'] = (df['pol_eff_dt'] - df['agt_join_dt']).dt.days
df = df[~df['po_num'].isnull()].reset_index(drop = True)
df['po_num'] = df['po_num'].astype(int)

stat_dict = dict({'1':'Premium Paying','2':'Premium Waiver','3':'Fully Paid','4':'Extended Term Ins.',
                   '5':'Reduced Paid-Up', '6':'Pending for premium', '7':'Support premium',
                   '8':'NB Pending', '9':'Support premium', 'A':'Not taken','B':'Lapsed',
                    'C':'Converted', 'D': 'Death Claimed', 'E':'Surrendered', 'F': 'Matured', 
                    'H':'Expired', 'L':'Deleted', 'M': 'MDR Claim', 'N': 'Not taken', 
                    'R': 'Rejected', 'T': 'TPD Claim', 'X': 'Closed'})

df['status'] = df['pol_stat_cd'].map(lambda x: stat_dict[x])

df['cvg_active'] = (df['cvg_stat_cd'].isin(['1','2','3','5','7'])).astype(int)
df['pol_active'] = (df['pol_stat_cd'].isin(['1','2','3','5','7'])).astype(int)

df['pol_eff_yr'] = df['pol_eff_dt'].dt.year

# first policy bought, use this one please
first_p = df.sort_values(by='pol_eff_dt').drop_duplicates(subset='po_num', keep='first')['pol_num']

df['if_first_pol'] = df['pol_num'].isin(first_p).astype(int)
df['repeat_sales'] = 1 - df['if_first_pol']

tclient_details['birth_dt'] = pd.to_datetime(tclient_details['birth_dt'])
tclient_details['age_this_year'] = current_year - tclient_details['birth_dt'].dt.year
tclient_details['is_female'] = (tclient_details['sex_code'] == 'F').astype(int)

age_this_year_dict = dict(zip(tclient_details['cli_num'], tclient_details['age_this_year']))
birth_dict = dict(zip(tclient_details['cli_num'], pd.to_datetime(tclient_details['birth_dt'])))
gender_dict = dict(zip(tclient_details['cli_num'], tclient_details['sex_code'].map(lambda x: str(x).upper())))
cli_num_name_dict = dict(zip(tclient_details['cli_num'], tclient_details['cli_nm']))
del tclient_details


df['po_age_this_year'] = df['po_num'].map(lambda x: age_this_year_dict[str(x)] if str(x) in age_this_year_dict else np.nan)
df['in_age_this_year'] = df['cli_num'].map(lambda x: age_this_year_dict[x] if x in age_this_year_dict else np.nan)

# COMMAND ----------

#some estimation of family relationship and nbv margin

# process relationship of beneficiary to insured
rel_dict = dict({2:'Spouse',
                 1:'Self',
                 0:'Others',
                 52:'Others',
                 51:'Parent',
                 3:'Child',
                 5:'Others',
                 4:'Others',
                 10:'Others',
                 31:'Others'
                })

df['insrd'] = df['rel_to_insrd'].astype(int).map(lambda x: rel_dict[x])

df['cvg_eff_yr'] = df['cvg_eff_dt'].dt.year

#process insured relationship
df['be_is_cli'] = (df['insrd'] == 'Self').astype(int)
# PO is cli
df['po_is_cli'] = (df['po_num'].astype(str) == df['cli_num']).astype(int)
df['po_cli_age_gap'] = list(zip(df['po_is_cli'], df['po_age_this_year'], df['in_age_this_year']))
df['po_cli_age_gap'] = df['po_cli_age_gap'].map(lambda x: np.nan if x[0] == 1 else x[1] - x[2])

df['po_gender'] = df['po_num'].map(lambda x: gender_dict[str(x)] if str(x) in gender_dict else np.nan)
df['ins_gender'] = df['cli_num'].map(lambda x: gender_dict[x] if x in gender_dict else np.nan)

def which_rela(a,b,c,d):
    if a == 1:
        y = 'self'
    else:
        if (b>=18)&(b<=40):
            y = 'child'
        else:
            if b > 40:
                y = 'others'
            else:
                if b < -17:
                    y = 'parent'
                else:
                    if c != d:
                        y = 'spouse'
                    else:
                        y = 'others'
    return y

df['po_ins_rela'] = list(zip(df['po_is_cli'], df['po_cli_age_gap'], df['po_gender'], df['ins_gender']))
df['po_ins_rela'] = df['po_ins_rela'].map(lambda x: which_rela(x[0],x[1],x[2],x[3]))

#prepare margin dict
margin = margin.drop_duplicates()

margin_last_year = margin.groupby(['plan_code','customer_needs']
               )['effective_date'].nunique().reset_index().sort_values(by = 'effective_date',ascending = False
                                                                      ).drop_duplicates(keep = 'first', 
                                                                                        subset = ['plan_code'])

margin_type_dict = dict(zip(margin_last_year['plan_code'], (margin_last_year['customer_needs'])))


margin['nbv_margin_agency'] = margin['nbv_margin_agency'].astype(float)
margin_nbv_dict = margin[margin['effective_qtr'].map(lambda x: x[:4]) == str(current_year - 1)
                        ].groupby(['plan_code'])['nbv_margin_agency'].mean().to_dict()

#prd type and margin
df['type'] = df['plan_code'].map(lambda x: margin_type_dict[x] if x in margin_type_dict else np.nan)

# nbv and margin
df['margin_nbv'] = df['plan_code'].map(lambda x: margin_nbv_dict[x] if x in margin_nbv_dict else np.nan)
df['nbv'] = df['adjust_ape'] * df['margin_nbv']

# prd cus mix
df['prd_cus_mix'] = df['type'] + '_' + df['po_ins_rela']

 # A/N:not-taken, B:lapsed, R:Rejected
df['is_non_taken'] = (df['pol_stat_cd'].isin(['A', 'N'])).astype(int)
df['lapsed'] = (df['pol_stat_cd'] == 'B').astype(int)
df['Rejected'] = (df['pol_stat_cd'] == 'R').astype(int)

df['cvg_eff_week_from_agt_join'] = ((df['cvg_eff_dt'].dt.date - 
                                     df['agt_join_dt'].dt.date).dt.days/7
                                   ).astype(int) + 1

#use july 2023 as the cut off
df['max_date'] = max_date

df = df[(df['max_date']>= df['cvg_eff_dt'])&(df['max_date']>= df['pol_eff_dt'])].reset_index(drop = True)

df['last_n_month_eff_cvg'] = (df['cvg_eff_dt'].dt.to_period('M').astype(int) - 
                              df['max_date'].dt.to_period('M').astype(int)
                             ) - 1
tier_dict = dict(zip(mdrt['Agt code'], mdrt[' Ranking ']))

df['tier'] = df['wa_cd_1'].map(lambda x: tier_dict[x] if x in tier_dict else 'Platinum')
df['year_month'] = df['cvg_eff_dt'].dt.to_period('M')

mp_level = dict(zip(mp['agt_cd'], mp['level']))
df['mp_level'] = df['wa_cd_1'].map(lambda x: mp_level[x] if x in mp_level else np.nan)

# COMMAND ----------

#prepare for other tables
#top_leads_needs.columns = [i.split('.')[1] for i in top_leads_needs.columns]

sub = df[['pol_num', 'wa_cd_1']].drop_duplicates()
pol_agt = dict(zip(sub['pol_num'], sub['wa_cd_1']))

sub = df[['cli_num', 'wa_cd_1']].drop_duplicates()
cli_agt = dict(zip(sub['cli_num'].astype(int), sub['wa_cd_1']))

sub = df[['po_num', 'wa_cd_1']].drop_duplicates()
po_agt = dict(zip(sub['po_num'], sub['wa_cd_1']))

# COMMAND ----------

# prepare agt table

tagtdm_daily['max_date'] = max_date
tagtdm_daily['tenure_mth'] = tagtdm_daily['max_date'].dt.to_period('M').astype(int) - tagtdm_daily['agt_join_dt'].dt.to_period('M').astype(int)
agt = tagtdm_daily[['wa_cd_1','agt_nm','rank_code', 'br_code', 'tenure_mth', 'agt_join_dt']]
agt = agt[agt['wa_cd_1'].isin(mp['agt_cd'])].reset_index(drop = True)
agt['tier'] = agt['wa_cd_1'].map(lambda x: tier_dict[x] if x in tier_dict else 'Platinum')

agt.tail(2)

# COMMAND ----------

#prepare aggregate level KPIs

#ape
f1 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] == -1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['adjust_ape'].sum().fillna(0)
f1.name = 'last_mth_ape'

f2 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -12)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['adjust_ape'].sum().fillna(0)
f2.name = 'last_yr_ape'

f3 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -3)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['adjust_ape'].sum().fillna(0)
f3.name = 'last_3m_ape'

f4 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -6)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['adjust_ape'].sum().fillna(0)
f4.name = 'last_6m_ape'

#nbv
f5 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] == -1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['nbv'].sum().fillna(0)
f5.name = 'last_mth_nbv'

f6 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -12)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['nbv'].sum().fillna(0)
f6.name = 'last_yr_nbv'

f7 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -3)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['nbv'].sum().fillna(0)
f7.name = 'last_3m_nbv'

f8 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -6)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['nbv'].sum().fillna(0)
f8.name = 'last_6m_nbv'

#po
f9 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] == -1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['po_num'].nunique().fillna(0)
f9.name = 'last_mth_po'

f10 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -12)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['po_num'].nunique().fillna(0)
f10.name = 'last_yr_po'

f11 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -3)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['po_num'].nunique().fillna(0)
f11.name = 'last_3m_po'

f12 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -6)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['po_num'].nunique().fillna(0)
f12.name = 'last_6m_po'

#cus
f13 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] == -1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['cli_num'].nunique().fillna(0)
f13.name = 'last_mth_cus'

f14 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -12)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['cli_num'].nunique().fillna(0)
f14.name = 'last_yr_cus'

f15 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -3)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['cli_num'].nunique().fillna(0)
f15.name = 'last_3m_cus'

f16 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -6)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['cli_num'].nunique().fillna(0)
f16.name = 'last_6m_cus'

#pol
f17 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] == -1)&
        (df['cvg_typ'] == 'B')&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['pol_num'].nunique().fillna(0)
f17.name = 'last_mth_pol'

f18 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -12)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (df['cvg_typ'] == 'B')&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['pol_num'].nunique().fillna(0)
f18.name = 'last_yr_pol'

f19 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -3)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (df['cvg_typ'] == 'B')&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['pol_num'].nunique().fillna(0)
f19.name = 'last_3m_pol'

f20 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -6)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (df['cvg_typ'] == 'B')&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['pol_num'].nunique().fillna(0)
f20.name = 'last_6m_pol'

#not taken pol
f21 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] == -1)&
        (df['cvg_typ'] == 'B')&
        (df['is_non_taken'] == 1)
       ].groupby(['wa_cd_1'])['pol_num'].nunique().fillna(0)
f21.name = 'last_mth_NT'

f22 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -12)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (df['cvg_typ'] == 'B')&
        (df['is_non_taken'] == 1)
       ].groupby(['wa_cd_1'])['pol_num'].nunique().fillna(0)
f22.name = 'last_yr_NT'

f23 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -3)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (df['cvg_typ'] == 'B')&
        (df['is_non_taken'] == 1)
       ].groupby(['wa_cd_1'])['pol_num'].nunique().fillna(0)
f23.name = 'last_3m_NT'

f24 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -6)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (df['cvg_typ'] == 'B')&
        (df['is_non_taken'] == 1)
       ].groupby(['wa_cd_1'])['pol_num'].nunique().fillna(0)
f24.name = 'last_6m_NT'

#Rejected pol
f25 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] == -1)&
        (df['cvg_typ'] == 'B')&
        (df['Rejected'] == 1)
       ].groupby(['wa_cd_1'])['pol_num'].nunique().fillna(0)
f25.name = 'last_mth_rej'

f26 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -12)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (df['cvg_typ'] == 'B')&
        (df['Rejected'] == 1)
       ].groupby(['wa_cd_1'])['pol_num'].nunique().fillna(0)
f26.name = 'last_yr_rej'

f27 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -3)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (df['cvg_typ'] == 'B')&
        (df['Rejected'] == 1)
       ].groupby(['wa_cd_1'])['pol_num'].nunique().fillna(0)
f27.name = 'last_3m_rej'

f28 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -6)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (df['cvg_typ'] == 'B')&
        (df['Rejected'] == 1)
       ].groupby(['wa_cd_1'])['pol_num'].nunique().fillna(0)
f28.name = 'last_6m_rej'

#lapse
f29 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['cvg_typ'] == 'B')&
        (df['pol_trmn_dt'].dt.to_period('M') == df['max_date'].dt.to_period('M'))&
        (df['status'] == 'Lapsed')
       ].groupby(['wa_cd_1'])['pol_num'].nunique().fillna(0)
f29.name = 'last_mth_lap'

f30 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['cvg_typ'] == 'B')&
        (df['pol_trmn_dt'].dt.to_period('M') <= df['max_date'].dt.to_period('M'))&
        (df['pol_trmn_dt'].dt.to_period('M') > df['max_date'].dt.to_period('M') - 12)&
        (df['status'] == 'Lapsed')
       ].groupby(['wa_cd_1'])['pol_num'].nunique().fillna(0)
f30.name = 'last_yr_lap'

f31 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['cvg_typ'] == 'B')&
        (df['pol_trmn_dt'].dt.to_period('M') <= df['max_date'].dt.to_period('M'))&
        (df['pol_trmn_dt'].dt.to_period('M') > df['max_date'].dt.to_period('M') - 3)&
        (df['status'] == 'Lapsed')
       ].groupby(['wa_cd_1'])['pol_num'].nunique().fillna(0)
f31.name = 'last_3m_lap'

f32 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['cvg_typ'] == 'B')&
        (df['pol_trmn_dt'].dt.to_period('M') <= df['max_date'].dt.to_period('M'))&
        (df['pol_trmn_dt'].dt.to_period('M') > df['max_date'].dt.to_period('M') - 6)&
        (df['status'] == 'Lapsed')
       ].groupby(['wa_cd_1'])['pol_num'].nunique().fillna(0)
f32.name = 'last_6m_lap'

#prd_type
f33 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] == -1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['type'].nunique().fillna(0)
f33.name = 'last_mth_prd'

f34 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -12)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['type'].nunique().fillna(0)
f34.name = 'last_yr_prd'

f35 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -3)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['type'].nunique().fillna(0)
f35.name = 'last_3m_prd'

f36 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -6)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['type'].nunique().fillna(0)
f36.name = 'last_6m_prd'

#family member
f37 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] == -1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['po_ins_rela'].nunique().fillna(0)
f37.name = 'last_mth_fam'

f38 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -12)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['po_ins_rela'].nunique().fillna(0)
f38.name = 'last_yr_fam'

f39 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -3)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['po_ins_rela'].nunique().fillna(0)
f39.name = 'last_3m_fam'

f40 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -6)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['po_ins_rela'].nunique().fillna(0)
f40.name = 'last_6m_fam'

#rider_ape
f41 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] == -1)&
        (df['cvg_typ'] == 'R')&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['adjust_ape'].sum().fillna(0)
f41.name = 'last_mth_rid'

f42 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -12)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (df['cvg_typ'] == 'R')&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['adjust_ape'].sum().fillna(0)
f42.name = 'last_yr_rid'

f43 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -3)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (df['cvg_typ'] == 'R')&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['adjust_ape'].sum().fillna(0)
f43.name = 'last_3m_rid'

f44 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -6)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (df['cvg_typ'] == 'R')&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['adjust_ape'].sum().fillna(0)
f44.name = 'last_6m_rid'

#repeat_ape
f45 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] == -1)&
        (df['repeat_sales'] == 1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['adjust_ape'].sum().fillna(0)
f45.name = 'last_mth_rep'

f46 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -12)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (df['repeat_sales'] == 1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['adjust_ape'].sum().fillna(0)
f46.name = 'last_yr_rep'

f47 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -3)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (df['repeat_sales'] == 1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['adjust_ape'].sum().fillna(0)
f47.name = 'last_3m_rep'

f48 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['last_n_month_eff_cvg'] >= -6)&
        (df['last_n_month_eff_cvg'] <= -1)&
        (df['repeat_sales'] == 1)&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['adjust_ape'].sum().fillna(0)
f48.name = 'last_6m_rep'


#avg_pol_size
f49 = f1/f17
f49.name = 'last_mth_pol_size'

f50 = f2/f18
f50.name = 'last_yr_pol_size'

f51 = f3/f19
f51.name = 'last_3m_pol_size'

f52 = f4/f20
f52.name = 'last_6m_pol_size'

#avg_po_size
f53 = f1/f9
f53.name = 'last_mth_po_size'

f54 = f2/f10
f54.name = 'last_yr_po_size'

f55 = f3/f11
f55.name = 'last_3m_po_size'

f56 = f4/f12
f56.name = 'last_6m_po_size'

f57 = (1 - f21/f19).fillna(1)
f57.name = 'last_mth_NT_per'

f58 = (1 - f22/f19).fillna(1)
f58.name = 'last_yr_NT_per'

f59 = (1 - f23/f19).fillna(1)
f59.name = 'last_3m_NT_per'

f60 = (1 - f24/f19).fillna(1)
f60.name = 'last_6m_NT_per'

f61 = (1 - f25/f19).fillna(1)
f61.name = 'last_mth_rej_per'

f62 = (1 - f26/f19).fillna(1)
f62.name = 'last_yr_rej_per'

f63 = (1 - f27/f19).fillna(1)
f63.name = 'last_3m_rej_per'

f64 = (1 - f28/f19).fillna(1)
f64.name = 'last_6m_rej_per'

f65 = (1 - f29/f19).fillna(1)
f65.name = 'last_mth_lap_per'

f66 = (1 - f30/f19).fillna(1)
f66.name = 'last_yr_lap_per'

f67 = (1 - f31/f19).fillna(1)
f67.name = 'last_3m_lap_per'

f68 = (1 - f32/f19).fillna(1)
f68.name = 'last_6m_lap_per'

f = pd.concat([f1,
f2,
f3,
f4,
f5,
f6,
f7,
f8,
f9,
f10,
f11,
f12,
f13,
f14,
f15,
f16,
f17,
f18,
f19,
f20,
f21,
f22,
f23,
f24,
f25,
f26,
f27,
f28,
f29,
f30,
f31,
f32,
f33,
f34,
f35,
f36,
f37,
f38,
f39,
f40,
f41,
f42,
f43,
f44,
f45,
f46,
f47,
f48,
f49,
f50,
f51,
f52,
f53,
f54,
f55,
f56,
f57,
f58,
f59,
f60,
f61,
f62,
f63,
f64,
f65,
f66,
f67,
f68
],axis =1)

# COMMAND ----------

# step 4. Validate and double check (WIP)

# COMMAND ----------

print(f1.sum())
print(f2.sum())
print(f3.sum())

# COMMAND ----------

print(f1.sum())
print(f2.sum())
print(f3.sum())

# COMMAND ----------

df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
               (df['last_n_month_eff_cvg'] >= -12)&
               (df['last_n_month_eff_cvg'] <= -1)&
                (~df['pol_stat_cd'].isin(['A','N','R']))
               ]['cvg_eff_dt'].max()

# COMMAND ----------

# Step 5. generate KPIs for output tables

# COMMAND ----------

# preprare benchmark KPIs of monthly ape and prd mix
avg_sales = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
               (df['last_n_month_eff_cvg'] >= -12)&
               (df['last_n_month_eff_cvg'] <= -1)&
                (~df['pol_stat_cd'].isin(['A','N','R']))
               ].groupby(['wa_cd_1','year_month'])['adjust_ape'].sum().unstack().fillna(0).astype(int).stack().reset_index().rename(columns= {0:'ape'})
avg_sales = avg_sales.groupby(['year_month']).mean().astype(int).reset_index()
avg_sales.columns = ['date', 'avg_ape_tier']
avg_sales['current_tier'] = 'Platinum'

mth_sales = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
               (df['last_n_month_eff_cvg'] >= -12)&
               (df['last_n_month_eff_cvg'] <= -1)&
                (~df['pol_stat_cd'].isin(['A','N','R']))
               ].groupby(['wa_cd_1','year_month'])['adjust_ape'].sum().unstack().fillna(0).astype(int).stack().reset_index()
mth_sales.columns = ['agt_cd', 'date', 'avg_ape_agt']
mth_sales['current_tier'] = 'Platinum'
mth_sales = pd.merge(mth_sales, avg_sales, on = ['current_tier','date'], how = 'left')

avg_prd = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
               (df['last_n_month_eff_cvg'] >= -12)&
               (df['last_n_month_eff_cvg'] <= -1)&
               (~df['pol_stat_cd'].isin(['A','N','R']))
               ].groupby(['type'])['adjust_ape'].sum().fillna(0).astype(int).reset_index()
avg_prd.columns = ['prd_type', 'sum_ape_tier']
avg_prd['current_tier'] = 'Platinum'

mth_prd = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
               (df['last_n_month_eff_cvg'] >= -12)&
               (df['last_n_month_eff_cvg'] <= -1)&
              (~df['pol_stat_cd'].isin(['A','N','R']))
               ].groupby(['wa_cd_1','type'])['adjust_ape'].sum().unstack().fillna(0).astype(int).stack().reset_index()
mth_prd.columns = ['agt_cd', 'prd_type', 'sum_ape_agt']
mth_prd['current_tier'] = 'Platinum'
mth_prd = pd.merge(mth_prd, avg_prd, on = ['current_tier','prd_type'], how = 'left')

# COMMAND ----------

# prepare claim KPIs
tclaim['agt_cd'] = tclaim['pol_num'].map(lambda x: pol_agt[x] if x in pol_agt else np.nan)
tclaim['clm_aprov_amt'] = tclaim['clm_aprov_amt']*1000
tclaim['adj_aprov_amt'] = tclaim['adj_aprov_amt']*1000
tclaim['clm_prvd_amt'] = tclaim['clm_prvd_amt']*1000

y3 = tclaim[(tclaim['clm_recv_dt'] <= max_date)
            &(tclaim['clm_recv_dt'] >= max_date - pd.offsets.DateOffset(years=3))
            ].groupby(['agt_cd'])['clm_id'].nunique()

y3.name = 'claim_cnt_last_3_yr'

y3_amt = tclaim[(tclaim['clm_recv_dt'] <= max_date)
                &(tclaim['clm_recv_dt'] >= max_date - pd.offsets.DateOffset(years=3))
               ].groupby(['agt_cd'])['clm_prvd_amt'].sum()

y3_amt.name = 'claim_amt_last_3_yr'

y3_appr = tclaim[(tclaim['clm_recv_dt'] <= max_date)
                &(tclaim['clm_recv_dt'] >= max_date - pd.offsets.DateOffset(years=3))
                &(tclaim['clm_stat_code'] == 'A')
               ].groupby(['agt_cd'])['clm_id'].nunique()

y3_appr.name = 'claim_appr_cnt_last_3_yr'

y3_appr_amt = tclaim[(tclaim['clm_recv_dt'] <= max_date)
                   &(tclaim['clm_recv_dt'] >= max_date - pd.offsets.DateOffset(years=3))
                    &(tclaim['clm_stat_code'] == 'A')
                ].groupby(['agt_cd'])['clm_aprov_amt'].sum()

y3_appr_amt.name = 'claim_appr_amt_last_3_yr'

y1_wip = tclaim[(tclaim['clm_recv_dt'] <= max_date)
                &(tclaim['clm_recv_dt'] >= max_date - pd.offsets.DateOffset(years=1))
                &(tclaim['clm_stat_code'] == 'I')
               ].groupby(['agt_cd'])['clm_id'].nunique()

y1_wip.name = 'claim_wip_last_1_yr'

y3_appr_rate = y3_appr/y3
y3_appr_rate.name = 'claim_appr_rate_last_3_yr'

clm = pd.concat([y3, y3_amt, y3_appr, y3_appr_amt, y1_wip], axis = 1).fillna(0).astype(int)
clm['claim_appr_rate_last_3_yr'] = clm['claim_appr_cnt_last_3_yr']/clm['claim_cnt_last_3_yr']

clm['chart'] = '3yr_claim'
clm['chart_kpi'] = 'pol_cnt_or_amt'

clm = clm.reset_index()
clm.tail(2)

# COMMAND ----------

#prepare persistency KPIs
pol_cnt = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['cvg_typ'] == 'B')&
        (~df['pol_stat_cd'].isin(['A','N','R']))
       ].groupby(['wa_cd_1'])['pol_num'].nunique()

pol_cnt.name = 'all_pol_cnt'

lap14 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['cvg_typ'] == 'B')&
        (df['pol_trmn_dt'].dt.to_period('M') <= df['pol_eff_dt'].dt.to_period('M') + 14)&
        (df['pol_trmn_dt'].dt.to_period('M') >= df['pol_eff_dt'].dt.to_period('M'))&
        (df['status'] == 'Lapsed')
       ].groupby(['wa_cd_1'])['pol_num'].nunique()

lap14.name = 'lap_14m_pol'

lap26 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['cvg_typ'] == 'B')&
        (df['pol_trmn_dt'].dt.to_period('M') <= df['pol_eff_dt'].dt.to_period('M') + 26)&
        (df['pol_trmn_dt'].dt.to_period('M') >= df['pol_eff_dt'].dt.to_period('M'))&
        (df['status'] == 'Lapsed')
       ].groupby(['wa_cd_1'])['pol_num'].nunique()
       
lap26.name = 'lap_26m_pol'

lap20 = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
        (df['cvg_typ'] == 'B')&
        (df['pol_trmn_dt'].dt.to_period('M') <= df['pol_eff_dt'].dt.to_period('M') + 20)&
        (df['pol_trmn_dt'].dt.to_period('M') >= df['pol_eff_dt'].dt.to_period('M'))&
        (df['status'] == 'Lapsed')
       ].groupby(['wa_cd_1'])['pol_num'].nunique()
       
lap20.name = 'lap_20m_pol'

last_yr_pol = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
                (df['last_n_month_eff_cvg'] >= -12)&
                (df['last_n_month_eff_cvg'] <= -1)&
                (df['cvg_typ'] == 'B')
                ].groupby(['wa_cd_1'])['pol_num'].nunique().fillna(0)
last_yr_pol.name = 'last_yr_pol'

per = pd.concat([pol_cnt, lap14, lap26, lap20, last_yr_pol, f22], axis = 1).fillna(0).astype(int)

per['14m_per'] = 1 - per['lap_14m_pol'] / per['all_pol_cnt']
per['20m_per'] = 1 - per['lap_20m_pol'] / per['all_pol_cnt']
per['26m_per'] = 1 - per['lap_26m_pol'] / per['all_pol_cnt']
per['12M_NT_rate'] = (per['last_yr_NT'] / per['last_yr_pol']).fillna(0)
del per['last_yr_pol']
del per['last_yr_NT']
per['chart'] = 'lapse_kpi'
per['chart_kpi'] = 'pol_cnt'

per = per.reset_index().rename(columns = {'wa_cd_1':'agt_cd'})

persis_raw_dict = dict(zip(m19_per_vn['agt_cd'], m19_per_vn['m19_per'].astype(int)/100))
per['19m_per_from_VN'] = per['agt_cd'].map(lambda x: persis_raw_dict[x])

per.tail(2)

# COMMAND ----------

#from lapse model > filter by snapshot please
lapse['agt_cd'] = lapse['pol_num'].map(lambda x: pol_agt[x] if x in pol_agt else np.nan)
lapse = lapse[(~lapse['agt_cd'].isnull())&(lapse['month_snapshot'] == snapshot)].reset_index(drop = True)

pol_eff_dict = dict(zip(df['pol_num'], df['pol_eff_dt']))
pol_po_dict = dict(zip(df['pol_num'], df['po_num']))
pd_to_dict = dict(zip(df['pol_num'], df['pd_to_dt']))
pol_prd_dict = dict(zip(df[df['cvg_typ'] == 'B']['pol_num'], df[df['cvg_typ'] == 'B']['type']))

pmt_mode_map = dict({1:'monthly', 3:'quarterly', 6:'half-yearly', 12:'yearly'})
df['pmt_mode_map'] = df['pmt_mode'].astype(int).map(lambda x: pmt_mode_map[x])
pol_pmt_dict = dict(zip(df['pol_num'], df['pmt_mode_map']))

lapse['pol_eff_dt'] = lapse['pol_num'].map(lambda x: pol_eff_dict[x] if x in pol_eff_dict else np.nan)
lapse['po_num'] = lapse['pol_num'].map(lambda x: pol_po_dict[x] if x in pol_po_dict else np.nan)
lapse['type'] = lapse['pol_num'].map(lambda x: pol_prd_dict[x] if x in pol_prd_dict else np.nan)
lapse['pmt_mode'] = lapse['pol_num'].map(lambda x: pol_pmt_dict[x] if x in pol_pmt_dict else np.nan)
lapse['cli_nm'] = lapse['po_num'].map(lambda x: cli_num_name_dict[str(x)] if str(x) in cli_num_name_dict else np.nan)
lapse['pd_to_dt'] = lapse['pol_num'].map(lambda x: pd_to_dict[x] if x in pd_to_dict else np.nan)

lps = lapse[(lapse['pd_to_dt'].dt.year == int(max_date_str[:4]))&(lapse['lapse_score']>=0.05)] 
lps = lps.sort_values('lapse_score',ascending = False).groupby('agt_cd').head(10)
lps = lps[['agt_cd','pol_num','po_num','cli_nm','pmt_mode','lapse_score','pol_eff_dt','pd_to_dt']]
lps.tail(2)

# COMMAND ----------

lapse_diag = lps.sort_values('lapse_score',ascending = False).groupby('agt_cd').head(3)
lapse_diag = lapse_diag.groupby(['agt_cd'])['po_num'].apply(list).reset_index()
lapse_diag['po_num'] = lapse_diag['po_num'].map(lambda x: list(set(x)))
lapse_diag.head(2)

# COMMAND ----------

# leads models output
leads_existing_model = leads_existing_model[leads_existing_model['image_date'] == max_date_str]
leads_existing_model['po_num'] = leads_existing_model['po_num'].astype(int)
leads_existing_model['agt_cd'] = leads_existing_model['po_num'].map(lambda x: po_agt[x] if x in po_agt else np.nan)
leads_existing_model = leads_existing_model[leads_existing_model['agt_cd'].isin(agt['wa_cd_1'])]
leads_existing_model = leads_existing_model.sort_values('p_1',ascending = False).groupby('agt_cd').head(10)

multiclass.columns = [i.replace('rep_purchase_comb_','') if 'rep_purchase_comb_' in i else i for i in multiclass.columns]
multiclass.columns = [i.replace('_PREDICTION','') if '_PREDICTION' in i else i for i in multiclass.columns]
multiclass = multiclass[multiclass['DEPLOYMENT_APPROVAL_STATUS']=='APPROVED']

multiclass['which_prd'] = multiclass[['health_base','inv_base','health_rider','riders']].idxmax(axis=1)
multiclass['inv_base'] = multiclass['which_prd'].map(lambda x: '*' if x == 'inv_base' else '')
#use 0.1 as cut off as the model use softmax as activation function
multiclass['health_base'] = multiclass['health_base'].map(lambda x: '*' if x > 0.1 else '')
multiclass['riders'] = multiclass['riders'].map(lambda x: '*' if x > 0.1 else '')
multiclass['health_rider'] = multiclass['health_rider'].map(lambda x: '*' if x > 0.1 else '')
multiclass.head(2)

nm = pd.merge(leads_existing_model, multiclass, on = 'po_num', how = 'left')
nm = nm[~nm['which_prd'].isnull()]
nm = nm.sort_values('p_1',ascending = False).groupby('agt_cd').head(10)
nm = nm[['agt_cd','po_num','health_base','inv_base','health_rider','riders','p_1']]
nm.head(2)

# COMMAND ----------

nm_diag = nm.sort_values('p_1',ascending = False).groupby('agt_cd').head(3)
nm_diag = nm_diag.groupby(['agt_cd'])['po_num'].apply(list).reset_index()
nm_diag = nm_diag.rename(columns = {'po_num':'cli_num'})
nm_diag.head(2)

# COMMAND ----------

mth_allowance.columns = [i.strip() for i in mth_allowance.columns]
#mth_allowance['monthly_allowance'] = mth_allowance['monthly_allowance'].map(lambda x: int(x.replace(',', ''))*1000)
mth_allowance['monthly_allowance'] = mth_allowance['monthly_allowance'].fillna(0).map(lambda x: int(x)*1000)
mth_allowance['qualify_date'] = pd.to_datetime(mth_allowance['qualify_date'], format='%Y%m').dt.to_period('M')

mth_allowance_dict = dict(zip(mth_allowance['agt_cd'], mth_allowance['monthly_allowance']))
qualify_date_dict = dict(zip(mth_allowance['agt_cd'], mth_allowance['qualify_date']))
agt['monthly_allowance'] = agt['wa_cd_1'].map(lambda x: mth_allowance_dict[x] if x in mth_allowance_dict else np.nan)
agt['qualify_date'] = agt['wa_cd_1'].map(lambda x: qualify_date_dict[x] if x in qualify_date_dict else pd.NaT)

agt['monthly_allowance'] = agt['monthly_allowance'].fillna(0)
agt['qualify_date'] = agt['qualify_date'].fillna(agt['qualify_date'].max())
agt['next_qualify_check'] = agt['qualify_date'] + 11
agt['qualify_date'] = agt['qualify_date'].astype(str)
agt['next_qualify_check'] = agt['next_qualify_check'].astype(str)
agt.head(2)

# COMMAND ----------

#page 1 KPIs
page1 = agt.rename(columns = {'wa_cd_1':'agt_cd'})
page1['report_month'] = mth_partition
page1['qualify_tenure'] = (pd.to_datetime(page1['report_month']).dt.to_period('M').astype(int) - 
                           pd.to_datetime(page1['qualify_date']).dt.to_period('M').astype(int)) + 1
next_tier = dict({'Platinum': 'MDRT', 'MDRT': 'COT', 'COT': 'TOT', 'TOT': 'TOT'})
ape_all_agt = df[df['year_month'].astype(str) == mth_partition].groupby(['wa_cd_1'])['adjust_ape'].sum()
last_mth_ape_dict = ape_all_agt.to_dict()
last_mth_ape_rank_dict = ape_all_agt.rank(ascending=False).astype(int).to_dict()
page1['last_mth_ape'] = page1['agt_cd'].map(lambda x: last_mth_ape_dict[x] if x in last_mth_ape_dict else 0)
page1['rank_of_last_mth_ape'] = page1['agt_cd'].map(lambda x: last_mth_ape_rank_dict[x] if x in last_mth_ape_rank_dict else np.nan)
page1['next_tier'] = page1['tier'].map(lambda x: next_tier[x])
page1['next_tier_benchmark'] = page1['next_tier'].map(lambda x: ape_benchmark[x])
page1['tenure_ym'] = page1['tenure_mth'].map(lambda x: str(divmod(x, 12)[0]) + ' y ' + str(divmod(x, 12)[1]) + ' m')
page1['monthly_allowance_rank'] = (agt.shape[0] - page1['monthly_allowance'].rank(axis=0, ascending=False).astype(int))/agt.shape[0]
page1['monthly_allowance_rank'] = page1['monthly_allowance_rank'].map(lambda x: 0.99 if x >= 0.99 else x)
rank_min_allowance = page1['monthly_allowance_rank'].min()
page1['monthly_allowance_rank'] = page1['monthly_allowance_rank'].map(lambda x: 0 if x == rank_min_allowance else x)
page1.head(2)

# COMMAND ----------

#page 2
page2 = pd.concat([f.fillna(0)[['last_3m_pol','last_3m_ape','last_3m_lap','last_3m_rep','last_3m_rid','last_3m_po']], 
                   f[['last_3m_pol_size','last_3m_po_size']]], axis = 1).reset_index()

page2 = page2.rename(columns = {'wa_cd_1':'agt_cd'})
page2['current_tier'] = page2['agt_cd'].map(lambda x: tier_dict[x] if x in tier_dict else 'Platinum')
page2['tier'] = page2['agt_cd'].map(lambda x: tier_dict[x] if x in tier_dict else 'Platinum')
page2['next_tier'] = page2['tier'].map(lambda x: next_tier[x])

p2_avg = page2.set_index('agt_cd').groupby(['tier']).mean().unstack().reset_index().groupby(['tier','level_0'])[0].max().unstack()
p2_avg['last_3m_po_size'] = (page2.set_index('agt_cd').groupby(['tier'])['last_3m_ape'].sum())/(page2.set_index('agt_cd').groupby(['tier'])['last_3m_po'].sum())
p2_avg['last_3m_pol_size'] = ((page2.set_index('agt_cd').groupby(['tier'])['last_3m_ape'].sum() - page2.set_index('agt_cd').groupby(['tier'])['last_3m_rid'].sum())
                              /(page2.set_index('agt_cd').groupby(['tier'])['last_3m_po'].sum()))

p2_current = p2_avg.copy()
p2_current.columns = ['current_'+i for i in p2_current.columns]
p2_current = p2_current.reset_index().rename(columns = {'tier': 'current_tier'})
p2_next = p2_avg.copy()
p2_next.columns = ['next_'+i for i in p2_next.columns]
p2_next = p2_next.reset_index().rename(columns = {'tier': 'next_tier'})

page2['Platinum'] = 'Platinum'
p2_plati = page2.set_index('agt_cd').groupby(['Platinum']).mean().unstack().reset_index().groupby(['Platinum','level_0'])[0].max().unstack()
p2_plati['last_3m_po_size'] = (page2.set_index('agt_cd').groupby(['Platinum'])['last_3m_ape'].sum())/(page2.set_index('agt_cd').groupby(['Platinum'])['last_3m_po'].sum())
p2_plati['last_3m_pol_size'] = ((page2.set_index('agt_cd').groupby(['Platinum'])['last_3m_ape'].sum() - 
                                 page2.set_index('agt_cd').groupby(['Platinum'])['last_3m_rid'].sum())
                              /(page2.set_index('agt_cd').groupby(['Platinum'])['last_3m_po'].sum()))
p2_plati.columns = ['platinum_'+i for i in p2_plati.columns]
p2_plati = p2_plati.reset_index()

page2_all = pd.merge(page2, p2_current, on = 'current_tier', how = 'left' )
page2_all = pd.merge(page2_all, p2_next, on = 'next_tier', how = 'left' )
page2_all = pd.merge(page2_all, p2_plati, on = 'Platinum', how = 'left' )
page2_all

# COMMAND ----------

#prepare for diagnosing
avg_rid_per = f43.sum()/f3.sum()
avg_rep_per = f47.sum()/f3.sum()
avg_case_size = f51.mean()
avg_fami_mbr = f39.median()
avg_14m_per = per['14m_per'].median()

def diagnose_recommend(x, avg_rid_per, avg_rep_per, avg_case_size, avg_fami_mbr, avg_14m_per):
    out = []
    if x[6]/3 >= 600000000/12:
        out.append("Congratulations!") 
        out.append("You are one of Manulife's top Manulife Pro Platinum Agents - this means you are the best of the best!")
        out.append("You are well on track to requalify for Platinum status at the next assessment date on " + x[9] + ".")
        out.append("Keep up the momentum to get more Manulife Pro Monthly Allowance each month! Here are some further customer sales opportunities for you to take action.")
    elif (x[6]/3 > 0)&(x[7] > 0):
        out.append("Your sales performance in the last 3 months seems to be below your potential - but don't worry, we are here to support you! Here are some ideas:")
        if (x[0] <= avg_rid_per - 0.1)&(len(out) < 4):
            out.append("Sell more riders! You sold " + str(int(x[0]*100)) + "% riders, vs " + str(int(avg_rid_per*100)) + "% for other Platinum agents.")
        if (x[1] <= avg_rep_per - 0.1)&(len(out) < 4):
            out.append("Cross sell to your existing customers! You sold " + str(int(x[1]*100)) + "% to existing customers, vs " + str(int(avg_rep_per*100)) + "% for other Platinum agents.")
        if (x[3] <= avg_case_size*0.7)&(len(out) < 4):
            out.append("Consider to grow your HNW customer base! Your average case size is " + str(int(x[3]/100000)/10) + "M, vs " + str(int(avg_case_size/1000000)) + "M for other Platinum agents.")
        if (x[4] < avg_fami_mbr)&(len(out) < 4):
            out.append("Try to sell to your customers family members! You sold to " + str(int(x[4])) + " types of family members, vs " + str(int(avg_fami_mbr)) + " for other Platinum agents.")
        if (x[2] <= avg_14m_per - 0.1)&(len(out) < 4):
            out.append("Improve your sales quality! " + str(int((1-x[2])*100)) + "% of your policy lapsed after first year, vs " + str(int((1-avg_14m_per)*100)) + "% for other Platinum agents.")
        if len(out) < 4:
            out.append("Bring this report to discuss with your sales manager or other mentors to help you get back in the game.")
        if len(out) < 4:
            out.append("We also have a lot of practical learning and development materials on Manuacademy for you to explore!")
        if len(out) < 4:
            out.append("We look forward to seeing you bounce back!")
    else:
        out.append("Hi " + x[10] + ", where have you been? We've missed you here at Manulife and we would love to see you back!") 
        out.append("As a Manulife Pro Platinum Agent, you are recognized as our privileged group of top agents.")
        out.append("Please reach out to your sales manager to discuss how we can help you get back into the game.")
        out.append("We look forward to seeing you bounce back!")
    return out

rec = pd.concat([agt.set_index('wa_cd_1')['tier'], 
                (f.fillna(0)['last_3m_rid'] / f['last_3m_ape']),
                (f.fillna(0)['last_6m_rep'] / f['last_6m_ape']),
                per.set_index(['agt_cd'])['14m_per'],
                f['last_6m_pol_size'],
                f['last_6m_fam'],
                f['last_6m_prd'],
                f.fillna(0)['last_3m_ape'],
                f.fillna(0)['last_3m_pol'],
                f.fillna(0)['last_yr_pol_size'],
                agt.set_index('wa_cd_1')['next_qualify_check'],
                agt.set_index('wa_cd_1')['agt_nm']
                ], axis = 1)
rec.columns = ['level', 'rid','rep','14m_per','last_6m_pol_size','last_6m_fam','last_6m_prd','last_3m_ape','last_3m_pol','last_yr_pol_size',
               'next_qualify_check', 'agt_name']
rec['zip'] = list(zip(rec['rid'],
                      rec['rep'],
                      rec['14m_per'],
                      rec['last_6m_pol_size'],
                      rec['last_6m_fam'],
                      rec['last_6m_prd'],
                      rec['last_3m_ape'],
                      rec['last_3m_pol'],
                      rec['last_yr_pol_size'],
                      rec['next_qualify_check'],
                      rec['agt_name']
                      ))
rec['diag_list'] = rec['zip'].map(lambda x: diagnose_recommend(x, avg_rid_per, avg_rep_per, avg_case_size, avg_fami_mbr, avg_14m_per))
rec['diag_0'] = rec['diag_list'].map(lambda x: x[0])
rec['diag_1'] = rec['diag_list'].map(lambda x: x[1])
rec['diag_2'] = rec['diag_list'].map(lambda x: x[2])
rec['diag_3'] = rec['diag_list'].map(lambda x: x[3])
rec = rec.reset_index().rename(columns = {'index':'agt_cd'})
rec.tail(2)

# COMMAND ----------

qualify_date_mth = dict(zip(agt['wa_cd_1'], pd.to_datetime(agt['qualify_date']).dt.to_period('M')))
max_qualify_date = pd.to_datetime(agt['qualify_date']).dt.to_period('M').max()
df['qualify_date'] = df['wa_cd_1'].map(lambda x: qualify_date_mth[x] if x in qualify_date_mth else max_qualify_date)

ape_this_year = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
                   (df['cvg_eff_dt'].dt.year == max_date.year)&
                   (df['cvg_eff_dt'] <= max_date)&
                   (~df['pol_stat_cd'].isin(['A','N','R']))
                   ].groupby(['wa_cd_1'])['adjust_ape'].sum().fillna(0)
ape_this_year.name = 'ape_this_year'

ape_since_qualify = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
                        (df['cvg_eff_dt'].dt.to_period('M') >= df['qualify_date'])&
                        (df['cvg_eff_dt'] <= max_date)&
                         (~df['pol_stat_cd'].isin(['A','N','R']))
                      ].groupby(['wa_cd_1'])['adjust_ape'].sum().fillna(0)
ape_since_qualify.name = 'ape_since_qualify'

pol_since_qualify = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
                        (df['cvg_eff_dt'].dt.to_period('M') >= df['qualify_date'])&
                        (df['cvg_eff_dt'] <= max_date)&
                         (~df['pol_stat_cd'].isin(['A','N','R']))&
                        (df['cvg_typ'] == 'B')
                      ].groupby(['wa_cd_1'])['pol_num'].nunique().fillna(0)
pol_since_qualify.name = 'pol_since_qualify'

active_month = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
                        (df['cvg_eff_dt'].dt.to_period('M') >= df['qualify_date'])&
                        (df['cvg_eff_dt'] <= max_date)&
                         (~df['pol_stat_cd'].isin(['A','N','R']))
                      ].groupby(['wa_cd_1'])['last_n_month_eff_cvg'].nunique().fillna(0)
active_month.name = 'active_month'

total_custm = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
                 (df['cvg_eff_dt'] <= max_date)&
                  (~df['pol_stat_cd'].isin(['A','N','R']))
                  ].groupby(['wa_cd_1'])['po_num'].nunique()
total_custm.name = 'total_custm'

best_month = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
                (df['cvg_eff_dt'] <= max_date)&
                  (~df['pol_stat_cd'].isin(['A','N','R']))
                  ].groupby(['wa_cd_1', 'cvg_eff_yr_mth'])['adjust_ape'].sum().reset_index().groupby(['wa_cd_1'])['adjust_ape'].max()
best_month.name = 'best_month'

agt_mth_sales = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
                  (~df['pol_stat_cd'].isin(['A','N','R']))&
                  (df['cvg_eff_dt'] <= max_date)
                  ].groupby(['wa_cd_1', 'cvg_eff_yr_mth'])['adjust_ape'].sum().reset_index()
which_month = agt_mth_sales.sort_values('adjust_ape',ascending = False).groupby('wa_cd_1').head(1).set_index('wa_cd_1').rename(columns={'cvg_eff_yr_mth':'which_month_max', 'adjust_ape':'max_mth_ape'})

best_case = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
               (df['cvg_eff_dt'] <= max_date)&
                  (~df['pol_stat_cd'].isin(['A','N','R']))
                  ].groupby(['wa_cd_1', 'pol_num'])['adjust_ape'].sum().reset_index().groupby(['wa_cd_1'])['adjust_ape'].max()
best_case.name = 'best_case'

active_custm = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
                  (df['pol_stat_cd'] == '1')&
                  (df['cvg_eff_dt'] <= max_date)&
                  (df['pol_trmn_dt'].isnull())
                  ].groupby(['wa_cd_1'])['po_num'].nunique()
active_custm.name = 'active_custm'

total_ape = df[(df['wa_cd_1'].isin(agt['wa_cd_1']))&
               (df['cvg_eff_dt'] <= max_date)&
                  (~df['pol_stat_cd'].isin(['A','N','R']))
                  ].groupby(['wa_cd_1'])['adjust_ape'].sum()
total_ape.name = 'total_ape'

dash = pd.concat([agt.set_index('wa_cd_1')[['agt_nm','agt_join_dt','tier']],
                  ape_since_qualify, pol_since_qualify, active_month,
                  total_custm, best_month, best_case, active_custm, total_ape,
                  which_month, ape_this_year], axis = 1)

dash['ape_since_qualify'] = dash['ape_since_qualify'].fillna(0).astype(int)
dash['pol_since_qualify'] = dash['pol_since_qualify'].fillna(0).astype(int)
dash['active_month'] = dash['active_month'].fillna(0).astype(int)

dash['mp_link_url'] = mp_link_url

dash['qualify_ape_min'] = 0
dash['qualify_ape_target'] = 600000000
dash['qualify_ape_max'] = 600000000

dash['qualify_pol_min'] = 0
dash['qualify_pol_target'] = 30
dash['qualify_pol_max'] = 30

dash['qualify_act_mth_min'] = 0
dash['qualify_act_target'] = 6
dash['qualify_act_max'] = 12

dash['next_tier'] = dash['tier'].map(lambda x: next_tier[x])
dash['next_tier_benchmark'] = dash['next_tier'].map(lambda x: ape_benchmark[x])
dash['gap_to_next_tier'] = dash['next_tier_benchmark'] - dash['ape_this_year'].fillna(0)
dash['gap_to_next_tier'] = dash['gap_to_next_tier'].map(lambda x: 0 if x < 0 else x)

dash = dash.reset_index().rename(columns = {'wa_cd_1': 'agt_cd'})
dash.head(2)

# COMMAND ----------

fyp_rank = df[(df['last_n_month_eff_cvg'] == -1)
             ].groupby(['wa_cd_1'])['adjust_ape'].sum().fillna(0).rank(axis=0, ascending=False, method = 'min').astype(int)
fyp_rank = fyp_rank.reset_index()
fyp_rank.columns = ['agt_cd', 'rank_fyp_last_mth']
fyp_rank['total_inforce_agt'] = tagtdm_daily.shape[0]
fyp_rank['rank_per_fyp_last_mth'] = fyp_rank['rank_fyp_last_mth']/fyp_rank['total_inforce_agt']

fyp_rank

# COMMAND ----------

select = mp[['agt_cd']].rename(columns = {'agt_cd':'agt'})
select['title_month'] = max_date + timedelta(days = 15)

# COMMAND ----------

select['report_month'] = mth_partition
page1['report_month'] = mth_partition
nm['report_month'] = mth_partition
nm_diag['report_month'] = mth_partition
lps['report_month'] = mth_partition
lapse_diag['report_month'] = mth_partition
page2_all['report_month'] = mth_partition
mth_sales['report_month'] = mth_partition
mth_prd['report_month'] = mth_partition
clm['report_month'] = mth_partition
per['report_month'] = mth_partition
fyp_rank['report_month'] = mth_partition
rec['report_month'] = mth_partition
dash['report_month'] = mth_partition

# COMMAND ----------

select.to_csv('/dbfs/mnt/lab/vn/project/temp_luobinr/select.csv', index = False)
pd.merge(mp[['agt_cd']], page1, on = 'agt_cd', how = 'left').to_csv('/dbfs/mnt/lab/vn/project/temp_luobinr/vn_mp_page1.csv', index = False)
pd.merge(mp[['agt_cd']], nm, on = 'agt_cd', how = 'left').to_csv('/dbfs/mnt/lab/vn/project/temp_luobinr/vn_mp_needs.csv', index = False)
pd.merge(mp[['agt_cd']], nm_diag, on = 'agt_cd', how = 'left').to_csv('/dbfs/mnt/lab/vn/project/temp_luobinr/vn_mp_needs_diag.csv', index = False)
pd.merge(mp[['agt_cd']], lps, on = 'agt_cd', how = 'left').to_csv('/dbfs/mnt/lab/vn/project/temp_luobinr/vn_mp_lapse.csv', index = False)
pd.merge(mp[['agt_cd']], lapse_diag, on = 'agt_cd', how = 'left').to_csv('/dbfs/mnt/lab/vn/project/temp_luobinr/vn_mp_lapse_diag.csv', index = False)
pd.merge(mp[['agt_cd']], page2_all, on = 'agt_cd', how = 'left').to_csv('/dbfs/mnt/lab/vn/project/temp_luobinr/vn_mp_page2.csv', index = False)
pd.merge(mp[['agt_cd']], mth_sales, on = 'agt_cd', how = 'left').to_csv('/dbfs/mnt/lab/vn/project/temp_luobinr/vn_mp_mth_sales.csv', index = False)
pd.merge(mp[['agt_cd']], mth_prd, on = 'agt_cd', how = 'left').to_csv('/dbfs/mnt/lab/vn/project/temp_luobinr/vn_mp_prd_mix.csv', index = False)
pd.merge(mp[['agt_cd']], clm, on = 'agt_cd', how = 'left').to_csv('/dbfs/mnt/lab/vn/project/temp_luobinr/vn_mp_claim.csv', index = False)
pd.merge(mp[['agt_cd']], per, on = 'agt_cd', how = 'left').to_csv('/dbfs/mnt/lab/vn/project/temp_luobinr/vn_mp_persis.csv', index = False)
pd.merge(mp[['agt_cd']], fyp_rank, on = 'agt_cd', how = 'left').to_csv('/dbfs/mnt/lab/vn/project/temp_luobinr/vn_mp_rank.csv', index = False)
pd.merge(mp[['agt_cd']], rec, on = 'agt_cd', how = 'left').to_csv('/dbfs/mnt/lab/vn/project/temp_luobinr/vn_mp_recommend.csv', index = False)
pd.merge(mp[['agt_cd']], dash, on = 'agt_cd', how = 'left').to_csv('/dbfs/mnt/lab/vn/project/temp_luobinr/vn_mp_dashboard.csv', index = False)

# COMMAND ----------

# for VN version recommendation

# COMMAND ----------

avg_rid_per = f43.sum()/f3.sum()
avg_rep_per = f47.sum()/f3.sum()
avg_case_size = f51.mean()
avg_fami_mbr = f39.median()
avg_14m_per = per['14m_per'].median()

def diagnose_recommend(x, avg_rid_per, avg_rep_per, avg_case_size, avg_fami_mbr, avg_14m_per):
    out = []
    if x[6]/3 >= 600000000/12:
        out.append("Chúc mừng bạn!")
        out.append("Bạn là một trong những Đại lý Manulife Pro Bạch kim hàng đầu của Manulife - điều này có nghĩa là bạn là người giỏi nhất trong số những người giỏi nhất!")
        out.append("Bạn đang đi đúng hướng để giữ vững hạng Bạch kim vào kỳ đánh giá tiếp theo vào " + x[9] + ".")
        out.append("Hãy giữ vững phong độ để nhận được khoản Thưởng Năng Suất Xuất Sắc Tháng dành cho đại lý Manulife Pro. Dưới đây là một số gợi ý khai thác thêm hợp đồng  dành cho bạn.")
    elif (x[6]/3 > 0)&(x[7] > 0):
        out.append("Hiệu suất bán hàng của bạn trong 3 tháng qua dường như thấp hơn khả năng của bạn nhưng đừng lo lắng; chúng tôi ở đây để hỗ trợ bạn. Dưới đây là một số ý tưởng dành cho bạn:")
        if (x[0] <= avg_rid_per - 0.1)&(len(out) < 4):
            out.append("Bán nhiều sản phẩm bỗ trợ hơn! Bạn đã bán " + str(int(x[0]*100)) + "% sản phẩm bỗ trợ, so với " + str(int(avg_rid_per*100)) + "% của các Đại lý Bạch kim khác .")
        if (x[1] <= avg_rep_per - 0.1)&(len(out) < 4):
            out.append("Bán chéo cho khách hàng hiện tại của bạn! Bạn đã bán " + str(int(x[1]*100)) + "% cho khách hàng hiện tại, so với " + str(int(avg_rep_per*100)) + "% của các đại lý Bạch kim khác.")
        if (x[3] <= avg_case_size*0.7)&(len(out) < 4):
            out.append("Hãy cân nhắc để mở rộng nguồn khách hàng chất lượng cao của bạn! Độ lớn hợp đồng trung bình của bạn là " + str(int(x[3]/100000)/10) + "M, so với " + str(int(avg_case_size/1000000)) + "M của các đại lý Bạch kim khác.")
        if (x[4] < avg_fami_mbr)&(len(out) < 4):
            out.append("Cơ hội cho bạn khai thác hợp đồng từ các thành viên trong gia đình khách hàng! Bạn đã bán cho " + str(int(x[4])) + " nhóm thành viên trong gia đình so với " + str(int(avg_fami_mbr)) + " của các đại lý Bạch kim khác.")
        if (x[2] <= avg_14m_per - 0.1)&(len(out) < 4):
            out.append("Hãy cải thiện chất lượng bán hàng của bạn! " + str(int((1-x[2])*100)) + "% hợp đồng bạn đã bán bị mất hiệu lực sau năm đầu tiên, so với " + str(int((1-avg_14m_per)*100)) + "% đối với các đại lý Bạch kim khác.")
        if len(out) < 4:
            out.append("Đừng do dự! Hãy đem báo cáo này đến để tham vấn với người quản lý hoặc cố vấn của bạn để họ có thể giúp bạn quay lại đường đua.")
        if len(out) < 4:
            out.append("Có rất nhiều tài liệu học tập và phát triển kỹ năng thực tế trên Manuacademy đang chờ bạn khám phá!")
        if len(out) < 4:
            out.append("Bạn hãy tin vào sức mạnh và sức bật của bản thân!")
    else:
        out.append("Bạn " + x[10] + " ơi!, Bạn ở đâu rồi? Chúng tôi rất nhớ bạn và tin rằng bạn sẽ trở lại lợi hại hơn xưa!")
        out.append("Là một Pro Bạch Kim, bạn là thành viên của nhóm đại lý xuất sắc nhất với những đặc quyền riêng của Manulife Pro dành cho bạn.")
        out.append("Hãy đến ngay với cấp quản lý của bạn để tham vấn cách phù hợp nhất giúp bạn quay lại cuộc đua và chinh phục những đỉnh cao mới.")
        out.append("Bạn hãy tin vào sức mạnh và sức bật của bản thân!")
    return out

rec = pd.concat([agt.set_index('wa_cd_1')['tier'], 
                (f.fillna(0)['last_3m_rid'] / f['last_3m_ape']),
                (f.fillna(0)['last_6m_rep'] / f['last_6m_ape']),
                per.set_index(['agt_cd'])['14m_per'],
                f['last_6m_pol_size'],
                f['last_6m_fam'],
                f['last_6m_prd'],
                f.fillna(0)['last_3m_ape'],
                f.fillna(0)['last_3m_pol'],
                f.fillna(0)['last_yr_pol_size'],
                agt.set_index('wa_cd_1')['next_qualify_check'],
                agt.set_index('wa_cd_1')['agt_nm']
                ], axis = 1)
rec.columns = ['level', 'rid','rep','14m_per','last_6m_pol_size','last_6m_fam','last_6m_prd','last_3m_ape','last_3m_pol','last_yr_pol_size',
               'next_qualify_check', 'agt_name']
rec['zip'] = list(zip(rec['rid'],
                      rec['rep'],
                      rec['14m_per'],
                      rec['last_6m_pol_size'],
                      rec['last_6m_fam'],
                      rec['last_6m_prd'],
                      rec['last_3m_ape'],
                      rec['last_3m_pol'],
                      rec['last_yr_pol_size'],
                      rec['next_qualify_check'],
                      rec['agt_name']
                      ))
rec['diag_list'] = rec['zip'].map(lambda x: diagnose_recommend(x, avg_rid_per, avg_rep_per, avg_case_size, avg_fami_mbr, avg_14m_per))
rec['diag_0'] = rec['diag_list'].map(lambda x: x[0])
rec['diag_1'] = rec['diag_list'].map(lambda x: x[1])
rec['diag_2'] = rec['diag_list'].map(lambda x: x[2])
rec['diag_3'] = rec['diag_list'].map(lambda x: x[3])
rec = rec.reset_index().rename(columns = {'index':'agt_cd'})
rec['report_month'] = mth_partition
pd.merge(mp[['agt_cd']], rec, on = 'agt_cd', how = 'left').to_csv('/dbfs/mnt/lab/vn/project/temp_luobinr/vn_mp_recommend_vn.csv', index = False)

# COMMAND ----------

# Ad hoc processing
