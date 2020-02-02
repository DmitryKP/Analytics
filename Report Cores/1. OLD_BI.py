print('OLD BI started')

import dill
import pyodbc
from google.cloud import bigquery
import pandas as pd
pd.set_option('display.max_rows', 1000)
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from IPython.display import display, HTML
import win32com.client
import numpy as np
import pymorphy2
morph = pymorphy2.MorphAnalyzer()
from datetime import datetime
import warnings
import datetime
warnings.filterwarnings("ignore")

print('Привет! Помни,что OLD BI обновляется первее, чем NEW BI, для пополнения справочника')
client = bigquery.Client.from_service_account_json(r'xxx.json')
connection_DB_CRM = pyodbc.connect(Trusted_Connection='yes', driver = '{SQL Server}',server = 'xxx' , database = 'xxx')

#Подгрузка CRM данных и подготовка да;нных
sql_crm="SELECT * FROM [datamartMarketing].[vwCrmProspect]"
SQL_CRM_all = pd.read_sql(sql_crm,connection_DB_CRM)
SQL_CRM_all1=SQL_CRM_all.fillna(0)
SQL_CRM_all2=SQL_CRM_all1[(~SQL_CRM_all1['source_form_url'].str.contains('dev.|dev3|localhost|obc/ru|ssr-dv.|open-am.|landings.loc|open.ru', na=False))]
SQL_CRM_all2['year'] = pd.DatetimeIndex(SQL_CRM_all2['date_creation']).year
SQL_CRM_all2['month'] = pd.DatetimeIndex(SQL_CRM_all2['date_creation']).month
dict_russian_month = {1:'Январь',2:'Февраль',3:'Март',4:'Апрель',5:'Май',6:'Июнь',7:'Июль',8:'Август',9:'Сентябрь',10:'Октябрь',11:'Ноябрь',12:'Декабрь'}
SQL_CRM_all21 = SQL_CRM_all2.replace({"month": dict_russian_month})
SQL_CRM_all21['SourceMedium']=SQL_CRM_all21['ga_cs'].astype(str) + SQL_CRM_all21['ga_cm'].astype(str)
SQL_CRM_all21.rename(columns={'ga_cn': 'Campaign'}, inplace=True)

#Подгрузка Куба и подготовка данных

coub_old = pd.read_excel(r'C:\Users\korpachev\BI_Reports\Coub1_2016_2018.xlsx')
coub_old.fillna(0, inplace=True)

office = win32com.client.Dispatch("Excel.Application")
wb = office.Workbooks.Open(r'C:\Users\korpachev\BI_Reports\Coub1_2019.xlsx')
wb.RefreshAll()
wb.Save()
wb.Close()
coub_new = pd.read_excel(r'C:\Users\korpachev\BI_Reports\Coub1_2019.xlsx')
coub_new.fillna(0,inplace=True)
coub_new['Месяц'] = coub_new['Месяц'].apply(lambda x: str(x).replace(' 2019',''))

coub_df = pd.concat([coub_old, coub_new], axis=0)
del coub_old
del coub_new

#Выгрузка данных из Analytics Edge

today_date = datetime.datetime.now().strftime('%Y%m%d')
QUERY_GA = ('WITH VISITS_NEW_USERS AS (SELECT date, source, medium, campaign, Sessions, New_Users, date_formatted, CONCAT(date_formatted,"|",campaign) as concat_b FROM (SELECT date, trafficSource.source AS source, trafficSource.medium AS medium, trafficSource.campaign AS campaign, COUNT(DISTINCT CONCAT(fullVisitorId, CAST(visitId AS STRING))) as Sessions, COUNT(DISTINCT IF(visitNumber=1, fullVisitorId, NULL) ) as New_Users, FORMAT_DATE("%Y-%m-%d", PARSE_DATE("%Y%m%d", date)) AS date_formatted FROM `api-open-broker.108613784.ga_sessions_*`, UNNEST(hits) as hits WHERE _TABLE_SUFFIX BETWEEN "20191101" AND "{}" GROUP BY date, source, medium, campaign, date_formatted ORDER BY DATE DESC)), BUDGETS AS (SELECT utm_source, date, utm_campaign, sum, CONCAT(CAST(date AS STRING),"|",utm_campaign) as concat_b FROM (SELECT utm_source, ________ as date, utm_campaign, sum FROM `open-broker-230412.OpenBroker_2019.stats_*` WHERE _TABLE_SUFFIX BETWEEN "20191101" AND "{}" ORDER BY DATE desc)) SELECT date_formatted as Date, source as Source, medium as Medium, campaign as Campaign, Sessions, New_Users, sum as Cost FROM VISITS_NEW_USERS w LEFT JOIN BUDGETS p USING (concat_b) ORDER BY date_formatted DESC'.format(today_date,today_date))
QUERY_GA_job = client.query(QUERY_GA)
stat_ga_last = QUERY_GA_job.to_dataframe()
stat_ga_last.rename(columns={'New_Users': 'New Users'}, inplace=True)
stat_ga_last['Date']=pd.to_datetime(stat_ga_last['Date'])
stat_ga_last['year'] = pd.DatetimeIndex(stat_ga_last['Date']).year
stat_ga_last['month'] = pd.DatetimeIndex(stat_ga_last['Date']).month

stat_ga_last = stat_ga_last.groupby(['year','month','Source','Medium','Campaign'],as_index=False).sum()
stat_ga_last['Cost'] = np.where( stat_ga_last['Source'].str.contains('fb'), stat_ga_last['Cost']*1.2*1.1, stat_ga_last['Cost'])  
stat_ga_last['Cost'] = np.where( stat_ga_last['Source'].str.contains('mgcom_tm_web'), stat_ga_last['Cost']*1.2, stat_ga_last['Cost'])

stat_ga_hist = pd.read_excel(r'C:\Users\korpachev\BI_Reports\GA_Hist_All.xlsx')

stat_ga = pd.concat([stat_ga_hist, stat_ga_last], axis=0)
stat_ga = stat_ga.replace({"month": dict_russian_month})

#Подгрузка справочников и предварительное удаление дублей
df_spravochnik_all_campaigns = pd.read_excel(r'C:\Users\korpachev\BI_Reports\Sprаvochnik_All_Campaigns.xlsx')
df_spravochnik_all_campaigns_noduplicates = df_spravochnik_all_campaigns.drop_duplicates(subset='Campaign', keep="first")
df_Old_SourceMedium = pd.read_excel(r'C:\Users\korpachev\BI_Reports\Sprаvochnik_Old_SourceMedium.xlsx')
df_Old_SourceMedium_noduplicates = df_Old_SourceMedium.drop_duplicates(subset='SourceMedium', keep="first")


#Мердж GA и CRM со справочником, создание листов со списком несмэченных кампаний
df_camp_categ_GA=stat_ga.merge(df_spravochnik_all_campaigns_noduplicates,on='Campaign',how='left')
list_new_camp_GA=list(df_camp_categ_GA[df_camp_categ_GA['Campaign type'].isnull()]['Campaign'])
SQL_1=SQL_CRM_all21.merge(df_spravochnik_all_campaigns_noduplicates,on='Campaign',how='left')
list_new_camp_CRM=list(SQL_1[SQL_1['Campaign type'].isnull()]['Campaign'])
del SQL_1

#Докаталогизация кампаний через FUZZY
cam_name=df_spravochnik_all_campaigns_noduplicates['Campaign']
cam_type=df_spravochnik_all_campaigns_noduplicates['Campaign type']
list_c=list(set(list_new_camp_GA+list_new_camp_CRM))
print("Всего {} новых кампаний".format(len(list_c)))
cam_cat_def=[]
max_matches_index_value=[]
list_categ=[]
for index, elem1 in enumerate(list_c):
    list_of_max_matches=[]
    for elem2 in cam_name:
        list_of_max_matches.append(str(fuzz.partial_ratio(str(elem1),str(elem2))))
    max_matches_index_value.append(str(list_of_max_matches.index(max(list_of_max_matches)))+'__'+str(max(list_of_max_matches)))
    print(index,end = ' ')
for index,e in enumerate(max_matches_index_value):
    if float(str(e).split('__')[1])>7:
        list_categ.append(cam_type[int(str(e).split('__')[0])])
    else:
        list_categ.append('Другое')

#Создание справочника
new_camp_df=pd.DataFrame()
new_camp_df['Campaign']=list_c
new_camp_df['Campaign type']=list_categ

resp='Да'
#Условие обновления кампаний 
if new_camp_df.empty == False:
    if resp=='Да':
        df_spravochnik_all_campaigns_new = pd.concat([df_spravochnik_all_campaigns_noduplicates, new_camp_df], axis=0)
        df_spravochnik_all_campaigns_new_noduplicates = df_spravochnik_all_campaigns_new.drop_duplicates(subset='Campaign', keep="first")
        df_spravochnik_all_campaigns_new_noduplicates.to_excel(r'C:\Users\korpachev\BI_Reports\Sprаvochnik_All_Campaigns.xlsx',index=False)
        df_spravochnik_all_campaigns_noduplicates = pd.read_excel(r'C:\Users\korpachev\BI_Reports\Sprаvochnik_All_Campaigns.xlsx')
        df_camp_categ_GA=stat_ga.merge(df_spravochnik_all_campaigns_noduplicates,on='Campaign',how='left')
    else:
        new_camp_df['Для ручной категоризации'] = "Новые кампании"
        df_spravochnik_all_campaigns_handtreatening = pd.concat([df_spravochnik_all_campaigns_noduplicates, new_camp_df], axis=0)
        df_spravochnik_all_campaigns_handtreatening.to_excel(r'C:\Users\korpachev\BI_Reports\Sprаvochnik_All_Campaigns.xlsx',index=False)
        print("Напиши Ок, как откатегоризуешь все вручную в Sprаvochnik_All_Campaigns. Не забудь удалить третий столбец c маркером Новые кампании!")
        answer_verification1=str(input())
        if answer_verification1=='Ок':
            df_spravochnik_all_campaigns_noduplicates = pd.read_excel(r'C:\Users\korpachev\BI_Reports\Sprаvochnik_All_Campaigns.xlsx')
            df_camp_categ_GA=stat_ga.merge(df_spravochnik_all_campaigns_noduplicates,on='Campaign',how='left')
        else:
            print('Повтори')

df_camp_categ_GA['SourceMedium']=df_camp_categ_GA['Source'].astype(str) + df_camp_categ_GA['Medium'].astype(str)

#Мердж GA и CRM со справочником, создание листов со списком несмэченных SourceMedium
df_sm_categ_all_GA=df_camp_categ_GA.merge(df_Old_SourceMedium_noduplicates,on='SourceMedium',how='left')
list_new_sm_GA=list(df_sm_categ_all_GA[df_sm_categ_all_GA['SourceMedium_Type'].isnull()]['SourceMedium'])
SQL_1=SQL_CRM_all21.merge(df_Old_SourceMedium_noduplicates, on='SourceMedium', how='left')
list_new_sm_CRM=list(SQL_1[SQL_1['SourceMedium_Type'].isnull()]['SourceMedium'])
del SQL_1


cam_name=df_Old_SourceMedium_noduplicates['SourceMedium']
cam_type=df_Old_SourceMedium_noduplicates['SourceMedium_Type']
list_c=list(set(list_new_sm_GA+list_new_sm_CRM))
list_c_nonref=[]
list_c_ref=[]
for e in list_c:
    if str(e).lower().count('refer')==0:
        list_c_nonref.append(e)
    else:
        list_c_ref.append(e)
print("Всего {} новых реферальных кампаний".format(str(len(list_c_ref))))
print("Всего {} новых нереферальных кампаний".format(str(len(list_c_nonref))))

#Докаталогизация кампаний через FUZZY
cam_cat_def=[]
max_matches_index_value=[]
list_categ=[]
for index, elem1 in enumerate(list_c_nonref):
    list_of_max_matches=[]
    for elem2 in cam_name:
        list_of_max_matches.append(str(fuzz.partial_ratio(str(elem1),str(elem2))))
    max_matches_index_value.append(str(list_of_max_matches.index(max(list_of_max_matches)))+'__'+str(max(list_of_max_matches)))
    print(index,end = ' ')
for index,e in enumerate(max_matches_index_value):
    if float(str(e).split('__')[1]) > 7:
        list_categ.append(cam_type[int(str(e).split('__')[0])])
    else:
        list_categ.append('Другое')

new_sm_df_ref=pd.DataFrame()
new_sm_df_ref['SourceMedium'] = list_c_ref
new_sm_df_ref['SourceMedium_Type']='Referral'
new_sm_df_ref.to_csv(r'C:/Users/korpachev/BI_Reports/Spravochnik_Add_NewReferralSourceMedium.csv',index=False)
new_sm_df_ref=pd.read_csv(r'C:/Users/korpachev/BI_Reports/Spravochnik_Add_NewReferralSourceMedium.csv')
new_sm_df_nonref=pd.DataFrame()
new_sm_df_nonref['SourceMedium'] = list_c_nonref
new_sm_df_nonref['SourceMedium_Type'] = list_categ
new_sm_df=pd.concat([new_sm_df_ref, new_sm_df_nonref], axis=0)

resp=='Да'
if new_sm_df_nonref.empty == False:
    if resp=='Да':
        df_Old_SourceMedium_all_new = pd.concat([df_Old_SourceMedium_noduplicates, new_sm_df], axis=0)
        df_Old_SourceMedium_all_new_noduplicates = df_Old_SourceMedium_all_new.drop_duplicates(subset='SourceMedium', keep="first")
        writer = pd.ExcelWriter(r'C:\Users\korpachev\BI_Reports\Sprаvochnik_Old_SourceMedium.xlsx', engine='xlsxwriter',options={'strings_to_urls': False})
        df_Old_SourceMedium_all_new_noduplicates.to_excel(writer, index=False)
        writer.close()
        df_Old_SourceMedium_noduplicates = pd.read_excel(r'C:\Users\korpachev\BI_Reports\Sprаvochnik_Old_SourceMedium.xlsx')
        df_sm_categ_all_GA=df_camp_categ_GA.merge(df_Old_SourceMedium_noduplicates,on='SourceMedium',how='left')
    else:
        new_sm_df['Для ручной категоризации'] = "Новые источники/каналы"
        df_Old_SourceMedium_all_new = pd.concat([df_Old_SourceMedium_noduplicates, new_sm_df], axis=0)
        df_Old_SourceMedium_all_new_noduplicates = df_Old_SourceMedium_all_new.drop_duplicates(subset='SourceMedium', keep="first")
        writer = pd.ExcelWriter(r'C:\Users\korpachev\BI_Reports\Sprаvochnik_Old_SourceMedium.xlsx', engine='xlsxwriter',options={'strings_to_urls': False})
        df_Old_SourceMedium_all_new_noduplicates.to_excel(writer, index=False)
        writer.close()
        print("Напиши Ок, как откатегоризуешь все вручную в Sprаvochnik_Old_SourceMedium. Не забудь удалить третий столбец c маркером Новые Источники/каналы!")
        answer_verification2=str(input())
        if answer_verification2=='Ок':
            df_Old_SourceMedium_noduplicates = pd.read_excel(r'C:\Users\korpachev\BI_Reports\Sprаvochnik_Old_SourceMedium.xlsx')
            df_sm_categ_all_GA=df_camp_categ_GA.merge(df_Old_SourceMedium_noduplicates,on='SourceMedium',how='left')
        else:
            print('Повтори')   


#Процессинг
SQL_camp_cat=SQL_CRM_all21.merge(df_spravochnik_all_campaigns_noduplicates,on='Campaign',how='left')
SQL_camp_sm_cat=SQL_camp_cat.merge(df_Old_SourceMedium_noduplicates,on='SourceMedium',how='left')

full_merge=SQL_camp_sm_cat.merge(coub_df,on='GUID',how='left')
full_merge1=full_merge.fillna(0)
full_merge2=full_merge1.drop_duplicates(subset='GUID', keep="last")

gr1=full_merge1.groupby(['year', 'month', 'Campaign type', 'SourceMedium_Type' ],as_index=False)['Кол-во новых проспектов', 'Кол-во новых контактов','Зарегистрировано договоров БО', 'К-во персон 5 - 50 т.р.','К-во персон 50 - 100 т.р.', 'ДС+ЦБ Ввод руб', 'Фин рез П без НДС'].sum()
gr2=full_merge2.groupby(['year', 'month', 'Campaign type', 'SourceMedium_Type' ],as_index=False)['Активы руб на конец П'].sum()
gr3=df_sm_categ_all_GA.groupby(['year', 'month', 'Campaign type', 'SourceMedium_Type' ],as_index=False)['Sessions','New Users','Cost'].sum()
gr3['year'] = gr3['year'].astype(int)

gr1['Concat']=gr1['year'].astype(str)+gr1['month'].astype(str)+gr1['Campaign type'].astype(str)+gr1['SourceMedium_Type'].astype(str)
gr2['Concat']=gr2['year'].astype(str)+gr2['month'].astype(str)+gr2['Campaign type'].astype(str)+gr2['SourceMedium_Type'].astype(str)
gr3['Concat']=gr3['year'].astype(str)+gr3['month'].astype(str)+gr3['Campaign type'].astype(str)+gr3['SourceMedium_Type'].astype(str)

total_gr = gr1.merge(gr2,on='Concat',how='left').merge(gr3,on='Concat',how='left')
total_gr2 = total_gr[['year_x', 'month_x', 'SourceMedium_Type_x', 'Campaign type_x', 'Sessions', 'New Users', 'Кол-во новых проспектов', 'Кол-во новых контактов', 'Зарегистрировано договоров БО', 'К-во персон 5 - 50 т.р.','К-во персон 50 - 100 т.р.','ДС+ЦБ Ввод руб', 'Фин рез П без НДС', 'Cost']]
total_gr2['year_x'] = total_gr2['year_x'].astype(int)
total_gr2.fillna(0, inplace=True)

backup_nb = pd.read_excel(r'C:/Users/korpachev/BI_Reports/Output/NB_OpenBroker.xlsx').to_excel(r'C:/Users/korpachev/BI_Reports/Output/Backups/automated/NB_OpenBroker1.xlsx',index=False)
backup_nbo = pd.read_excel(r'C:/Users/korpachev/BI_Reports/Output/NB_Opentrainer.xlsx').to_excel(r'C:/Users/korpachev/BI_Reports/Output/Backups/automated/NB_Opentrainer1.xlsx',index=False)
backup_ob = pd.read_excel(r'C:/Users/korpachev/BI_Reports/Output/OB_OpenBroker.xlsx').to_excel(r'C:/Users/korpachev/BI_Reports/Output/Backups/automated/OB_OpenBroker1.xlsx',index=False)
backup_pr = pd.read_excel(r'C:/Users/korpachev/BI_Reports/Output/Pages_Openbroker.xlsx').to_excel(r'C:/Users/korpachev/BI_Reports/Output/Backups/automated/Pages_Openbroker1.xlsx',index=False)


total_gr2.to_excel(r'C:/Users/korpachev/BI_Reports/Output/OB_OpenBroker.xlsx',index=False)

print('OLD BI completed')

print('Prediction data started')
from IPython.display import display, HTML
import numpy as np
import matplotlib


CRM_time_series = SQL_CRM_all21.copy()
CRM_time_series['week'] = CRM_time_series['date_creation'].dt.to_period('W').apply(lambda r: r.start_time).dt.date
#CRM_time_series['month_year'] = pd.to_datetime(CRM_time_series['date_creation']).dt.to_period('M')
CRM_time_series['month_year'] = CRM_time_series.date_creation.values.astype('datetime64[M]')
CRM_time_series['date'] = pd.to_datetime(CRM_time_series['date_creation']).dt.date

series_requests = CRM_time_series.groupby('week')['GUID'].count().to_frame().reset_index().rename(columns={'week': 'ds', 'GUID': 'y'}).head(-1)
series_requests['ds'] = series_requests['ds'].astype(str)

old_pred = pd.read_excel(r'C:/Users/korpachev/BI_Reports/Output/Prediction_TS.xlsx')
series_requests['ds'] = series_requests['ds'].astype(str)
old_pred['ds'] = old_pred['ds'].astype(str)
old_pred[['ds','trend']].merge(series_requests,on='ds',how='left').to_excel(r'C:/Users/korpachev/BI_Reports/Output/Prediction_TS.xlsx',index=False)

print('Prediction data completed')