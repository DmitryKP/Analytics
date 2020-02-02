print('NEW BI started')

import pandas as pd
#import difflib
from fuzzywuzzy import fuzz
from google.cloud import bigquery
import win32com.client
import pyodbc
import datetime
import numpy as np
import warnings
warnings.filterwarnings("ignore")


client = bigquery.Client.from_service_account_json(r'xxx')
connection_DB_CRM = pyodbc.connect(Trusted_Connection='yes', driver = '{SQL Server}',server = 'xxx' , database = 'xxx')


#Подгрузка последнего месяца из CRM-ки 
sql_crm="SELECT * FROM [datamartMarketing].[vwCrmProspect]"
SQL_CRM_all = pd.read_sql(sql_crm,connection_DB_CRM)
SQL_CRM_all = SQL_CRM_all[(~SQL_CRM_all['source_form_url'].str.contains('dev.|dev3|localhost|obc/ru|ssr-dv.|open-am.|landings.loc|open.ru'))]

SQL_CRM_all = SQL_CRM_all[['GUID','ga_cs','ga_cm','ga_cn','date_creation']]

SQL_CRM_all['year'] = pd.DatetimeIndex(SQL_CRM_all['date_creation']).year
SQL_CRM_all['month'] = pd.DatetimeIndex(SQL_CRM_all['date_creation']).month
dict_russian_month={1:'Январь',2:'Февраль',3:'Март',4:'Апрель',5:'Май',6:'Июнь',7:'Июль',8:'Август',9:'Сентябрь',10:'Октябрь',11:'Ноябрь',12:'Декабрь'}
SQL_CRM_all=SQL_CRM_all.replace({"month": dict_russian_month})
SQL_CRM_all=SQL_CRM_all.fillna('(not set)')

SQL_CRM_all['SourceMedium'] = SQL_CRM_all['ga_cs'].astype(str) + SQL_CRM_all['ga_cm'].astype(str)
SQL_CRM_all['SourceMediumCampaign'] = SQL_CRM_all['ga_cs'].astype(str) + SQL_CRM_all['ga_cm'].astype(str) + SQL_CRM_all['ga_cn'].astype(str)


#Подгрузка справочника
df_spravochnik_all_campaigns = pd.read_excel(r'C:\Users\korpachev\BI_Reports\Sprаvochnik_All_Campaigns.xlsx')
df_spravochnik_all_campaigns_noduplicates = df_spravochnik_all_campaigns.drop_duplicates(subset='Campaign', keep="first")
df_Old_SourceMedium = pd.read_excel(r'C:\Users\korpachev\BI_Reports\Sprаvochnik_Old_SourceMedium.xlsx')
df_Old_SourceMedium_noduplicates = df_Old_SourceMedium.drop_duplicates(subset='SourceMedium', keep="first")
df_group_campaigns = pd.read_excel(r'C:\Users\korpachev\BI_Reports\Sprаvochnik_Group_by_Campaigns.xlsx')
df_New_SourceMedium = pd.read_excel(r'C:\Users\korpachev\BI_Reports\Sprаvochnik_New_SourceMedium.xlsx')


#Подгрузка Куба и подготовка данных

coub_old = pd.read_excel(r'C:\Users\korpachev\BI_Reports\Coub1_2016_2018.xlsx')
coub_old.fillna(0, inplace=True)
coub_new = pd.read_excel(r'C:\Users\korpachev\BI_Reports\Coub1_2019.xlsx')
coub_new.fillna(0,inplace=True)
    
df_COUB_GUID_Dirty = pd.concat([coub_old, coub_new], axis=0)

del coub_old
del coub_new

df_COUB_GUID = df_COUB_GUID_Dirty[~df_COUB_GUID_Dirty['GUID'].isnull()]
df_COUB_GUID = df_COUB_GUID[df_COUB_GUID['GUID']!="<...>"]
df_COUB_GUID.fillna(0,inplace=True)
df_COUB_GUID = df_COUB_GUID[['GUID', 'Год', 'Месяц', 'Кол-во новых проспектов', 'Кол-во новых контактов', 'Зарегистрировано договоров БО', 'К-во персон 5 - 50 т.р.','К-во персон 50 - 100 т.р.','Активы руб на конец П', 'ДС+ЦБ Ввод руб', 'Фин рез П без НДС']]


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
stat_ga['SourceMedium'] = stat_ga['Source'] + stat_ga['Medium']
stat_ga['SourceMediumCampaignYearMonth'] = stat_ga['Source'].astype(str) + stat_ga['Medium'].astype(str) + stat_ga['Campaign'].astype(str) + stat_ga['year'].astype(str) + stat_ga['month'].astype(str)

#Обработка данных из аналитикса и их последующая категоризация 

df3=stat_ga.merge(df_spravochnik_all_campaigns,on='Campaign',how='left')
df3.rename(columns={'Campaign type': 'Старое название кампании'}, inplace=True)
df4=df3.merge(df_group_campaigns,on='Старое название кампании',how='left')
df5=df4.merge(df_Old_SourceMedium,on='SourceMedium',how='left')
df5.rename(columns={'SourceMedium_Type': 'Источник по старому'}, inplace=True)
df6=df5.merge(df_New_SourceMedium,on='Источник по старому',how='left')

df7=df6.drop_duplicates(subset='SourceMediumCampaignYearMonth', keep="first") #Там нет дат - каждая строчка уникальная связка должна быть!


df_pageviews=df7.groupby(['year','month','Источник по новому','Новое название кампании'],as_index=False).sum()
df_pageviews['fin_con'] = df_pageviews['year'].astype(str) + df_pageviews['month'] + df_pageviews['Источник по новому'] + df_pageviews['Новое название кампании']

#Докатегоризация нового CRM-файла

SQL_CRM_all.rename(columns={'ga_cn': 'Campaign'}, inplace=True)

df8=SQL_CRM_all.merge(df_spravochnik_all_campaigns,on='Campaign',how='left')
df8.rename(columns={'Campaign type': 'Старое название кампании'}, inplace=True)
df9=df8.merge(df_group_campaigns,on='Старое название кампании',how='left')
df10=df9.merge(df_Old_SourceMedium,on='SourceMedium',how='left')
df10.rename(columns={'SourceMedium_Type': 'Источник по старому'}, inplace=True)
df11=df10.merge(df_New_SourceMedium,on='Источник по старому',how='left')

df121=df11.drop_duplicates(subset='GUID', keep="last")
df121 = df121[['GUID', 'Новое название кампании', 'Источник по новому']]

#Слияние КУБа с CRM

coubs_crm = df_COUB_GUID.merge(df121,on='GUID',how='left')
coubs_crm = coubs_crm[~coubs_crm['Новое название кампании'].isnull()]
coubs_crm_fin = coubs_crm.groupby(['Год','Месяц','Источник по новому','Новое название кампании'],as_index=False)['Кол-во новых проспектов', 'Кол-во новых контактов', 'Зарегистрировано договоров БО', 'К-во персон 5 - 50 т.р.','К-во персон 50 - 100 т.р.','ДС+ЦБ Ввод руб', 'Фин рез П без НДС'].sum()
coubs_crm_fin['fin_con'] = coubs_crm_fin['Год'].astype(str) + coubs_crm_fin['Месяц'].astype(str) + coubs_crm_fin['Источник по новому'].astype(str) + coubs_crm_fin['Новое название кампании'].astype(str)

#Создание итоговой таблицы
df_crm_coub_fin_grouped = df_pageviews.merge(coubs_crm_fin,on='fin_con',how='left')
df_crm_coub_fin_grouped = df_crm_coub_fin_grouped[['year','month','Источник по новому_x','Новое название кампании_x','Sessions','New Users','Кол-во новых проспектов','Кол-во новых контактов','Зарегистрировано договоров БО','К-во персон 5 - 50 т.р.', 'К-во персон 50 - 100 т.р.','ДС+ЦБ Ввод руб','Фин рез П без НДС','Cost']]
df_crm_coub_fin_grouped.fillna(0, inplace=True)
df_crm_coub_fin_grouped = df_crm_coub_fin_grouped[df_crm_coub_fin_grouped['year']>=2017]
df_crm_coub_fin_grouped['Conc_Budget_Month'] = df_crm_coub_fin_grouped['year'].astype(str) + df_crm_coub_fin_grouped['month'].astype(str) + df_crm_coub_fin_grouped['Источник по новому_x'].astype(str)

df_planinb=pd.read_excel(r'C:\Users\korpachev\BI_Reports\PlanBudjetsforNewBI.xlsx')
df_planinb['Conc_Budget_Month'] = '2019' + df_planinb['Conc_Budget_Month'].astype(str)

df_crm_coub_fin_grouped = df_crm_coub_fin_grouped.merge(df_planinb, on='Conc_Budget_Month', how='left')
number_of_Conc_Budget_Month=df_crm_coub_fin_grouped.groupby(['Conc_Budget_Month'],as_index=False)['Источник по новому_x'].count()
df_crm_coub_fin_grouped = df_crm_coub_fin_grouped.merge(number_of_Conc_Budget_Month,on='Conc_Budget_Month', how='left')
df_crm_coub_fin_grouped.fillna(0, inplace=True)
df_crm_coub_fin_grouped['Планируемый бюджет']=df_crm_coub_fin_grouped['Планируемый бюджет полностью']/df_crm_coub_fin_grouped['Источник по новому_x_y']

df_crm_coub_fin_grouped=df_crm_coub_fin_grouped[['year','month','Источник по новому_x_x','Новое название кампании_x','Sessions','New Users','Кол-во новых проспектов','Кол-во новых контактов','Зарегистрировано договоров БО','К-во персон 5 - 50 т.р.','К-во персон 50 - 100 т.р.','ДС+ЦБ Ввод руб','Фин рез П без НДС','Cost','Планируемый бюджет']]

df_crm_coub_fin_grouped.to_excel(r'C:/Users/korpachev/BI_Reports/Output/NB_OpenBroker.xlsx',index=False)

print('NEW BI completed')