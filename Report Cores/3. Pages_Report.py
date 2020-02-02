print('Pages started')

import pandas as pd
import numpy as np
import difflib
from fuzzywuzzy import fuzz
from google.cloud import bigquery
import win32com.client
import pyodbc
import re
import warnings
from datetime import datetime
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import json 
import datetime as dt

warnings.filterwarnings("ignore")

client = bigquery.Client.from_service_account_json(r'xxx.json')
connection_DB_CRM = pyodbc.connect(Trusted_Connection='yes', driver = '{SQL Server}',server = 'xxx' , database = 'xxx')

print('Подгрузка и обработка CRM')
sql_crm="SELECT * FROM [datamartMarketing].[vwCrmProspect]"
CRM_all = pd.read_sql(sql_crm,connection_DB_CRM)

CRM_all['year'] = pd.DatetimeIndex(CRM_all['date_creation']).year
CRM_all['month'] = pd.DatetimeIndex(CRM_all['date_creation']).month

CRM_1 = CRM_all[(CRM_all['year']>=2018) & (~CRM_all['source_form_url'].str.contains('dev.|dev3|localhost|obc/ru|ssr-dv.|open-am.|landings.loc|/open.ru'))]
CRM_1=CRM_1[CRM_1['marketing_campaign_name']!='']
CRM_1=CRM_1[~(CRM_1['source_form_url'].str.contains("opentrainer"))]

CRM_2=CRM_1[((CRM_1['source_form_url']!='')&(~CRM_1['marketing_campaign_name'].str.contains('Триал|триал')))|(CRM_1['marketing_campaign_name'].str.contains('Триал'))]
CRM_2.loc[(CRM_2.source_form_url.str.contains('.open-am.ru/'))&(CRM_2.marketing_campaign_name == 'Открытие Инвестиции'), 'source_form_url'] = CRM_2.http_referer

dict_russian_month = {1:'Январь',2:'Февраль',3:'Март',4:'Апрель',5:'Май',6:'Июнь',7:'Июль',8:'Август',9:'Сентябрь',10:'Октябрь',11:'Ноябрь',12:'Декабрь'}
CRM_2.replace({"month": dict_russian_month}, inplace=True)

print('Подгрузка COUB')
coub_old = pd.read_excel(r'C:\Users\korpachev\BI_Reports\Coub1_2016_2018.xlsx')
coub_old.fillna(0, inplace=True)
coub_new = pd.read_excel(r'C:\Users\korpachev\BI_Reports\Coub1_2019.xlsx')
coub_new.fillna(0,inplace=True)

df_COUB_GUID_Dirty = pd.concat([coub_old, coub_new], axis=0)
del coub_old
del coub_new

df_COUB_GUID = df_COUB_GUID_Dirty[~df_COUB_GUID_Dirty['GUID'].isnull()]


VIEW_ID = 'xxx'
credentials = ServiceAccountCredentials.from_json_keyfile_name(
            r'xxx.json', scopes=['https://www.googleapis.com/auth/analytics'])
service = build('analyticsreporting', 'v4', credentials=credentials)

now_date = dt.datetime.today().strftime("%Y-%m-%d")
start_dates = [str(e).split(' ')[0] for e in pd.date_range(start='2018-01-01', end=now_date, freq=pd.offsets.MonthBegin())]
end_dates = [str(e).split(' ')[0] for e in pd.date_range(start='2018-01-01', end=now_date, freq=pd.offsets.MonthEnd())]
if len(start_dates) != len(end_dates):
    end_dates = end_dates + [now_date]

bq_Pages_stat = []
for i,e in enumerate(start_dates):
    response = service.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'pageSize': 100000000,
          'dateRanges': [{'startDate': e, 'endDate': end_dates[i] }],
          'metrics': [{'expression': 'ga:pageviews'},{'expression': 'ga:uniquePageviews'},{'expression': 'ga:avgTimeOnPage'},{'expression': 'ga:bounceRate'},{'expression': 'ga:exitRate'}],
          'dimensions': [{'name': 'ga:yearMonth'}, {'name': 'ga:pagePath'}],
          "dimensionFilterClauses": [
            {
              "filters": [
                {
                  "dimensionName": "ga:pagePath",
                  "operator": "BEGINS_WITH",
                  "expressions": ["open-broker.ru"]
                }
              ]
            }
          ]
        }
      ]
    }
    ).execute()
    data = response.get('reports', [])[0].get('data', {}).get('rows', [])
    GA_Stat = pd.DataFrame(data)
    bq_Pages_stat.append(GA_Stat)

bq_Pages_stat = pd.concat(bq_Pages_stat,axis=0)
bq_Pages_stat['date'] = bq_Pages_stat['dimensions'].apply(lambda x: x[0])
bq_Pages_stat['Page'] = bq_Pages_stat['dimensions'].apply(lambda x: x[1])
bq_Pages_stat['Pageviews'] = bq_Pages_stat['metrics'].apply(lambda x: float(x[0]['values'][0]))
bq_Pages_stat['UniquePageviews'] = bq_Pages_stat['metrics'].apply(lambda x: float(x[0]['values'][1]))
bq_Pages_stat['AvgTimeOnPage'] = bq_Pages_stat['metrics'].apply(lambda x: float(x[0]['values'][2]))
bq_Pages_stat['BounceRate'] = bq_Pages_stat['metrics'].apply(lambda x: float(x[0]['values'][3]))
bq_Pages_stat['ExitRate'] = bq_Pages_stat['metrics'].apply(lambda x: float(x[0]['values'][4]))
bq_Pages_stat.drop(['dimensions','metrics'], axis=1, inplace=True)

bq_Pages_stat.replace(0,np.NaN,inplace=True)
bq_Pages_stat0 = bq_Pages_stat[~bq_Pages_stat['Page'].str.contains("opentrainer",na=False)]
bq_Pages_stat0['year'] = bq_Pages_stat0['date'].apply(lambda x:int(str(x)[:4]))
bq_Pages_stat0['month'] = bq_Pages_stat0['date'].apply(lambda x:int(str(x)[4:]))
bq_Pages_stat0.replace({"month": dict_russian_month}, inplace=True)

#Обработка URLs в CRM и GA

urls_m=list(CRM_2['marketing_campaign_name'])
urls=list(CRM_2['source_form_url'])
unique_match=[]
for index,e in enumerate(urls):
    if str(e)=='':
        if str(urls_m[index]).count('2019')!=0:
            unique_match.append('open-broker.ru/investor-2019/')
        elif str(urls_m[index]).count('2018')!=0:
            unique_match.append('open-broker.ru/investor-2018/')
    else:
        tempo_e=' '+str(e)
        tempo_re=re.sub('\s(https://|http://)','', str(tempo_e))
        tempo_re0=re.sub(r'((\?|\#).+|\#|\?)','', str(tempo_re))
        if tempo_re0.count('lp/investor')!=0 and tempo_re0.count('lp/investor-2018')==0: 
            tempo_re00=re.sub(r'(lp/investor)','lp/investor-2018', str(tempo_re0))
        else:
            tempo_re00=tempo_re0
        tempo_re01 = re.sub('(\s|lp\/3\.0\/|lp\/|undefined)','', str(tempo_re00))
        if tempo_re01[len(tempo_re01)-1]=='/':
            tempo_re_fin=str(tempo_re01)
        else:
            tempo_re_fin=str(tempo_re01)+'/'
        tempo_re_fin_1=re.sub(r'\/\/|\/\s\/','/', str(tempo_re_fin))
        tempo_re_fin_2_1=re.sub(r'.ru/ru/','.ru/', str(tempo_re_fin_1))
        tempo_re_fin_2_2=re.sub(r'(\/faq).+|(\/faq)','/faq/', str(tempo_re_fin_2_1))
        tempo_re_fin_2=re.sub(r'(\/event\-).+','/event/', str(tempo_re_fin_2_2))
        tempo_re_fin_2_3=re.sub(r'(\/issuers).+|(\/issuers)','/issuers/', str(tempo_re_fin_2))
        unique_match.append(tempo_re_fin_2_3)
CRM_2['Ссылка_очищенная'] = unique_match

list_bq=list(bq_Pages_stat0['Page'])
unique_match_bq=[]
for tempos_re in list_bq:
    tempos_re1=re.sub(r'((\?|\#|welcome&|utm|csi&|vas&pact|~).+|\#|\?|lp\/3\.0\/|lp\/|undefined)','', str(tempos_re))
    if tempos_re1[len(tempos_re1)-1]=='/':
        tempos_re_fin=str(tempos_re1)
    elif tempos_re1[len(tempos_re1)-1]=='/' and tempos_re1[len(tempos_re1)-2]=='/':
        tempos_re_fin=str(tempos_re1)[0:len(str(tempos_re1))-1]
    else:
        tempos_re_fin=str(tempos_re1)+'/'
    tempos_re_fin_1=re.sub(r'\/\/|\/\s\/','/', str(tempos_re_fin))
    tempos_re_fin_2_1=re.sub(r'.ru/ru/','.ru/', str(tempos_re_fin_1))
    tempos_re_fin_2_2=re.sub(r'(\/faq).+|(\/faq)','/faq/', str(tempos_re_fin_2_1))
    tempos_re_fin_2=re.sub(r'(\/event\-).+','/event/',str(tempos_re_fin_2_2))
    tempos_re_fin_2_3=re.sub(r'(\/issuers).+|(\/issuers)','/issuers/', str(tempos_re_fin_2))
    unique_match_bq.append(tempos_re_fin_2_3)
bq_Pages_stat0['Ссылка_очищенная'] = unique_match_bq



print('Подгрузка справочника для страниц не с Remote')
spr_URL=pd.read_excel(r'C:\Users\korpachev\BI_Reports\Sprаvochnik_URLS_(NoDOS).xlsx')
NE_DOS_DF = CRM_2[CRM_2['Ссылка_очищенная']!='open-broker.ru/remote-opening/'].merge(spr_URL,on='Ссылка_очищенная',how='left')
new_urls = NE_DOS_DF[NE_DOS_DF['Приоритетность'].isnull()]['Ссылка_очищенная'].unique()

list_dfs= []
for e in new_urls:
    dft = CRM_2[CRM_2['Ссылка_очищенная'].isin(new_urls)].groupby(['Ссылка_очищенная','marketing_campaign_name'],as_index=False)['GUID'].count()
    if len(dft['marketing_campaign_name']) >= 1:
        list_dfs.append(dft['marketing_campaign_name'][0])

new_urls_DF = pd.DataFrame (
    {'Ссылка_очищенная': new_urls,
     'Категория страницы': list_dfs,
     'Мастер Категория': 'Автоопределимая',
     'Приоритетность': 'Средняя',
     'Ссылка_Fin': new_urls
    } )

spr_URL = pd.concat( [spr_URL, new_urls_DF], axis=0 )
spr_URL.to_excel(r'C:\Users\korpachev\BI_Reports\Sprаvochnik_URLS_(NoDOS).xlsx',index=False)
NE_DOS_DF_FIN = CRM_2[CRM_2['Ссылка_очищенная']!='open-broker.ru/remote-opening/'].merge(spr_URL,on='Ссылка_очищенная',how='left')
NE_DOS_DF_FIN['DOS'] = 'Not'


print('Подгрузка справочника для страниц с Remote')
spr_MC=pd.read_excel(r'C:\Users\korpachev\BI_Reports\Sprаvochnik_Marketing_Campaigns(DOS).xlsx')
DOS_DF = CRM_2[CRM_2['Ссылка_очищенная']=='open-broker.ru/remote-opening/'].merge(spr_MC,on='marketing_campaign_name',how='left')
new_mark_camps = DOS_DF[DOS_DF['Приоритетность'].isnull()]['marketing_campaign_name'].unique()

list_dfs= []
for e in new_mark_camps:
    dft = CRM_2[CRM_2['marketing_campaign_name'].isin(new_mark_camps)].groupby(['marketing_campaign_name','Ссылка_очищенная'],as_index=False)['GUID'].count()[['marketing_campaign_name','Ссылка_очищенная']]
    list_dft = [e for e in dft[dft['marketing_campaign_name'] == e]['Ссылка_очищенная'].unique() if e != 'open-broker.ru/remote-opening/']
    if list_dft == []:
        list_dfs.append('Внешний источник')#('Внешний источник с МК: {}'.format(str(e)))
    else:
        list_dfs.append(list_dft[0])

new_mark_camps_DF = pd.DataFrame (
    {'marketing_campaign_name': new_mark_camps,
     'Категория страницы': new_mark_camps,
     'Мастер Категория': 'Автоопределимая',
     'Приоритетность': 'Средняя',
     'Ссылка_Fin': list_dfs
    } )

spr_MC = pd.concat( [spr_MC, new_mark_camps_DF], axis=0 )
spr_MC.to_excel(r'C:\Users\korpachev\BI_Reports\Sprаvochnik_Marketing_Campaigns(DOS).xlsx',index=False)
DOS_DF_FIN = CRM_2[CRM_2['Ссылка_очищенная']=='open-broker.ru/remote-opening/'].merge(spr_MC,on='marketing_campaign_name',how='left')
DOS_DF_FIN['DOS'] = 'Yes'

SQL_CRM_all5 = pd.concat( [NE_DOS_DF_FIN, DOS_DF_FIN], axis=0 )

SQL_CRM_all5['Консультации'] = 0
SQL_CRM_all5['ДОСы'] = 0

SQL_CRM_all5.loc[ SQL_CRM_all5['DOS'] == 'Not', 'Консультации'] = 1
SQL_CRM_all5.loc[ SQL_CRM_all5['DOS'] == 'Yes', 'ДОСы'] = 1


#Группировка Консультации/ДОСов
crm_groupby = SQL_CRM_all5.groupby(['year','month','Приоритетность','Мастер Категория','Категория страницы','Ссылка_Fin'],as_index=False)[['Консультации','ДОСы']].sum()
crm_groupby['conc'] =  crm_groupby[['year','month','Приоритетность','Мастер Категория','Категория страницы','Ссылка_Fin']].astype(str).values.sum(axis=1)

#Группировка финансовых данных кроме активов
coub=SQL_CRM_all5.merge(df_COUB_GUID,on='GUID',how='left').fillna(0)
coub_groupby=coub.groupby(['year','month','Приоритетность','Мастер Категория','Категория страницы','Ссылка_Fin'],as_index=False)[['Кол-во новых проспектов', 'Кол-во новых контактов', 'Зарегистрировано договоров БО', 'К-во персон 5 - 50 т.р.', 'ДС+ЦБ Ввод руб', 'Фин рез П без НДС', 'К-во персон 50 - 100 т.р.']].sum()
coub_groupby['conc'] =  coub_groupby[['year','month','Приоритетность','Мастер Категория','Категория страницы','Ссылка_Fin']].astype(str).values.sum(axis=1)

#Группировка активов
actives=SQL_CRM_all5.merge(df_COUB_GUID,on='GUID',how='left').fillna(0).drop_duplicates(subset='GUID', keep="last")
actives_groupby=actives.groupby(['year','month','Приоритетность','Мастер Категория','Категория страницы','Ссылка_Fin'],as_index=False)[['Активы руб на конец П']].sum()
actives_groupby['conc'] =  actives_groupby[['year','month','Приоритетность','Мастер Категория','Категория страницы','Ссылка_Fin']].astype(str).values.sum(axis=1)


#Группировка GA данных
#bq_groupby=bq_Pages_stat0.merge(spr_URL,on='Ссылка_очищенная',how='left').groupby(['year','month','Приоритетность','Мастер Категория','Категория страницы','Ссылка_Fin'],as_index=False)['Pageviews'].sum()
#ga_gr = bq_Pages_stat0.merge(spr_URL,on='Ссылка_очищенная',how='left').groupby(['year','month','Приоритетность','Мастер Категория','Категория страницы','Ссылка_Fin'],as_index=False).agg({'Pageviews': 'sum', 'UniquePageviews': 'sum', 'AvgTimeOnPage': 'mean','BounceRate': 'mean','ExitRate': 'mean'})
#дебаггинг новой статистики bq_groupby[(bq_groupby['year'] == 2019)&(bq_groupby['month'] == 'Август')&(bq_groupby['Ссылка_Fin'] == 'open-broker.ru/learning/trading-demo-account/')]

bq_groupby = bq_Pages_stat0.merge(spr_URL,on='Ссылка_очищенная',how='left').groupby(['year','month','Приоритетность','Мастер Категория','Категория страницы','Ссылка_Fin'],as_index=False).agg({'Pageviews': 'sum', 'AvgTimeOnPage':'mean', 'BounceRate':'mean', 'ExitRate': 'mean'})
bq_groupby['conc'] =  bq_groupby[['year','month','Приоритетность','Мастер Категория','Категория страницы','Ссылка_Fin']].astype(str).values.sum(axis=1)

#Слияние таблиц
total_merge = crm_groupby.merge(coub_groupby,on='conc',how='left').merge(actives_groupby,on='conc',how='left').merge(bq_groupby,on='conc',how='left')
total_merge = total_merge[['year_x','month_x','Приоритетность_x','Мастер Категория_x','Категория страницы_x','Ссылка_Fin_x','Pageviews','Консультации', 'ДОСы', 'Кол-во новых проспектов','Кол-во новых контактов', 'Зарегистрировано договоров БО','К-во персон 5 - 50 т.р.', 'К-во персон 50 - 100 т.р.', 'ДС+ЦБ Ввод руб', 'Фин рез П без НДС','Активы руб на конец П','AvgTimeOnPage','BounceRate', 'ExitRate']]
total_merge = total_merge.loc[:, ~total_merge.columns.duplicated()]

total_merge['Общее число заявок']=total_merge['Консультации'].astype(int) + total_merge['ДОСы'].astype(int)
total_merge['Конверсия по консультациям']=total_merge['Консультации']/total_merge['Pageviews']
total_merge['Конверсия по ДОСам'] = total_merge['ДОСы'] / total_merge['Pageviews']
total_merge['Общая конверсия'] = total_merge['Общее число заявок'] / total_merge['Pageviews']

total_merge.rename(columns={'year_x':'Год','month_x':'Месяц','Приоритетность_x':'Приоритетность','Мастер Категория_x':'Категория','Категория страницы_x':'Категории Галимова','Ссылка_Fin_x':'Ссылка','Pageviews':'Просмотры страниц','Зарегистрировано договоров БО':'Зарегистрировано договоров Б'}, inplace=True)
total_merge = total_merge[['Год', 'Месяц', 'Приоритетность', 'Категория', 'Категории Галимова', 'Ссылка', 'Просмотры страниц', 'Консультации', 'ДОСы', 'Общее число заявок', 'Конверсия по консультациям', 'Конверсия по ДОСам', 'Общая конверсия', 'Кол-во новых проспектов', 'Кол-во новых контактов', 'Зарегистрировано договоров Б', 'К-во персон 5 - 50 т.р.', 'Активы руб на конец П', 'ДС+ЦБ Ввод руб', 'Фин рез П без НДС','AvgTimeOnPage','BounceRate', 'ExitRate']]
total_merge.fillna(0,inplace=True)
total_merge.loc[total_merge['Ссылка'] == 'Внешний источник', 'Ссылка'] = 'Внешний переход: '+ total_merge['Категории Галимова']
total_merge['Приоритетность'] = total_merge['Ссылка']

total_merge.to_excel(r'C:/Users/korpachev/BI_Reports/Output/Pages_Openbroker.xlsx',index=False)

print('Pages completed')