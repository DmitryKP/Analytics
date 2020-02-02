# coding=utf-8

import pandas as pd
import pyodbc

def multiinsert(cnxn, cursor, selector, values_list):
    for value in values_list:
        cursor.execute(selector, value)
        cnxn.commit()
        
cnxn = pyodbc.connect(Trusted_Connection='yes', driver = '{SQL Server}', server = 'xxx' , database = 'xxx')
cursor = cnxn.cursor()

dict_russian_month={'Январь': 1,'Февраль': 2,'Март': 3,'Апрель': 4,'Май': 5,'Июнь': 6,'Июль': 7,'Август': 8,'Сентябрь': 9,'Октябрь': 10,'Ноябрь': 11,'Декабрь': 12}


print('Подгрузка данных в METABASE')

print('New BI')
NB_OpenBroker_df= pd.read_excel(r'C:/Users/korpachev/BI_Reports/Output/NB_OpenBroker.xlsx',index=False)
NB_OpenBroker_df.replace({"month": dict_russian_month},inplace=True)
NB_OpenBroker_df['month'] = NB_OpenBroker_df['year'].astype(str) + '.' + NB_OpenBroker_df['month'].astype(str)
NB_OpenBroker_df['month'] = pd.to_datetime(NB_OpenBroker_df['month']).dt.strftime('%Y.%m')

list_of_tuples = [tuple(x) for x in NB_OpenBroker_df.values]

cursor.execute("DELETE FROM [MetabasePilot ].[Marketing].[New_BI]")
cnxn.commit()

selector = '''
INSERT INTO [MetabasePilot ].[Marketing].[New_BI]([year], [month], [Источник по новому_x_x], [Новое название кампании_x], [Sessions], [New Users], [Кол-во новых проспектов], [Кол-во новых контактов], [Зарегистрировано договоров БО], [К-во персон 5 - 50 т.р.], [К-во персон 50 - 100 т.р.], [ДС+ЦБ Ввод руб], [Фин рез П без НДС], [Cost], [Планируемый бюджет])
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
'''  
values_list = list_of_tuples

multiinsert(cnxn, cursor, selector, values_list)

if pd.read_sql("SELECT * FROM [MetabasePilot ].[Marketing].[New_BI]", cnxn).shape == NB_OpenBroker_df.shape:
    print('Все ок')
else:
    print('Не мэчится')

print('New BI Opentrainer')
NB_Opentrainer_df= pd.read_excel(r'C:/Users/korpachev/BI_Reports/Output/NB_Opentrainer.xlsx',index=False)
NB_Opentrainer_df.replace({"month": dict_russian_month},inplace=True)
NB_Opentrainer_df['month'] = NB_Opentrainer_df['year'].astype(str) + '.' + NB_Opentrainer_df['month'].astype(str)
NB_Opentrainer_df['month'] = pd.to_datetime(NB_Opentrainer_df['month']).dt.strftime('%Y.%m')

list_of_tuples = [tuple(x) for x in NB_Opentrainer_df.values]

cursor.execute("DELETE FROM [MetabasePilot ].[Marketing].[New_BI_Opentrainer]")
cnxn.commit()

selector = '''
INSERT INTO [MetabasePilot ].[Marketing].[New_BI_Opentrainer]([year], [month], [Источник по новому_x_x], [Новое название кампании_x], [Sessions], [New Users], [Кол-во новых проспектов], [Кол-во новых контактов], [Зарегистрировано договоров БО], [К-во персон 5 - 50 т.р.], [К-во персон 50 - 100 т.р.], [ДС+ЦБ Ввод руб], [Фин рез П без НДС], [Cost], [Планируемый бюджет],[created],[confirmed])
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
'''  
values_list = list_of_tuples

multiinsert(cnxn, cursor, selector, values_list)

if pd.read_sql("SELECT * FROM [MetabasePilot ].[Marketing].[New_BI_Opentrainer]", cnxn).shape == NB_Opentrainer_df.shape:
    print('Все ок')
else:
    print('Не мэчится')


print('Old BI')
Old_BI_Openbroker_df= pd.read_excel(r'C:/Users/korpachev/BI_Reports/Output/OB_OpenBroker.xlsx',index=False)
Old_BI_Openbroker_df.replace({"month_x": dict_russian_month},inplace=True)
Old_BI_Openbroker_df['month_x'] = Old_BI_Openbroker_df['year_x'].astype(str) + '.' + Old_BI_Openbroker_df['month_x'].astype(str) 
Old_BI_Openbroker_df['month_x'] = pd.to_datetime(Old_BI_Openbroker_df['month_x']).dt.strftime('%Y.%m')

list_of_tuples = [tuple(x) for x in Old_BI_Openbroker_df.values]

cursor.execute("DELETE FROM [MetabasePilot ].[Marketing].[Old_BI_Openbroker]")
cnxn.commit()

selector = '''
INSERT INTO [MetabasePilot ].[Marketing].[Old_BI_Openbroker]([Year], [Month], [SourceMedium_Type], [Campaign type], [Sessions], [New Users], [Кол-во новых проспектов], [Кол-во новых контактов], [Зарегистрировано договоров БО], [К-во персон 5 - 50 т.р.], [К-во персон 50 - 100 т.р.], [ДС+ЦБ Ввод руб], [Фин рез П без НДС], [Cost])
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
'''  
values_list = list_of_tuples

multiinsert(cnxn, cursor, selector, values_list)

if pd.read_sql("SELECT * FROM [MetabasePilot ].[Marketing].[Old_BI_Openbroker]", cnxn).shape == Old_BI_Openbroker_df.shape:
    print('Все ок')
else:
    print('Не мэчится')


print('Pages Report')
Pages_df= pd.read_excel(r'C:/Users/korpachev/BI_Reports/Output/Pages_Openbroker.xlsx',index=False).fillna(0,inplace=False)
Pages_df.replace({"Месяц": dict_russian_month},inplace=True)
Pages_df['Месяц'] = Pages_df['Год'].astype(str) + '.' + Pages_df['Месяц'].astype(str)
Pages_df['Месяц'] = pd.to_datetime(Pages_df['Месяц']).dt.strftime('%Y.%m')

list_of_tuples = [tuple(x) for x in Pages_df.values]

cursor.execute("DELETE FROM [MetabasePilot ].[Marketing].[Pages_report]")
cnxn.commit()

selector = '''
INSERT INTO [MetabasePilot ].[Marketing].[Pages_report]([Год], [Месяц], [Приоритетность], [Категория], [Категории Галимова], [Ссылка], [Просмотры страниц], [Консультации], [ДОСы], [Общее число заявок], [Конверсия по консультациям], [Конверсия по ДОСам], [Общая конверсия], [Кол-во новых проспектов], [Кол-во новых контактов], [Зарегистрировано договоров Б], [К-во персон 5 - 50 т.р.], [Активы руб на конец П], [ДС+ЦБ Ввод руб], [Фин рез П без НДС], [avgtime], [bounces], [exits])
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
'''  
values_list = list_of_tuples

multiinsert(cnxn, cursor, selector, values_list)

if pd.read_sql("SELECT * FROM [MetabasePilot ].[Marketing].[Pages_report]", cnxn).shape == Pages_df.shape:
    print('Все ок')
else:
    print('Не мэчится')

print('Prediction_TS')

Prediction_df= pd.read_excel(r'C:/Users/korpachev/BI_Reports/Output/Prediction_TS.xlsx',index=False)

pr_notnull = Prediction_df[~Prediction_df['y'].isnull()]
pr_null = Prediction_df[Prediction_df['y'].isnull()][['ds','trend']]

list_of_tuples_notnull = [tuple(x) for x in pr_notnull.values]
list_of_tuples_null = [tuple(x) for x in pr_null.values]


cursor.execute("DELETE FROM [MetabasePilot ].[Marketing].[Prediction_TimeSeries]")
cnxn.commit()

selector = '''
INSERT INTO [MetabasePilot ].[Marketing].[Prediction_TimeSeries]([Date], [Req_f], [Req_p])
VALUES (?, ?, ?);
'''  
values_list = list_of_tuples_notnull

multiinsert(cnxn, cursor, selector, values_list)

selector = '''
INSERT INTO [MetabasePilot ].[Marketing].[Prediction_TimeSeries]([Date], [Req_f])
VALUES (?, ?);
'''  
values_list = list_of_tuples_null

multiinsert(cnxn, cursor, selector, values_list)



if pd.read_sql("SELECT * FROM [MetabasePilot ].[Marketing].[Prediction_TimeSeries]", cnxn).shape == Prediction_df.shape:
    print('Все ок')
else:
    print('Не мэчится')

print('Не мэчится из-за Nkont_f, Nkont_p, ND_f, ND_p, NK_f, NK_p, NK50_f, NK50_p')