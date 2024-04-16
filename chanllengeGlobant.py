import pandas as pd
import numpy as np


df_departaments = pd.read_csv('/Users/Cristiam/Documents/Globant/departments.csv', sep=',', header=None, names =['Id','Departament'])
df_employees = pd.read_csv('/Users/Cristiam/Documents/Globant/hired_employees.csv', sep=',', header=None, names =['Id','Name','Datetime','Departament_id','Job_id'])
df_jobs = pd.read_csv('/Users/Cristiam/Documents/Globant/jobs.csv', sep=',', header=None, names =['Id','Job'])

# 2021 filter year
df_employees['Datetime'] = pd.to_datetime(df_employees['Datetime'])
df_employees2021 = df_employees[df_employees['Datetime'].dt.strftime('%Y') == '2021']

# Join DataFrames 2021
df_employees2021dpt = pd.merge(df_employees2021, df_departaments, left_on='Departament_id', right_on='Id',how='left')
dfEmp2021 = pd.merge(df_employees2021dpt, df_jobs, left_on='Job_id', right_on='Id',how='left')

# Quarter
dfEmp2021['Quarter'] = dfEmp2021['Datetime'].dt.quarter

# pivot Chanllenge 1
pt = pd.pivot_table(dfEmp2021, values='Name', index=['Departament','Job'],
                    columns=['Quarter'], aggfunc="count", fill_value=0)

#Export Report 1
print('Report #1: \n')
print(pt)
pt.to_csv('/Users/Cristiam/Documents/Globant/challenge/projectNotebbok/challenge1.csv', sep=',')

#mean 2021
mean2021 = dfEmp2021.groupby(by = ["Departament"]).count()["Id_x"].mean()

#Join DataFRame
df_employees_t = pd.merge(df_employees, df_departaments, left_on='Departament_id', right_on='Id',how='left')
dfEmp_total = pd.merge(df_employees_t, df_jobs, left_on='Job_id', right_on='Id',how='left')

#Count
dfCant = dfEmp_total.groupby(by = ["Departament_id","Departament"]).count()["Id_x"]

dfChallenge2 = dfCant[dfCant>mean2021].sort_values(ascending=False)

#Export Report 2
print('Report #2: \n')
print(dfChallenge2)
dfChallenge2.to_csv('/Users/Cristiam/Documents/Globant/challenge/projectNotebbok/challenge2.csv', sep=',')