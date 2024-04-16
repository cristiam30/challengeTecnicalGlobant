import pandas as pd
import numpy as np

file_input = str(input("Digite la ruta local de los archivos a cargar: (departments.csv, hired_employees.csv, jobs.csv): "))
file_output = str(input("Digite la ruta local donde se exportaran los reportes: "))

df_departaments = pd.read_csv(file_input + 'departments.csv', sep=',', header=None, names =['Id','Departament'])
df_employees = pd.read_csv(file_input + 'hired_employees.csv', sep=',', header=None, names =['Id','Name','Datetime','Departament_id','Job_id'])
df_jobs = pd.read_csv(file_input + 'jobs.csv', sep=',', header=None, names =['Id','Job'])

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
pt.to_csv(file_output + 'challenge1.csv', sep=',')

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
dfChallenge2.to_csv(file_output + 'challenge2.csv', sep=',')