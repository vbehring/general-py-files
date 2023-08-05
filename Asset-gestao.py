# -*- coding: utf-8 -*-
"""
Created on Wed Mar  1 13:35:38 2023

@author: 01216333
"""
import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

connection = pyodbc.connect(r'Driver={SQL Server};Server=servername;Database=ServiceNow_VSC;Trusted_Connection=yes;')

cur = connection.cursor()

q1 = """
select  concat(cast(DATEPART(year, [u_vale_resolved_date]) as varchar(4)), ' - ',cast(DATEPART(week, [u_vale_resolved_date]) as varchar(2))) as Week_Number,
        AVG(DATEDIFF(hour, [opened_at], [u_vale_resolved_date])) as Avg_Hours_Task,
		count([u_vale_resolved_date]) as Number_Tickets
from [ServiceNow_VSC].[dbo].[sc_task]
where [dv_assignment_group] = 'ITIL Processes Hardware Asset Coordinator -GL'
and [u_vale_resolved_date] >= '2023-01-01'
and [dv_state] = 'Closed'
and [dv_assigned_to] not in ('System Administrator', 'Chave Processamento')

group by DATEPART(year, [u_vale_resolved_date]), DATEPART(week, [u_vale_resolved_date])
order by DATEPART(year, [u_vale_resolved_date]), DATEPART(week, [u_vale_resolved_date])
"""

dfavgtask = pd.read_sql(q1,connection)

plt.plot(dfavgtask['Week_Number'], dfavgtask['Avg_Hours_Task'], marker = 'o', mec = 'r')
plt.ylabel('Average Time(Hours)')
plt.xticks(rotation = 45)
plt.xlabel('Week Number')
plt.title('Average time to resolve Asset SR Tasks by week')
x = dfavgtask.index.to_numpy()
(m, b) = np.polyfit(x, dfavgtask['Avg_Hours_Task'],1)
yp1 = np.polyval([m, b], x)
plt.plot(x, yp1, color ='red')
plt.show()

(m, b) = np.polyfit(dfavgtask['Number_Tickets'], dfavgtask['Avg_Hours_Task'],1)
yp = np.polyval([m, b], dfavgtask['Number_Tickets'])
plt.plot(dfavgtask['Number_Tickets'], yp, color ='red')
plt.scatter(dfavgtask['Number_Tickets'], dfavgtask['Avg_Hours_Task'])
plt.ylabel('Average Time(Hours)')
plt.xlabel('Number of Tickes')
plt.show()

q1breakdown = """
select  [short_description],
concat(cast(DATEPART(year, [u_vale_resolved_date]) as varchar(4)), ' - ',cast(DATEPART(week, [u_vale_resolved_date]) as varchar(2))) as Week_Number,
        AVG(DATEDIFF(hour, [opened_at], [u_vale_resolved_date])) as Avg_Hours_Task,
		count([u_vale_resolved_date]) as Number_Tickets
from [ServiceNow_VSC].[dbo].[sc_task]
where [dv_assignment_group] = 'ITIL Processes Hardware Asset Coordinator -GL'
and [u_vale_resolved_date] >= '2023-01-01'
and [dv_state] = 'Closed'
and [dv_assigned_to] not in ('System Administrator', 'Chave Processamento')

group by [short_description], DATEPART(year, [u_vale_resolved_date]), DATEPART(week, [u_vale_resolved_date])
order by [short_description], DATEPART(year, [u_vale_resolved_date]), DATEPART(week, [u_vale_resolved_date])
"""

dftaskbd = pd.read_sql(q1breakdown,connection)

temp = dftaskbd.groupby("short_description")

for nametask, _ in temp:
    print(nametask)
    dftemp = dftaskbd[dftaskbd['short_description']==nametask]
    dftemp.reset_index(drop=True, inplace=True)
    plt.plot(dftemp['Week_Number'], dftemp['Avg_Hours_Task'], marker = 'o', mec = 'r')
    plt.ylabel('Average Time(Hours)')
    plt.xticks(rotation = 45)
    plt.xlabel('Week Number')
    plt.title(nametask)
    if len(dftemp)>1:
        x = dftemp.index.to_numpy()
        (m, b) = np.polyfit(x, dftemp['Avg_Hours_Task'],1)
        yp1 = np.polyval([m, b], x)
        plt.plot(x, yp1, color ='red')
    plt.show()
    
    plt.scatter(dftemp['Number_Tickets'], dftemp['Avg_Hours_Task'])
    plt.ylabel('Average Time(Hours)')
    plt.xlabel('Number of Tickes')
    plt.title(nametask)
    plt.show()

# Para verificar
# https://stackoverflow.com/questions/33150510/how-to-create-groupby-subplots-in-pandas

q2 = """
select concat(cast(DATEPART(year, [closed_at]) as varchar(4)), ' - ',cast(DATEPART(week, [closed_at]) as varchar(2))) as Week_Number,
		AVG(DATEDIFF(hour, [opened_at], [closed_at])) as Avg_Hours_Task,
		count([closed_at]) as Number_Tickets
from [ServiceNow_VSC].[dbo].[incident_task]
where dv_assignment_group = 'ITIL Processes Hardware Asset Coordinator -GL'
and dv_state = 'Closed Complete'
and [closed_at] >= '2023-01-01'
group by DATEPART(year, [closed_at]), DATEPART(week, [closed_at])
order by DATEPART(year, [closed_at]), DATEPART(week, [closed_at])
"""

dfavginc = pd.read_sql(q2,connection)

plt.plot(dfavginc['Week_Number'], dfavginc['Avg_Hours_Task'], marker = 'o', mec = 'r')
plt.ylabel('Average Time(Hours)')
plt.xticks(rotation = 45)
plt.xlabel('Week Number')
plt.title('Average time to resolve Asset Incident Tasks by week')
plt.show()

plt.scatter(dfavginc['Number_Tickets'], dfavginc['Avg_Hours_Task'])
plt.ylabel('Average Time(Hours)')
plt.xlabel('Number of Tickes')
plt.show()

q3 = """
Select
	(select count([AST_Serial_Number])
	FROM [ServiceNow_VSC].[dbo].[alm_asset_vale_lookup]
	where [AST_Country] = 'Brazil' and [AST_Status] = 'In use' and
	[AST_Ownership_Type] = 'Lease') as Total_Lease,

	(select count([AST_Serial_Number])
	FROM [ServiceNow_VSC].[dbo].[alm_asset_vale_lookup]
	where [AST_Country] = 'Brazil' and [AST_Status] = 'In use' and
	[AST_Ownership_Type] = 'Lease' 
	and [AST_End_Date] <= getdate()-90) as Total_Overdue,

	cast((select count([AST_Serial_Number])
	FROM [ServiceNow_VSC].[dbo].[alm_asset_vale_lookup]
	where [AST_Country] = 'Brazil' and [AST_Status] = 'In use' and
	[AST_Ownership_Type] = 'Lease' 
	and [AST_End_Date] <= getdate()-90) as float )
	/
	cast((select count([AST_Serial_Number])
	FROM [ServiceNow_VSC].[dbo].[alm_asset_vale_lookup]
	where [AST_Country] = 'Brazil' and [AST_Status] = 'In use' and
	[AST_Ownership_Type] = 'Lease') as float) * 100 as Ratio,

	format(getdate(), 'yyyy-MM-dd') as Date_Extraction
"""

df_leases = pd.read_csv('LeaseOverdue.csv', sep=';', encoding = 'utf-16', low_memory=False)

dflease = pd.read_sql(q3,connection)

df_leases = pd.concat([df_leases, dflease])

df_leases.drop_duplicates(keep = 'first', inplace = True)

df_leases.to_csv('LeaseOverdue.csv', sep=';', encoding = 'utf-16', index = False)

plt.plot(df_leases['Date_Extraction'], df_leases['Ratio'], marker = 'o', mec = 'r')
plt.ylabel('Percentage (%)')
plt.xticks(rotation = 45)
plt.xlabel('Date')
plt.title('Percentage of Overdue Machines in use')
plt.show()
