# -*- coding: utf-8 -*-
"""
Created on Fri Feb  9 09:06:51 2018

@author: 01216333
"""

#import pandas as pd
import pyodbc
import datetime
import os

connection = pyodbc.connect(r'Driver={SQL Server};Server=server_name;Database=Remedy_VSC;Trusted_Connection=yes;')
cursor = connection.cursor()

dict_status = {0:'Assigned', 1:'Pending', 2:'Waiting Approval', 3:'Planning', 4:'In Progress', 5:'Completed', 6:'Rejected', 7:'Cancelled', 8:'Closed'}
dict_req = {1000: 'Draft', 1800: 'Submitted', 2000: 'Pending', 3000: 'Waiting Approval',
            4000: 'Initiated', 5000: 'In Progress', 6000: 'Completed', 
            7000: 'Rejected', 8000: 'Cancelled', 9000: 'Closed'}



def my_custom_sql(wonum):
    cursor.execute("""SELECT [CustomerFullName], [Submit_Date], [Status], [Summary], [ASORG], [ASCPY], [ASGRP], [Chg_Location_Address],
	[Detailed_Description], [SRID]
	FROM [Remedy_VSC].[dbo].[WOI_WorkOrder] WHERE [Work_Order_ID] = '%s'"""%(wonum))
    row = cursor.fetchone()
    return row

def req_sql(wonum):
    cursor.execute("""SELECT [Request_Number]
      ,[Submit_Date]
      FROM [Remedy_VSC].[dbo].[SRM_Request]
      where [Request_Number] = '%s'"""%(wonum))
    row = cursor.fetchone()
    return row

def reqfull_sql(reqnum):
    cursor.execute("""SELECT [Request_Number]
	  ,[Submit_Date]
      ,[AppRequestID]
      ,[TitleFromSRD]
      ,[Status]
      ,[Closed_Date]
      ,[Approval_Date]
      ,[Customer_Full_Name]
      ,[Internet_E_mail]
      ,[z1D_Approver]
  FROM [Remedy_VSC].[dbo].[SRM_Request]
  where [Request_Number] = '%s'"""%(reqnum))
    row = cursor.fetchone()
    return row
    
def inc_sql(incnum):
    cursor.execute("""SELECT  [Incident_Number]
		,[Submit_Date]
		,[SRID]
		,[Customer_Login_ID]
		,[Detailed_Decription]
		,[z1D_Template_Name]
		,[Assigned_Support_Company]
		,[Assigned_Support_Organization]
		,[Assigned_Group]
		,[vale_Status]
		,[vale_Status_Reason]
		,[Resolution]
      ,[vale_impact]
	  ,[vale_Urgency]
	  ,[vale_Priority]
	  ,[vale_Reported_Source]
  FROM [Remedy_VSC].[dbo].[HPD_Help_Desk]
  where Incident_Number = '%s'"""%(incnum))
    row = cursor.fetchone()
    return row

def get_wo_sql(reqnum):
    cursor.execute("""SELECT WO.Work_Order_ID, WO.Summary 
        FROM [Remedy_VSC].[dbo].[WOI_WorkOrder] WO 
        where WO.SRID = '%s'"""%(reqnum))
    row = cursor.fetchall()
    return row
    
def get_wo_workdetail_sql(wonum):
    cursor.execute("""SELECT [Detailed_Description]
      ,[Work_Log_Submit_Date]
      FROM [Remedy_VSC].[dbo].[WOI_WorkInfo]
      where [Work_Order_ID] = '%s'
      order by [Work_Log_Submit_Date]"""%(wonum))
    row = cursor.fetchall()
    return row
    
def get_inc_workdetail_sql(incnum):
    cursor.execute("""SELECT [Detailed_Description]
      ,[Work_Log_Submit_Date]
      ,[Work_Log_Submitter]
  FROM [Remedy_VSC].[dbo].[HPD_WorkLog]
  where Incident_Number = '%s'
  order by [Work_Log_Submit_Date]"""%(incnum))
    row = cursor.fetchall()
    return row

def get_inc_sql(reqnum):
    cursor.execute("""SELECT  [Incident_Number]
    FROM [Remedy_VSC].[dbo].[HPD_Help_Desk]
    where [SRID] = '%s'"""%(reqnum))
    row = cursor.fetchall()
    return row    
    
def get_req_appr_sql(reqnum):
    cursor.execute("""SELECT [Approval_Audit_Trail]
        FROM [Remedy_VSC].[dbo].[SRM_RequestApDetailSignature]
        where [Request_Number] = '%s'
        order by [Create_Date_Sig]"""%(reqnum))
    row = cursor.fetchall()
    return row

def get_fullname_sql(id):
    cursor.execute("""SELECT [Full_Name]
  FROM [Remedy_VSC].[dbo].[CTM_People]
  where [Remedy_Login_ID] = '%s'"""%(id))
    row = cursor.fetchone()
    return row
    
def appr_transf(appr):
    resp = ''
    if appr[0] is None:
        return resp
    tempstr = appr[0].replace('\x04','\n').replace('\x03','\n').replace('\x00','')
    tempstr = tempstr.split('\n')
    for i in range(0, len(tempstr)):
        if len(tempstr[i]) == 10 and tempstr[i].isdigit():
            tempstr[i] = datetime.datetime.fromtimestamp(int(tempstr[i])).strftime('%Y-%m-%d %H:%M:%S')
            name = get_fullname_sql(tempstr[i+1])
            if name is not None:
                tempstr[i+1] = name[0]
        resp += tempstr[i] + '\n'
    return resp
    
def get_wo_relationship(reqnum):
    cursor.execute("""Select origem.Work_Order_ID as WoOriginal,
	   relacionamento.Work_Order_ID as WoRelacionada,
	   origem.SRID as REQOriginal
        From [Remedy_VSC].[dbo].[WOI_WorkOrder] origem
       join [Remedy_VSC].[dbo].[WOI_WorkOrderRelationshipInter] relacionamento on (origem.Work_Order_ID = relacionamento.Request_ID01) 
       join [Remedy_VSC].[dbo].[WOI_WorkOrder] relacionada on (relacionamento.Work_Order_ID = relacionada.Work_Order_ID)
       Where origem.SRID = '%s'"""%(reqnum))
    row = cursor.fetchall()
    return row

def get_wo_parent(wonum):
    cursor.execute("""Select origem.Work_Order_ID as WoOriginal,
	   relacionamento.Work_Order_ID as WoRelacionada,
	   origem.SRID as REQOriginal
        From [Remedy_VSC].[dbo].[WOI_WorkOrder] origem
       join [Remedy_VSC].[dbo].[WOI_WorkOrderRelationshipInter] relacionamento on (origem.Work_Order_ID = relacionamento.Request_ID01) 
       join [Remedy_VSC].[dbo].[WOI_WorkOrder] relacionada on (relacionamento.Work_Order_ID = relacionada.Work_Order_ID)
       Where relacionamento.Work_Order_ID = '%s'"""%(wonum))
    row = cursor.fetchone()
    return row
    
def get_pending_appr(reqnum):
    cursor.execute("""SELECT  FullName = [Remedy_VSC].DBO.FN_TIRA_ACENTOS(a.FullName),
         a.Approvers
  FROM [Remedy_VSC].[dbo].[SRM_RequestApDetailSignature] a 
  LEFT OUTER JOIN [Remedy_VSC].[dbo].[SRM_Request] b on a.Request_Number = b.Request_Number
  where a.Sig_TermState_Date is NULL and a.Approval_Status = 0
  and a.Request_Number = '%s'"""%(reqnum))
    row = cursor.fetchall()
    return row

def display_req(reqnum):
    f = open('REQ_Dump.txt', 'w')
    pending_appr_list = []
    pending_appr = []
    out = ''
    appr_list = ''
    wo_list = []
    inc_list = []
    req = reqfull_sql(reqnum)
    if req is None:
        print('Req not found')
    else:
        for i in range(len(req)):
            if req[i] is None:
                req[i] = ''
        if req[5] != '':
            req[5] = req[5].strftime('%Y-%m-%d %H:%M')
        if req[6] != '':
            req[6] = req[6].strftime('%Y-%m-%d %H:%M')
        out += 'Request: ' + req[0] + '\nSubmit Date: ' + req[1].strftime('%Y-%m-%d %H:%M') + \
        '\nTitle: ' + req[3] + '\nStatus: ' + dict_req[req[4]] + '\nClosed Date: ' + \
        req[5] + '\nApproval Date: ' + req[6] + '\nRequested For: ' + req[7] + '\nRequested By: ' + req[8]

        apprs = get_req_appr_sql(reqnum)
        pending_appr = get_pending_appr(reqnum)
        wos = get_wo_sql(reqnum)
        incs = get_inc_sql(reqnum)
        if apprs is not None:
            for appr in apprs:
                appr_list += appr_transf(appr)    
        if wos is not None:
            for wo in wos:
                wo_list.append(wo[0])
        if incs is not None:
            for inc in incs:
                inc_list.append(inc[0])
        if pending_appr is not None:
            for i in pending_appr:
                pending_appr_list.append(i[0])
    f.write(out)
    if len(appr_list) > 0:
        f.write('\n\n=========== Approval ===========\n\n')
        f.write(appr_list)
    if len(pending_appr) > 0:
        f.write('\n\n=========== Pending Approval ===========\n\n')
        f.write(' '.join(pending_appr_list).replace('\x00' , '\n\n'))
    if len(wo_list) > 0:
        f.write('\n\n=========== Work Orders ============\n\n')
        for i in wo_list:
            f.write(display_wo(i))
        f.close()
    if len(inc_list) > 0:
        f.write('\n\n=========== Incident ============\n\n')
        for i in inc_list:
            f.write(display_inc(i))
        f.close()
    
def display_wo(wonum):
    wodet_list = []
    out = ''
    temp = my_custom_sql(wonum)
    wodet = get_wo_workdetail_sql(wonum)
    for i in wodet:
        wodet_list.append('Log Entry Date: ' + str(i[1]) + '\n' + i[0])
    
    out = '\n----\nWO: ' + wonum + '\nWO Submit Date: ' + temp[1].strftime('%Y-%m-%d %H:%M') + \
    '\nWO Status: ' + dict_status[temp[2]] +  '\nSummary' + temp[3] + \
    '\nCompany: ' + temp[4] +  '\nOrganization: ' + temp[5] + \
    '\nGroup: ' + temp[6] +  '\nLocation: ' +  temp[7] +  \
    '\n\n============== Description ================\n' +  str(temp[8]) + \
    '\n\n==============Work Details=============== \n\n' + ' '.join(wodet_list).replace('\x00' , '\n\n')
    return out
    #print(' '.join(wodet_list).replace('\x00' , '\n\n'))
    
def display_inc(incnum):
    out = ''
    incdetail_list = []
    temp = inc_sql(incnum)
    for i in range(len(temp)):
        if temp[i] is None:
            temp[i] = ''
    name = get_fullname_sql(temp[3])
    if name is not None:
        temp[3] = name[0]
    incdet = get_inc_workdetail_sql(incnum)
    for i in incdet:
        incdetail_list.append('\nLog Entry Date: ' + i[1].strftime('%Y-%m-%d %H:%M') + '\n' + 'Submitter:' + i[2] + '\n' + i[0])
    out = '\n----\nINC Number: ' + incnum + '\nRequester: ' + temp[3] + \
    '\nSubmit Date: ' + temp[1].strftime('%Y-%m-%d %H:%M') + \
    '\nStatus: ' + temp[9] + '\nResolution: ' + temp[11] + '\nUrgemcy: ' + temp[13] + \
    '\nPriority: ' + temp[14] + '\nImpact: ' + temp[12] + '\nSource: ' + temp[15] +\
    '\n\n==============Work Details=============== \n\n' + ' '.join(incdetail_list).replace('\x00' , '\n\n')
    return out
    
def get_reqnum(ticket):
    if ticket[0:3].upper() == 'REQ':
        return ticket
    if ticket[0:3].upper() == 'INC':
        temp = inc_sql(ticket)
        return temp[2]
    if ticket[0:2].upper() == 'WO':
        temp = my_custom_sql(ticket)
        return temp[9]

ticket = 'REQ000014681703'
#print(get_reqnum(ticket))

display_req(get_reqnum(ticket))
#display_inc('INC000002931219')
osCommandString = "notepad.exe REQ_Dump.txt"
os.system(osCommandString)
