# -*- coding: utf-8 -*-
"""
Created on Tue Jul 20 13:16:22 2021

@author: 01216333
Versão 20220822 - Correção no merge de tabelas para pegar alterações de paises
"""

import pandas as pd
import pyodbc

connection = pyodbc.connect(r'Driver={SQL Server};Server=servername;Database=Remedy_VSC;Trusted_Connection=yes;')
cursor = connection.cursor()

corrente_start = "2023-07-01"
corrente_end = "2023-07-31"
corrente_next = "2023-08-01"

anterior_start = "2023-06-01"
anterior_end = "2023-06-30"
anterior_next = "2023-07-01"

q_mes_corrente = """
SELECT       
       
       DISTINCT
       
	   [APPROVED_TO_INVOICE] = CASE WHEN ASSET.AST_Status = 'In use' and (ASSET.AST_Owner <> 'Vendor' or asset.ast_owner is null or asset.ast_owner = '')  THEN 'YES' ELSE 'NO' END
      ,[IF_NO_REASON] = ''
      ,[RU] = 'ASSET'
      ,[RU_2ND_LEVEL_DETAIL] = 'ASSET'
      ,[_3RD_LEVEL_DETAIL] = ASSET.AST_ReconciliationIdentity
      ,[_4TH_LEVEL_DETAIL] = ASSET.AST_Product_Name
      ,[FILE_CODE] = ''
      ,[START_DATE_OF_PERIOD] = '""" + corrente_start + """'
      ,[END_DATE_OF_PERIOD] = '""" + corrente_end +"""'
      ,[ASSET_ID_TAG] = ASSET.AST_Tag_Number
      ,[TYPE] =  case when ASSET.AST_Type IN ('Laptop','Notebook') then 'Laptop'
                                                when ASSET.AST_Type IN ('Desktop','Thin Client')  then 'Desktop' 
                                                ELSE 'N/I' end
      ,[MANUFACTURER] = ASSET.AST_Manufacturer
      ,[MODEL] = ASSET.AST_Product_Name
      ,[SERIAL_NUMBER] = ASSET.AST_Serial_Number
      ,[CLOCK_SPEED] = ''
      ,[MEMORY] = ''
      ,[HARD_DRIVE] = ''
      ,[ASSET_OWNER] = ASSET.AST_Owner
      ,[LEGAL_TAG] = ASSET.AST_Client_Asset_Tag
      ,[LAST_USER_LOGON] = ''
      ,[COMPANY] = ASSET.OWN_Company_Code
      ,[USER_NETWORK_ID] = ASSET.OWN_IAM_Login_ID
      ,[USER_FULL_NAME] = ASSET.OWN_Full_Name
      ,[STRUCTURE_CODE] = ''
      
      
      ,[EXECUTIVE_DIRECTOR_DEPT_L6] = ASSET.OWN_Level_02
      ,[GLOBAL_DIRECTOR_DEPT_L5] = ASSET.OWN_Level_03
      ,[DEPARTMENT_MANAGER_L4] = ASSET.OWN_Level_04
      ,[MANAGER_DEPT_L3] = ASSET.OWN_Level_05
      ,[AREA_MANAGER_DEPT_L2] = ASSET.OWN_Level_06
      
      
      ,[COST_CENTER] = ASSET.AST_Cost_Center
      ,[COUNTRY] = ASSET.AST_Country
      ,[STATE_PROVINCE]= ASSET.AST_State
      
      ,[SITE_TYPE]= ''
      ,[LOCATION] = UPPER(ASSET.AST_Site_Group)
      ,[CRITICITY_TYPE] = ''
      ,[NETWORK_SCANNING_ENABLED] = ''
      ,[TICKET] = ASSET.AST_CI_Description
      ,[INSTALL_DATE] = ASSET.AST_Installation_Date
      ,[ENTITY] = ASSET.OWN_Entity
      ,[DATA_IMPORTACAO] = GETDATE()
      ,[DATA_INDICADOR] = '""" + corrente_start + """'
      ,[PROCESSOR] = ''
      ,[OPERATIONAL_SYSTEM] = ''
      ,[IP_ADDRESS] = ''
      ,[HOSTNAME] = ASSET.AST_CI_Name
      ,[ELO_CONTRACT] = ASSET.USB_Contractor_Contract_Number
      ,[NON_ELO_CONTRACT] = ASSET.USB_Contractor_Contract_Number
      ,[CONTRACT_COMPANY_NAME] = ASSET.USB_Company_Name
      ,[BUILDING] = ASSET.AST_Site
      ,[ROOM] = ASSET.AST_Room
      ,[FLOOR] = ASSET.AST_Floor
      ,[KTR_ORIGEM] = 'ASSET'
      ,[COMPETENCE_DATE] = '""" + corrente_start + """'
      ,[SOURCE] = 'ASSET'
      ,[DATA_TYPE] = 'APROVADO'
      ,[ORIGINAL_LEASE_END_DATE] = ASSET.AST_End_Date
      ,[LAST_SCAN_DATE] = ASSET.AST_Last_Scan_Date
      ,[HP_INVENTORY_CONTROL_NUMBER] = ''
      ,[HP_INVOICE_KEY] = ''
      ,[STATUS] = AST_Status 
      ,ACQ_METHOD = ASSET.AST_Ownership_Type
      ,PURCHASE_DATE  = ASSET.AST_Purchase_Date
      ,EXTENDED_LEASE_END_DATE = ASSET.AST_End_Date
      ,[UsedByID] = ASSET.USB_IAM_Login_ID
      ,[UsedByFullName] = ASSET.USB_Full_Name
      ,[STATUS_MDM] = ASSET.OWN_Status_MDM
      ,[UsedById_Contract_Number] = ASSET.USB_Contractor_Contract_Number
      ,[UsedById_Company_Name] = ASSET.USB_Company_Name
      ,[UsedById_Contract_Manager]     = ASSET.USB_Contractor_Contract_Manager  
      ,Site_ID = ASSET.AST_Site_Id
      ,Vale_AST_Site_Id = ASSET.AST_Site_Id

      ,[PBI_Cost_Center] = ASSET.AST_Cost_Center
      ,[PBI_Site_Id] = ASSET.AST_Site_Id
      ,[PBI_Level_02] = ASSET.OWN_Level_02
      ,[PBI_Level_03] = ASSET.OWN_Level_03
      ,[PBI_Level_04] = ASSET.OWN_Level_04
      ,[PBI_Level_05] = ASSET.OWN_Level_05
      ,[PBI_Level_06] = ASSET.OWN_Level_06
      ,[PBI_Company_Code] = ASSET.OWN_Company_Code
      ,[PBI_Company_Name] = ASSET.OWN_Company_Name
      ,[PBI_Entity]      =  ASSET.OWN_Entity
	  ,PBI_SN_SITE_ID = ASSET.AST_Site_Id
     
  FROM ServiceNow_VSC.dbo.alm_asset_vale_lookup_historic AS ASSET
  
  WHERE
    [AST_Type] is not NULL    
 AND ASSET.AST_Status in ('In use') 
 AND CONVERT(DATE, ASSET.competence_date) = '""" + corrente_next + """'
 AND (ASSET.AST_Country NOT IN ('Indonesia','Korea, Republic of','New Caledonia','Taiwan, Republic of China','Taiwan','Korea') OR ASSET.AST_Country IS NULL)
 AND ASSET.AST_Type IN ('Laptop','Desktop','Notebook','Thin Client')
 AND ASSET.ast_category = 'EUC'
"""

q_mes_ant = """
SELECT       
       
       DISTINCT
       
	   [APPROVED_TO_INVOICE] = CASE WHEN ASSET.AST_Status = 'In use' and (ASSET.AST_Owner <> 'Vendor' or asset.ast_owner is null or asset.ast_owner = '')  THEN 'YES' ELSE 'NO' END
      ,[IF_NO_REASON] = ''
      ,[RU] = 'ASSET'
      ,[RU_2ND_LEVEL_DETAIL] = 'ASSET'
      ,[_3RD_LEVEL_DETAIL] = ASSET.AST_ReconciliationIdentity
      ,[_4TH_LEVEL_DETAIL] = ASSET.AST_Product_Name
      ,[FILE_CODE] = ''
      ,[START_DATE_OF_PERIOD] = '""" + anterior_start + """'
      ,[END_DATE_OF_PERIOD] = '""" + anterior_end + """'
      ,[ASSET_ID_TAG] = ASSET.AST_Tag_Number
      ,[TYPE] =  case when ASSET.AST_Type IN ('Laptop','Notebook') then 'Laptop'
                                                when ASSET.AST_Type IN ('Desktop','Thin Client')  then 'Desktop' 
                                                ELSE 'N/I' end
      ,[MANUFACTURER] = ASSET.AST_Manufacturer
      ,[MODEL] = ASSET.AST_Product_Name
      ,[SERIAL_NUMBER] = ASSET.AST_Serial_Number
      ,[CLOCK_SPEED] = ''
      ,[MEMORY] = ''
      ,[HARD_DRIVE] = ''
      ,[ASSET_OWNER] = ASSET.AST_Owner
      ,[LEGAL_TAG] = ASSET.AST_Client_Asset_Tag
      ,[LAST_USER_LOGON] = ''
      ,[COMPANY] = ASSET.OWN_Company_Code
      ,[USER_NETWORK_ID] = ASSET.OWN_IAM_Login_ID
      ,[USER_FULL_NAME] = ASSET.OWN_Full_Name
      ,[STRUCTURE_CODE] = ''
      
      
      ,[EXECUTIVE_DIRECTOR_DEPT_L6] = ASSET.OWN_Level_02
      ,[GLOBAL_DIRECTOR_DEPT_L5] = ASSET.OWN_Level_03
      ,[DEPARTMENT_MANAGER_L4] = ASSET.OWN_Level_04
      ,[MANAGER_DEPT_L3] = ASSET.OWN_Level_05
      ,[AREA_MANAGER_DEPT_L2] = ASSET.OWN_Level_06
      
      
      ,[COST_CENTER] = ASSET.AST_Cost_Center
      ,[COUNTRY] = ASSET.AST_Country
      ,[STATE_PROVINCE]= ASSET.AST_State
      
      ,[SITE_TYPE]= ''
      ,[LOCATION] = UPPER(ASSET.AST_Site_Group)
      ,[CRITICITY_TYPE] = ''
      ,[NETWORK_SCANNING_ENABLED] = ''
      ,[TICKET] = ASSET.AST_CI_Description
      ,[INSTALL_DATE] = ASSET.AST_Installation_Date
      ,[ENTITY] = ASSET.OWN_Entity
      ,[DATA_IMPORTACAO] = GETDATE()
      ,[DATA_INDICADOR] = '""" + anterior_start + """'
      ,[PROCESSOR] = ''
      ,[OPERATIONAL_SYSTEM] = ''
      ,[IP_ADDRESS] = ''
      ,[HOSTNAME] = ASSET.AST_CI_Name
      ,[ELO_CONTRACT] = ASSET.USB_Contractor_Contract_Number
      ,[NON_ELO_CONTRACT] = ASSET.USB_Contractor_Contract_Number
      ,[CONTRACT_COMPANY_NAME] = ASSET.USB_Company_Name
      ,[BUILDING] = ASSET.AST_Site
      ,[ROOM] = ASSET.AST_Room
      ,[FLOOR] = ASSET.AST_Floor
      ,[KTR_ORIGEM] = 'ASSET'
      ,[COMPETENCE_DATE] = '""" + anterior_start + """'
      ,[SOURCE] = 'ASSET'
      ,[DATA_TYPE] = 'APROVADO'
      ,[ORIGINAL_LEASE_END_DATE] = ASSET.AST_End_Date
      ,[LAST_SCAN_DATE] = ASSET.AST_Last_Scan_Date
      ,[HP_INVENTORY_CONTROL_NUMBER] = ''
      ,[HP_INVOICE_KEY] = ''
      ,[STATUS] = AST_Status 
      ,ACQ_METHOD = ASSET.AST_Ownership_Type
      ,PURCHASE_DATE  = ASSET.AST_Purchase_Date
      ,EXTENDED_LEASE_END_DATE = ASSET.AST_End_Date
      ,[UsedByID] = ASSET.USB_IAM_Login_ID
      ,[UsedByFullName] = ASSET.USB_Full_Name
      ,[STATUS_MDM] = ASSET.OWN_Status_MDM
      ,[UsedById_Contract_Number] = ASSET.USB_Contractor_Contract_Number
      ,[UsedById_Company_Name] = ASSET.USB_Company_Name
      ,[UsedById_Contract_Manager]     = ASSET.USB_Contractor_Contract_Manager  
      ,Site_ID = ASSET.AST_Site_Id
      ,Vale_AST_Site_Id = ASSET.AST_Site_Id

      ,[PBI_Cost_Center] = ASSET.AST_Cost_Center
      ,[PBI_Site_Id] = ASSET.AST_Site_Id
      ,[PBI_Level_02] = ASSET.OWN_Level_02
      ,[PBI_Level_03] = ASSET.OWN_Level_03
      ,[PBI_Level_04] = ASSET.OWN_Level_04
      ,[PBI_Level_05] = ASSET.OWN_Level_05
      ,[PBI_Level_06] = ASSET.OWN_Level_06
      ,[PBI_Company_Code] = ASSET.OWN_Company_Code
      ,[PBI_Company_Name] = ASSET.OWN_Company_Name
      ,[PBI_Entity]      =  ASSET.OWN_Entity
	  ,PBI_SN_SITE_ID = ASSET.AST_Site_Id
     
  FROM ServiceNow_VSC.dbo.alm_asset_vale_lookup_historic AS ASSET
  
  WHERE
    [AST_Type] is not NULL    
 AND ASSET.AST_Status in ('In use') 
 AND CONVERT(DATE, ASSET.competence_date) = '""" + anterior_next + """'
 AND (ASSET.AST_Country NOT IN ('Indonesia','Korea, Republic of','New Caledonia','Taiwan, Republic of China','Taiwan','Korea') OR ASSET.AST_Country IS NULL)
 AND ASSET.AST_Type IN ('Laptop','Desktop','Notebook','Thin Client')
 AND ASSET.ast_category = 'EUC'
"""

#cursor.execute(q_mes_corrente)
dfcorr = pd.read_sql(q_mes_corrente,connection)
dfant = pd.read_sql(q_mes_ant,connection)

dfcorr = dfcorr[(dfcorr['APPROVED_TO_INVOICE'] == 'YES')]
dfant = dfant[(dfant['APPROVED_TO_INVOICE'] == 'YES')]

# remover CR e LF
dfant = dfant.replace(r'\n',' ', regex=True) 
dfant = dfant.replace(r'\r',' ', regex=True) 

dfcorr = dfcorr.replace(r'\n',' ', regex=True) 
dfcorr = dfcorr.replace(r'\r',' ', regex=True) 

# filtra o que tinha no mes anterior e não tem mais no mes atual (saiu)
# Alterado em 4/05/23 para incluir Asset Tag Id no parametro ON
dfout = pd.merge(dfant, dfcorr, on=['SERIAL_NUMBER','COUNTRY', 'ASSET_ID_TAG'], how="outer", indicator=True
              ).query('_merge=="left_only"')
# filtra o que nao tinha no mes anterior e tem no mes atual (entrou)
dfin = pd.merge(dfcorr, dfant, on=['SERIAL_NUMBER','COUNTRY', 'ASSET_ID_TAG'], how="outer", indicator=True
              ).query('_merge=="left_only"')

print('Anterior\n',dfant.groupby(by=['TYPE','COUNTRY']).size())
print('Corrente\n',dfcorr.groupby(by=['TYPE','COUNTRY']).size())
print('In\n',dfin.groupby(by=['TYPE_x','COUNTRY']).size())
print('Out\n',dfout.groupby(by=['TYPE_x','COUNTRY']).size())

#dftemp = dfin.drop_duplicates('HOSTNAME_x')

dfin.to_csv('onepage-in.csv', sep=';', encoding = 'utf-16', index = False)
dfout.to_csv('onepage-out.csv', sep=';', encoding = 'utf-16', index = False)
dfant.to_csv('onepage-ant.csv', sep=';', encoding = 'utf-16', index = False)
dfcorr.to_csv('onepage-corr.csv', sep=';', encoding = 'utf-16', index = False)

dfin2 = dfin.drop_duplicates(subset = 'SERIAL_NUMBER', keep = 'first')
