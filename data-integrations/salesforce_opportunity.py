# -*- coding: utf-8 -*-
"""
Title: Salesforce Opportunity Integration

Created on Wed Sep 15 16:14:22 2021

@author: benjaminrpetersen
"""

"""
Import Packages, Tools and Authorizations
"""
import pandas as pd, os, datetime, time # Standard Packages
os.chdir(os.path.dirname(os.path.realpath(__file__)))
parent_path = os.path.dirname(os.getcwd())
os.chdir(parent_path+'/tools/') # Hashing Algorithm
from gsa_hashing import gsa_hash
os.chdir(parent_path+'/authorizations/') # System Authorizations
from salesforce_client import client
from redshift import conn
today = datetime.date.today() # Today's Date
date_filter = today - datetime.timedelta(days=5) # Yesterday's Date
date_filter = str(date_filter) # String Transformation of Yesterday's Date
start = datetime.datetime.now()

" QUERY ACCOUNT SALESFORCE OBJECT "
df = pd.json_normalize(client.sobjects.query("   SELECT	Id\
                                                                , AccountId\
                                                                , RecordTypeId\
                                                                , Name\
                                                                , Description\
                                                                , StageName\
                                                                , Amount\
                                                                , CloseDate\
                                                                , IsClosed\
                                                                , IsWon\
                                                                , CampaignId\
                                                                , OwnerId\
                                                                , CreatedDate\
                                                                , CreatedById\
                                                                , LastModifiedDate\
                                                                , LastModifiedById\
                                                                , LastActivityDate\
                                                                , ContactId\
                                                                , LastViewedDate\
                                                                , Primary_Vendor__c\
                                                                , Contracting_Officer__c\
                                                                , StandardName__c\
                                                                , Activities_Count__c\
                                                                , Opportunity_Source__c\
                                                                , Reason_Closed__c\
                                                                , Selected_Business_Line__c\
                                                                , Category__c\
                                                                , Region_Opp__c\
                                                                , Lifetime_Amount_multi_year__c\
                                                                , Business_Line__c\
                                                                , Case__c\
                                                                , Complex__c\
                                                                , ContactRole_Count__c\
                                                                , COVID_19__c\
                                                                , Date_of_Last_Activity__c\
                                                                , Days_Since_Last_Activity__c\
                                                                , Opp_FiscalYear__c\
                                                                , GSA_Product_Svc__c\
                                                                , Product__c\
                                                                , x10x10__c\
                                                                , Market_Research_as_a_Service__c\
                                                                , Opportunity_Owner__c\
                                                                , Owners_ChampionId__c\
                                                                , Owners_Champion__c\
                                                                , Portfolio2__c\
                                                                , Rapid_Review__c\
                                                                , Reason_other__c\
                                                                , Self_Service_or_Assisted_Service__c\
                                                                , Stage_Duration__c\
                                                                , Status__c\
                                                                , SubReasonClosed__c\
                                                                , Subcategory__c\
                                                                , Unique_Identifier__c\
                                                    FROM	Opportunity\
                                                    WHERE   CreatedDate >= "+date_filter+"T00:00:00Z OR LastModifiedDate >= "+date_filter+"T00:00:00Z"))
                                                

"""
Update data in Redshift
"""
conn.execute("CREATE TABLE customer360.t_salesforce_opportunity AS (SELECT * FROM customer360.s_salesforce_opportunity WHERE 1=2);")
df.to_sql('t_salesforce_opportunity', conn, schema='customer360', index=False, if_exists='append',chunksize=10000, method='multi')
col = df.columns.str.lower()
update = '' # Update Statement Placeholder
i = 0 # Set Index
while i < len(col):
    if i == len(col)-1:
        update = update+('"'+col[i]+'" = t."'+col[i]+'"')
    else:
        update = update+('"'+col[i]+'" = t."'+col[i]+'", ')
    i=i+1 # Create update statement where fields equal temporary table
updated = conn.execute("select count(*) ct from customer360.t_salesforce_opportunity where id in (select id from customer360.s_salesforce_opportunity)").fetchall()[0][0]
created = conn.execute("select count(*) ct from customer360.t_salesforce_opportunity where id not in (select id from customer360.s_salesforce_opportunity)").fetchall()[0][0]
conn.execute("   update customer360.s_salesforce_opportunity s\
                 set    "+update+" \
                from    customer360.t_salesforce_opportunity t\
                where   s.id = t.id; commit;")
conn.execute("insert into customer360.s_salesforce_opportunity select * from customer360.t_salesforce_opportunity where id not in (select id from customer360.s_salesforce_opportunity); commit;") # Insert New Records
conn.execute("drop table customer360.t_salesforce_opportunity; commit;")
# conn.execute("GRANT SELECT ON ALL TABLES IN SCHEMA customer360 TO gabegoralski, rosemarythan, kiyounglee, bryanthales, ernestdakin, shilpakhanna, davidtreichler, raquelsumaray")
end = datetime.datetime.now()
difference = (end-start)
difference = difference.total_seconds()




