# -*- coding: utf-8 -*-
"""
Title: Salesforce Integration

Created on Wed Sep 15 12:14:33 2021

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
account = pd.json_normalize(client.sobjects.query("  SELECT  Id\
                                                            , MasterRecordId\
                                                            , Name\
                                                            , Phone\
                                                            , Website\
                                                            , OwnerId\
                                                            , CreatedDate\
                                                            , CreatedById\
                                                            , LastModifiedDate\
                                                            , LastModifiedById\
                                                            , LastActivityDate\
                                                            , Agency__c\
                                                            , Region__c\
                                                            , Bureau__c\
                                                            , DUNSNumber__c\
                                                            , DoDAAC_AAC__c\
                                                            , BillingState\
                                                            , Account_Nickname__c\
                                                            , BillingPostalCode\
                                                            , RecordTypeId\
                                                            , Classification__c\
                                                            , Account_Level__c\
                                                            , Agency2__c\
                                                            , Bureau_Code__c\
                                                            , CGAC_Agency_Code__c\
                                                            , Level1DropDown__c\
                                                            , Level2DropDown__c\
                                                            , ShippingAddress\
                                                            , NaicsCode__c\
                                                            , ParentId\
                                                            , BillingAddress\
                                                            , Sub_Type__c\
                                                    FROM    Account\
                                                    WHERE   CreatedDate >= "+date_filter+"T00:00:00Z OR LastModifiedDate >= "+date_filter+"T00:00:00Z"))

account['ShippingAddress'] = account.ShippingAddress.apply(lambda x:gsa_hash(x))
account['BillingAddress'] = account.BillingAddress.apply(lambda x:gsa_hash(x))
                                                
"""
Update data in Redshift
"""
conn.execute("CREATE TABLE customer360.t_salesforce_account AS (SELECT * FROM customer360.s_salesforce_account WHERE 1=2);")
account.to_sql('t_salesforce_account', conn, schema='customer360', index=False, if_exists='append',chunksize=10000, method='multi')
col = account.columns.str.lower()
update = '' # Update Statement Placeholder
i = 0 # Set Index
while i < len(col):
    if i == len(col)-1:
        update = update+('"'+col[i]+'" = t."'+col[i]+'"')
    else:
        update = update+('"'+col[i]+'" = t."'+col[i]+'", ')
    i=i+1 # Create update statement where fields equal temporary table
updated = conn.execute("select count(*) ct from customer360.t_salesforce_account where id in (select id from customer360.s_salesforce_account)").fetchall()[0][0]
created = conn.execute("select count(*) ct from customer360.t_salesforce_account where id not in (select id from customer360.s_salesforce_account)").fetchall()[0][0]
conn.execute("   update customer360.s_salesforce_account s\
                 set    "+update+" \
                from    customer360.t_salesforce_account t\
                where   s.id = t.id; commit;")
conn.execute("insert into customer360.s_salesforce_account select * from customer360.t_salesforce_account where id not in (select id from customer360.s_salesforce_account); commit;") # Insert New Records
conn.execute("drop table customer360.t_salesforce_account; commit;")
# conn.execute("GRANT SELECT ON ALL TABLES IN SCHEMA customer360 TO gabegoralski, rosemarythan, kiyounglee, bryanthales, ernestdakin, shilpakhanna, davidtreichler, raquelsumaray")
end = datetime.datetime.now()
difference = (end-start)
difference = difference.total_seconds()



