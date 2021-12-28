# -*- coding: utf-8 -*-
"""
Title: Salesforce RecordType Integration

Created on Thu Sep 16 11:17:19 2021

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
date_filter = today - datetime.timedelta(days=728) # Yesterday's Date
date_filter = str(date_filter) # String Transformation of Yesterday's Date
start = datetime.datetime.now()

" QUERY ACCOUNT SALESFORCE OBJECT "
df = pd.json_normalize(client.sobjects.query("   SELECT	Id\
                                                                , Name\
                                                                , Description\
                                                                , IsActive\
                                                                , CreatedDate\
                                                                , LastModifiedById\
                                                        FROM	RecordType"))
                                                    

"""
Update data in Redshift
"""
conn.execute("CREATE TABLE customer360.t_salesforce_recordtype AS (SELECT * FROM customer360.s_salesforce_recordtype WHERE 1=2);")
df.to_sql('t_salesforce_recordtype', conn, schema='customer360', index=False, if_exists='append',chunksize=10000, method='multi')
col = df.columns.str.lower()
update = '' # Update Statement Placeholder
i = 0 # Set Index
while i < len(col):
    if i == len(col)-1:
        update = update+('"'+col[i]+'" = t."'+col[i]+'"')
    else:
        update = update+('"'+col[i]+'" = t."'+col[i]+'", ')
    i=i+1 # Create update statement where fields equal temporary table
updated = conn.execute("select count(*) ct from customer360.t_salesforce_recordtype where id in (select id from customer360.s_salesforce_recordtype)").fetchall()[0][0]
created = conn.execute("select count(*) ct from customer360.t_salesforce_recordtype where id not in (select id from customer360.s_salesforce_recordtype)").fetchall()[0][0]
conn.execute("   update customer360.s_salesforce_recordtype s\
                 set    "+update+" \
                from    customer360.t_salesforce_recordtype t\
                where   s.id = t.id; commit;")
conn.execute("insert into customer360.s_salesforce_recordtype select * from customer360.t_salesforce_recordtype where id not in (select id from customer360.s_salesforce_recordtype); commit;") # Insert New Records
conn.execute("drop table customer360.t_salesforce_recordtype; commit;")
# conn.execute("GRANT SELECT ON ALL TABLES IN SCHEMA customer360 TO gabegoralski, rosemarythan, kiyounglee, bryanthales, ernestdakin, shilpakhanna, davidtreichler, raquelsumaray")
end = datetime.datetime.now()
difference = (end-start)
difference = difference.total_seconds()

