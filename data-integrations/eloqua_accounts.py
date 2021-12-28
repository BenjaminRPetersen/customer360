# -*- coding: utf-8 -*-
"""
Title: Import Eloqua Accounts

Created on Wed Sep  13 11:02:33 2021

@author: benjaminrpetersen
"""

"""
Import Packages, Tools and Authorizations
"""
import pandas as pd, os, datetime, time # Standard Packages 
from requests.adapters import HTTPAdapter # TO RESET CONNECTION
os.chdir(os.path.dirname(os.path.realpath(__file__)))
parent_path = os.path.dirname(os.getcwd())
os.chdir(parent_path+'/tools/') # Hashing Algorithm
from gsa_hashing import gsa_hash
os.chdir(parent_path+'/authorizations/') # System authorizations
from eloqua import s, retries, client_name, username, password
from redshift import conn
s.mount('http://', HTTPAdapter(max_retries=retries)) # Set Max Retries for API Call
today = datetime.date.today() # Today's Date
yesterday = today - datetime.timedelta(days=7) # Yesterday's Date
yesterday = str(yesterday) # String Transformation of Yesterday's Date
start = datetime.datetime.now()

"""
Get all contact fields for API Request
"""
fields = s.get('https://secure.p03.eloqua.com/api/bulk/2.0/accounts/fields',
		auth=(client_name+'\\'+username, password),
		headers = {'Content-Type':'application/json'}) # Get Account fields from Eloqua's API
fields = pd.json_normalize(fields.json()['items']) # Parse JSON into Dataframe

"""
Create export definition
"""
uri = pd.DataFrame(columns=['uri']) # Set Empty DataFrame
base_s=0 # Set Index
f='"'+fields.name[base_s].replace(' ', '_').replace('(', '').replace(')','')+'":"'+fields.statement[base_s]+'"\n' # Start Statement
base_s=base_s+1 # Prepare to Build the Rest of the Statement
while base_s < len(fields):
    temp_s='"'+fields.name[base_s].replace(' ', '_').replace('(', '').replace(')','')+'":"'+fields.statement[base_s]+'"\n'
    f = f+temp_s
    base_s=base_s+1 # Build Field Statement
                          
payload = '''{
              "name":"BI Fusion Accounts Export",
              "fields":{
                    '''+f+'''
                    }
              "filter":"'{{Account.Field(M_DateCreated)}}' >= \''''+yesterday+''' 00:00:00.000' OR '{{Account.Field(M_DateModified)}}' >= \''''+yesterday+''' 00:00:00.000'"
               }''' # Build Data Call to API
accounts = s.post('https://secure.p03.eloqua.com/api/bulk/2.0/accounts/exports',
                    auth=(client_name+'\\'+username, password),
                    data=payload,
                    headers={'Content-Type':'application/json'}) # Send Export Definition to Eloqua
uri = uri.append({'uri':accounts.json()['uri']}, ignore_index=True) # Store URI to Export Definition

"""
Export any data that has been changed or modified in the last day
"""
i=0 # Set Index for URI
uri = uri.uri[i] # Grab URI for Accounts
payload = '''{
   "syncedInstanceUri" : "'''+uri+'''"
}''' # Prepare request for Account Data

sync = s.post('https://secure.p03.eloqua.com/api/bulk/2.0/syncs',
          auth=(client_name+'\\'+username, password),
          data=payload,
          headers = {'Content-Type':'application/json'}) # Sync Account Data in Eloqua
status = sync.json()['status'] # Get status of Request

while status != 'success':
    time.sleep(1)
    sync = s.get(   ('https://secure.p03.eloqua.com/api/bulk/2.0'+sync.json()['uri']),
                    auth=(client_name+'\\'+username, password),
                    headers = {'Content-Type':'application/json'})
    status = sync.json()['status'] # Loop over request status until it's successful

if status == 'success':
    offset=0
    last_record=0
    response = s.get('https://secure.p03.eloqua.com/api/bulk/2.0'+uri+'/data',
              auth=(client_name+'\\'+username, password),
              data='''{
                        "limit":50000
                        "offset":'''+str(offset)+'''
                        }''',
              headers = {'Content-Type':'application/json'})
    accounts = pd.json_normalize(response.json()['items'])
    offset = offset+50000 # Grab the first 50,000 records
    if response.json()['hasMore'] == False:
            last_record=1
    else:
        pass
    while offset < 5000000 and last_record==0:
        response = s.get('https://secure.p03.eloqua.com/api/bulk/2.0'+uri+'/data',
              auth=(client_name+'\\'+username, password),
              data='''{
                        "limit":50000
                        "offset":'''+str(offset)+'''
                        }''',
              headers = {'Content-Type':'application/json'})
        if response.json()['hasMore'] == False:
            last_record=1
            pull = pd.json_normalize(response.json()['items'])
            accounts = pd.concat([accounts, pull])
        else:
            pull = pd.json_normalize(response.json()['items'])
            accounts = pd.concat([accounts, pull])
        offset = offset+50000 # Grab the rest of the records by looping

"""
Update data in Redshift
"""
conn.execute("CREATE TABLE customer360.t_eloqua_accounts AS (SELECT * FROM customer360.s_eloqua_accounts WHERE 1=2);")
accounts.to_sql('t_eloqua_accounts', conn, schema='customer360', index=False, if_exists='append',chunksize=10000, method='multi')
col = accounts.columns.str.lower()
update = '' # Update Statement Placeholder
i = 0 # Set Index
while i < len(col):
    if i == len(col)-1:
        update = update+(col[i]+' = t.'+col[i]+'')
    else:
        update = update+(col[i]+' = t.'+col[i]+', ')
    i=i+1 # Create update statement where fields equal temporary table
created = conn.execute("select count(*) ct from customer360.t_eloqua_accounts where eloqua_company_id not in (select eloqua_company_id from customer360.s_eloqua_accounts)").fetchall()[0][0]
updated = conn.execute("select count(*) ct from customer360.t_eloqua_accounts where eloqua_company_id in (select eloqua_company_id from customer360.s_eloqua_accounts)").fetchall()[0][0]
conn.execute("   update customer360.s_eloqua_accounts s\
                 set    "+update+" \
                from    customer360.t_eloqua_accounts t\
                where   s.eloqua_company_id = t.eloqua_company_id; commit;")
conn.execute("insert into customer360.s_eloqua_accounts select * from customer360.t_eloqua_accounts where eloqua_company_id not in (select eloqua_company_id from customer360.s_eloqua_accounts);  commit;") # Insert New Records
conn.execute("drop table customer360.t_eloqua_accounts;  commit;")
# conn.execute("GRANT SELECT ON ALL TABLES IN SCHEMA customer360 TO gabegoralski, rosemarythan, kiyounglee, bryanthales, ernestdakin, shilpakhanna, davidtreichler, raquelsumaray")
end = datetime.datetime.now()
difference = (end-start)
difference = difference.total_seconds()

