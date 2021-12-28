# -*- coding: utf-8 -*-
"""
Title: Eloqua Email Clickthrough Import
Created on Mon Sep 13 12:46:59 2021

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
os.chdir(parent_path+'/authorizations/') # System Authorizations
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
fields = s.get('https://secure.p03.eloqua.com/api/bulk/2.0/activities/fields',
		auth=(client_name+'\\'+username, password),
		headers = {'Content-Type':'application/json'})
fields = pd.json_normalize(fields.json()['items'])
fields = fields[fields.activityTypes.apply(lambda x: 'EmailClickthrough' in x)].reset_index()

"""
Create export definition
"""
uri = pd.DataFrame(columns=['uri']) # Set Empty DataFrame
base_s=0 # Set Index
f='"'+fields.internalName[base_s].replace(' ', '_').replace('(', '').replace(')','')+'":"'+fields.statement[base_s]+'"\n' # Start Statement
base_s=base_s+1 # Prepare to Build the Rest of the Statement
while base_s < len(fields):
    temp_s='"'+fields.internalName[base_s].replace(' ', '_').replace('(', '').replace(')','')+'":"'+fields.statement[base_s]+'"\n'
    f = f+temp_s
    base_s=base_s+1 # Build Field Statement
                          
payload = '''{
              "name":"BI Fusion EmailClickthrough Export",
              "fields":{
                    '''+f+'''
                    }
              "filter":"'{{Activity.Type}}' = 'EmailClickthrough' AND '{{Activity.CreatedAt}}' >= \''''+yesterday+''' 00:00:00.000'"
               }''' # Build Data Call to API
activities = s.post('https://secure.p03.eloqua.com/api/bulk/2.0/activities/exports',
                    auth=(client_name+'\\'+username, password),
                    data=payload,
                    headers={'Content-Type':'application/json'}) # Send Export Definition to Eloqua
uri = uri.append({'uri':activities.json()['uri']}, ignore_index=True) # Store URI to Export Definition

"""
Export any data that has been changed or modified in the last day
"""
i=0 # Set Index for URI
uri = uri.uri[i] # Grab URI for Acitivites
payload = '''{
   "syncedInstanceUri" : "'''+uri+'''"
}''' # Prepare request for Acitivites Data

sync = s.post('https://secure.p03.eloqua.com/api/bulk/2.0/syncs',
          auth=(client_name+'\\'+username, password),
          data=payload,
          headers = {'Content-Type':'application/json'}) # Sync Acitivites Data in Eloqua
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
    activities = pd.json_normalize(response.json()['items'])
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
            activities = pd.concat([activities, pull])
        else:
            pull = pd.json_normalize(response.json()['items'])
            activities = pd.concat([activities, pull])
        offset = offset+50000 # Grab the rest of the records by looping
if 'IpAddress' in activities.columns:
    activities['IpAddress'] = activities.IpAddress.apply(lambda x:gsa_hash(x))
if 'EmailAddress' in activities.columns:
    activities['EmailAddress'] = activities.EmailAddress.apply(lambda x:gsa_hash(x))
    
"""
Update data in Redshift
"""
conn.execute("CREATE TABLE customer360.t_eloqua_emailclickthrough AS (SELECT * FROM customer360.s_eloqua_emailclickthrough WHERE 1=2);")
activities.to_sql('t_eloqua_emailclickthrough', conn, schema='customer360', index=False, if_exists='append',chunksize=10000, method='multi')
col = activities.columns.str.lower()
update = '' # Update Statement Placeholder
i = 0 # Set Index
while i < len(col):
    if i == len(col)-1:
        update = update+(col[i]+' = t.'+col[i]+'')
    else:
        update = update+(col[i]+' = t.'+col[i]+', ')
    i=i+1 # Create update statement where fields equal temporary table
created = conn.execute("select count(*) ct from customer360.t_eloqua_emailclickthrough where id not in (select id from customer360.s_eloqua_emailclickthrough)").fetchall()[0][0]
updated = conn.execute("select count(*) ct from customer360.t_eloqua_emailclickthrough where id in (select id from customer360.s_eloqua_emailclickthrough)").fetchall()[0][0]
conn.execute("   update customer360.s_eloqua_emailclickthrough s\
                 set    "+update+" \
                from    customer360.t_eloqua_emailclickthrough t\
                where   s.id = t.id;  commit;")
conn.execute("insert into customer360.s_eloqua_emailclickthrough select * from customer360.t_eloqua_emailclickthrough where id not in (select id from customer360.s_eloqua_emailclickthrough); commit;") # Insert New Records
conn.execute("drop table customer360.t_eloqua_emailclickthrough;  commit;")
# conn.execute("GRANT SELECT ON ALL TABLES IN SCHEMA customer360 TO gabegoralski, rosemarythan, kiyounglee, bryanthales, ernestdakin, shilpakhanna, davidtreichler, raquelsumaray")
end = datetime.datetime.now()
difference = (end-start)
difference = difference.total_seconds()
