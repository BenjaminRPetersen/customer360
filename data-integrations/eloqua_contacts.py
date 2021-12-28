# -*- coding: utf-8 -*-
"""
Title: Import Eloqua Contacts

Created on Wed Sep  8 11:02:33 2021

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
fields = s.get('https://secure.p03.eloqua.com/api/bulk/2.0/contacts/fields',
		auth=(client_name+'\\'+username, password),
		headers = {'Content-Type':'application/json'}) # Get Contact fields from Eloqua's API
fields = pd.json_normalize(fields.json()['items']) # Parse JSON into Dataframe

"""
Create export definition
"""
uri = pd.DataFrame(columns=['uri']) # Set Empty DataFrame
base_s=0 # Set Index
f='"'+fields.internalName[base_s]+'":"'+fields.statement[base_s]+'"\n' # Start Statement
base_s=base_s+1 # Prepare to Build the Rest of the Statement
while base_s < len(fields):
    temp_s='"'+fields.internalName[base_s]+'":"'+fields.statement[base_s]+'"\n'
    f = f+temp_s
    base_s=base_s+1 # Build Field Statement
               
payload = '''{
              "name":"BI Fusion Contacts Export",
              "fields":{
                    '''+f+'''
                    }
              "filter":"'{{Contact.Field(C_DateCreated)}}' >= \''''+yesterday+''' 00:00:00.000' OR '{{Contact.Field(C_DateModified)}}' >= \''''+yesterday+''' 00:00:00.000'"
              }''' # Build Data Call to API
contacts = s.post('https://secure.p03.eloqua.com/api/bulk/2.0/contacts/exports',
                    auth=(client_name+'\\'+username, password),
                    data=payload,
                    headers={'Content-Type':'application/json'}) # Send Export Definition to Eloqua
uri = uri.append({'uri':contacts.json()['uri']}, ignore_index=True) # Store URI to Export Definition

"""
Export any data that has been changed or modified in the last day
"""
i=0 # Set Index for URI
uri = uri.uri[i] # Grab URI for Contacts
payload = '''{
   "syncedInstanceUri" : "'''+uri+'''"
}''' # Prepare request for Contact Data

sync = s.post('https://secure.p03.eloqua.com/api/bulk/2.0/syncs',
          auth=(client_name+'\\'+username, password),
          data=payload,
          headers = {'Content-Type':'application/json'}) # Sync Contact Data in Eloqua
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
    contacts = pd.json_normalize(response.json()['items'])
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
            contacts = pd.concat([contacts, pull])
        else:
            pull = pd.json_normalize(response.json()['items'])
            contacts = pd.concat([contacts, pull])
        offset = offset+50000 # Grab the rest of the records by looping
    contacts['C_EmailAddress'] = contacts.C_EmailAddress.apply(lambda x:gsa_hash(x)) # Hash PII Field
    contacts['C_EmailDisplayName'] = contacts.C_EmailAddress.apply(lambda x:gsa_hash(x)) # Hash PII Field
    contacts['C_Address1'] = contacts.C_EmailAddress.apply(lambda x:gsa_hash(x)) # Hash PII Field
    contacts['C_Address2'] = contacts.C_EmailAddress.apply(lambda x:gsa_hash(x)) # Hash PII Field
    contacts['C_Address3'] = contacts.C_EmailAddress.apply(lambda x:gsa_hash(x)) # Hash PII Field
contacts = contacts[['C_EmailAddress', 'C_FirstName', 'C_LastName', 'C_Zip_Postal', 'C_EmailAddressDomain', 'C_DateCreated', 'C_DateModified', 'ContactIDExt']] # Limit Contact Fields


"""
Update data in Redshift
"""
conn.execute("CREATE TABLE customer360.t_eloqua_contacts AS (SELECT * FROM customer360.s_eloqua_contacts WHERE 1=2);")
contacts.to_sql('t_eloqua_contacts', conn, schema='customer360', index=False, if_exists='append',chunksize=10000, method='multi')
col = contacts.columns.str.lower()
update = '' # Update Statement Placeholder
i = 0 # Set Index
while i < len(col):
    if i == len(col)-1:
        update = update+(col[i]+' = t.'+col[i]+'')
    else:
        update = update+(col[i]+' = t.'+col[i]+', ')
    i=i+1 # Create update statement where fields equal temporary table
created = conn.execute("select count(*) ct from customer360.t_eloqua_contacts where contactidext not in (select contactidext from customer360.s_eloqua_contacts)").fetchall()[0][0]
updated = conn.execute("select count(*) ct from customer360.t_eloqua_contacts where contactidext in (select contactidext from customer360.s_eloqua_contacts)").fetchall()[0][0]
conn.execute("   update customer360.s_eloqua_contacts s\
                 set    "+update+" \
                from    customer360.t_eloqua_contacts t\
                where   s.contactidext = t.contactidext;  commit;")
conn.execute("insert into customer360.s_eloqua_contacts select * from customer360.t_eloqua_contacts where contactidext not in (select contactidext from customer360.s_eloqua_contacts);  commit;") # Insert New Records
conn.execute("drop table customer360.t_eloqua_contacts; commit;")
# conn.execute("GRANT SELECT ON ALL TABLES IN SCHEMA customer360 TO gabegoralski, rosemarythan, kiyounglee, bryanthales, ernestdakin, shilpakhanna, davidtreichler, raquelsumaray")
end = datetime.datetime.now()
difference = (end-start)
difference = difference.total_seconds()

