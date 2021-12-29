# -*- coding: utf-8 -*-
"""
Title: Data Integration Scheduler

Created on Fri Sep 10 11:07:07 2021

@author: benjaminrpetersen
"""

"Import Base Packages"
import os, csv, requests, pandas as pd
from datetime import datetime, date # Used for logging
from os import listdir
from os.path import isfile, join

"Import Data Integration Files"
os.chdir(os.path.dirname(os.path.realpath(__file__)))
parent_path = os.path.dirname(os.getcwd())
mypath = parent_path+'\data-integrations\\'
connpath = parent_path+'\\authorizations\\'
integration_files = [f for f in listdir(mypath) if isfile(join(mypath, f))]

"Create Variables"
now = datetime.now()
file_date = now.strftime("%Y-%m-%d H%HM%MS%S")


"Logging Setup"
log_header = ['Integration File','Start Date Time', 'Status', 'Description', 'End Date Time', 'Duration in Seconds', 'Rows Updated', 'Rows Created'] # File Fields
filename = ('data-integration-'+file_date+'.csv')
log_rows = []
"""
######################################################################################################
# CHECK DATA CONNECTION
######################################################################################################
"""
os.chdir(os.path.dirname(connpath))
exec(open(join(connpath,'connection_checker.py')).read())
os.chdir(os.path.dirname(os.path.realpath(__file__)))
"""
######################################################################################################
# RUN DATA INTEGRATION
######################################################################################################
"""
file_index=0
while file_index < len(integration_files):
    os.chdir(mypath)
    fail = ''
    try:
        exec(open(integration_files[file_index]).read())
        stat = 'Pass'
        fail = 'Successful Run'
    except Exception as err:
        start = now.strftime("%Y-%m-%d %H:%M:%S")
        fail = err
        stat = 'Fail'
        end = now.strftime("%Y-%m-%d %H:%M:%S")
        difference = 0
        updated = 0
        created = 0
    log = [integration_files[file_index], start, stat, fail, end, difference, updated, created]
    log_rows.append(log)
    file_index = file_index+1

loger = pd.DataFrame(log_rows)
loger.columns = log_header

webhook_url = 'https://chat.googleapis.com/v1/spaces/AAAAwmb61yg/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=ekf8x0DkOvVy9VSfXmzZ3zSrDDcyyE6ux358wh7gCC8%3D'
webhook_json = '''{
                              "cards": [
                                {
                                  "sections": [
                                    {
                                      "widgets": [
                                        {
                                          "textParagraph": {
                                            "text": "Date: '''+str(date.today())+'''<br><b>Salesforce Integration</b> <br><i>Success</i>: '''+str(len(loger[(loger.Status == 'Pass') & (loger['Integration File'].str.contains('salesforce'))]))+''' <br><i>Fail</i>: '''+str(len(loger[(loger.Status == 'Fail') & (loger['Integration File'].str.contains('salesforce'))]))+''' <br><b>Eloqua Integration</b> <br><i>Success</i>: '''+str(len(loger[(loger.Status == 'Pass') & (loger['Integration File'].str.contains('eloqua'))]))+''' <br><i>Fail</i>: '''+str(len(loger[(loger.Status == 'Fail') & (loger['Integration File'].str.contains('eloqua'))]))+'''"
                                          }
                                        }
                                      ]
                                    }
                                  ]
                                }
                              ]
                            }'''
requests.post(webhook_url, webhook_json)

"Log Results of Run"
os.chdir('C:/customer360/logging/')
with open(filename, 'w') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(log_header)
    csvwriter.writerows(log_rows)

