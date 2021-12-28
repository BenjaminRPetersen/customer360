# -*- coding: utf-8 -*-
"""
Title: BI-Schema Scheduler

Created on Tue Oct 19 13:36:00 2021

@author: benjaminrpetersen
"""

"Import Base Packages"
import os, csv
from datetime import datetime # Used for logging
from os import listdir
from os.path import isfile, join

"Import BI-Schema Files"
os.chdir(os.path.dirname(os.path.realpath(__file__)))
parent_path = os.path.dirname(os.getcwd())
mypath = parent_path+r'\bi-data-warehouse\\'
integration_files = [f for f in listdir(mypath) if isfile(join(mypath, f))]

"Create Variables"
now = datetime.now()
file_date = now.strftime("%Y-%m-%d H%HM%MS%S")


"Logging Setup"
log_header = ['BI Procedure','Start Date Time', 'Status', 'Description', 'End Date Time', 'Duration in Seconds', 'Rows Count'] # File Fields
filename = ('bi-data-warehouse-'+file_date+'.csv')
log_rows = []

"""
######################################################################################################
# RUN SCHEMA BUILDER
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
        rows = 0
    log = [integration_files[file_index], start, stat, fail, end, difference, rows]
    log_rows.append(log)
    file_index = file_index+1

"Log Results of Run"
os.chdir('C:/customer360/logging/')
with open(filename, 'w') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(log_header)
    csvwriter.writerows(log_rows)

