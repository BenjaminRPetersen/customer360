# -*- coding: utf-8 -*-
"""
Title: Python Connection Variables

Created on Wed Sep  8 11:02:33 2021

@author: benjaminrpetersen
"""
import requests,os, logging
from requests.packages.urllib3.util.retry import Retry # RETRY CONNECTIONS
logging.basicConfig(level=logging.DEBUG) # LOGGING SETTINGS
s = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 502, 503, 504 ])
client_name = os.environ.get('ELOQUA') # CHANGE FOR PRODUCTION
username= os.environ.get('ELOQUA_USERNAME') # CHANGE FOR PRODUCTION
password= os.environ.get('ELOQUA_PASSWORD') # CHANGE FOR PRODUCTION