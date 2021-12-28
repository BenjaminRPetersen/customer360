# -*- coding: utf-8 -*-
"""
Title: Salesforce Authorization

Created on Wed Sep 15 12:19:52 2021

@author: benjaminrpetersen
"""

" Import Base Package for Connection"
from salesforce_api import Salesforce
import os

client = Salesforce(username=os.getenv('SALESFORCE_USERNAME'),
                    password=os.getenv('SALESFORCE_PASSWORD'),
                    security_token=os.getenv('SALESFORCE_SECURITY_TOKEN'),
                    domain=os.getenv('SALESFORCE_DOMAIN'))

