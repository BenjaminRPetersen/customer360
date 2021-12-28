# -*- coding: utf-8 -*-
"""
Title: Redshift Credentials
Created on Thu Sep  9 12:24:58 2021

@author: benjaminrpetersen
"""

from sqlalchemy import create_engine
import os
redshift_username = os.environ.get('REDSHIFT_USERNAME')
redshift_password = os.environ.get('REDSHIFT_PASSWORD')

conn = create_engine('postgresql://'+redshift_username+':'+redshift_password+'@rs-dev-tst1.prod-lde.bsp.gsa.gov:5439/edw')



