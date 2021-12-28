# -*- coding: utf-8 -*-
"""
Title: BI-Schema Scheduler

Created on Wed Dec 15

@author: benjaminrpetersen
"""

"""
Import Packages, Tools and Authorizations
"""
import os, datetime # Standard Packages
os.chdir(os.path.dirname(os.path.realpath(__file__)))
parent_path = os.path.dirname(os.getcwd())
os.chdir(parent_path+'/authorizations/') # System Authorizations
from redshift import conn
start = datetime.datetime.now()
          
"""
Update data in Redshift
"""
cur = conn.raw_connection().cursor()
cur.execute("call customer360.d_region(); commit;")
rows = conn.execute("select count(*) ct from customer360.d_region").fetchall()[0][0]
end = datetime.datetime.now()
difference = (end-start)
difference = difference.total_seconds()

