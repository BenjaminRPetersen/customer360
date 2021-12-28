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
cur.execute("drop view if exists customer360.v_customer cascade; commit;")
cur.execute("drop view if exists customer360.v_agency cascade; commit;")
cur.execute("drop view if exists customer360.v_region cascade; commit;")
cur.execute("drop view if exists customer360.v_category cascade; commit;")
cur.execute("drop view if exists customer360.v_case cascade; commit;")
rows = 0
end = datetime.datetime.now()
difference = (end-start)
difference = difference.total_seconds()

