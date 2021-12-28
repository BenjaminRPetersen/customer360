# -*- coding: utf-8 -*-
"""
Title: s_fpds_obligations Scheduler

Created on Tue Oct 19 15:59:08 2021

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
cur.execute("call customer360.s_fpds_obligations(); commit;")
rows = conn.execute("select count(*) ct from customer360.s_fpds_obligations").fetchall()[0][0]
end = datetime.datetime.now()
difference = (end-start)
difference = difference.total_seconds()

