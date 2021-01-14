# -*- coding: utf-8 -*-
"""
Created on Thu Dec 31 10:50:27 2020

@author: Jacob-Windows
"""

import os
from datetime import datetime
import pandas as pd
from common import myutils
import numpy as np



def state_reports_export(state, startdate, enddate):
    
    this_file = os.path.abspath(__file__)
    this_dir = os.path.dirname(this_file)
    sql_file = os.path.join(this_dir, 'get_state_reports.sql')
    
    
    writepath = "C:/Users/Jacob-Windows/Google Drive/State Reporting/January/2021_01_14/Step 1 State CSV files/"
    
    filepath = writepath + state + datetime.now().strftime("_%Y_%m_%d") + ".csv"
    
        
    with myutils.get_db_connection(ignore_role=True) as conn:
        with conn.cursor() as cur:
            with open(sql_file, 'r') as state_reports_export:
                reports = state_reports_export.read()
                reports = reports.format(startdate, enddate, state)
                cur.execute(reports)
                df=pd.DataFrame(data=cur.fetchall(),
                                columns=[desc.name for desc in cur.description])
                            
                if len(df)>0:
                    print("{} reports marked state reported from {}".format(len(df), state))
                    df=myutils.UTC2EST(df, "%m/%d/%Y %H:%M")
                    df.to_csv(filepath, index=False, encoding='utf-8')
                
                else:
                    print("No reports from {}".format(state))
                
            
if __name__=="__main__":
    today = datetime.now()
    enddate = today.strftime("%Y-%m-%d")
    startdate = datetime(2021,1,6).strftime("%Y-%m-%d")

    
    states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
              "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
              "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
              "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
              "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]
    
    for state in states:
        
        state_reports_export(state,startdate,enddate)

"""
FOR TESTING        
engine=create_engine(os.environ["PROD_FOLLOWER_DATABASE_URL"])
df=pd.read_sql_query(reports,engine)
"""