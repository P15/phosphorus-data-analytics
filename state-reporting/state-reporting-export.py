# -*- coding: utf-8 -*-
"""
Created on Thu Dec 31 10:50:27 2020

@author: Jacob-Windows
"""

import os
from datetime import datetime
import pandas as pd
from common import myutils
import time



def state_reports_export(state, startdate, enddate, sql_file):
      
    day = datetime.now()
    
    writepath = os.environ["gdrive_state_reporting_local_location"] + "/{}/Step 1 State CSV Files".format(now.strftime("%B/%Y_%m_%d"))
    
    filepath = writepath + state + day.strftime("_%Y_%m_%d") + ".csv"
    
        
    with myutils.get_db_connection(ignore_role=True, database="FOLLOWER") as conn:
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

def initialize_folders(now):
    folderpath = os.environ["gdrive_state_reporting_local_location"] + "/{}".format(now.strftime("%B/%Y_%m_%d"))
    step1path = folderpath + "/Step 1 State CSV Files"
    step2path = folderpath + "/Step 2 Transformed XLSX and PDF Files"
    step3path = folderpath + "/Step 3 Ready to Send"
    dirslist = [step1path,step2path,step3path]
    for path in dirslist:
        if not os.path.exists(path):
            print("Creating {}".format(path))
            os.makedirs(path)
            
if __name__=="__main__":
    
    now = datetime.now()
    enddate = now.strftime("%Y-%m-%d %H:%M")
    
    initialize_folders(now)        
    
    this_file = os.path.abspath(__file__)
    this_dir = os.path.dirname(this_file)
    sql_file = os.path.join(this_dir, 'get_state_reports.sql')

    
    with open("last_export","r") as file:
        startdate = file.read()
        
    
    print("Last export read as {}".format(startdate))
    


    
    states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
              "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
              "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
              "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
              "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

    
    for state in states:
        errorcount = 0
        while errorcount<6:
            try:
                state_reports_export(state,startdate,enddate,sql_file)
                break
            except Exception as e:
                print("Retrying {} due to: {}".format(state,e))
                errorcount=errorcount+1
                time.sleep(2)
                continue
        
    with open("last_export","w"), open as file:
        file.write(enddate)
    print("Last export written as {}".format(enddate))
