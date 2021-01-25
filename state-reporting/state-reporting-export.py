# -*- coding: utf-8 -*-
"""
Created on Thu Dec 31 10:50:27 2020

@author: Jacob-Windows
"""

import os
from datetime import datetime
import pandas as pd
from common import utils
import time



def state_reports_export(state, startdate, enddate, sql_file, step1path, day):
         
    filepath = step1path + "/" + state + day.strftime("_%Y_%m_%d") + ".csv"
    
    # Database argument defaults to "FOLLOWER". Can be changed to "STAGING","DEV", or "PROD". Not case sensitive. Prod requires ignore_role = False.
    # Role will be set to "dist_15_application_group"
    # The query being executed here only reads, does not write.
    # Reports are marked state reported after they are confirmed to have been sent.
    with utils.get_db_connection(ignore_role=True, database="FOLLOWER") as conn:
        with conn.cursor() as cur:
            with open(sql_file, 'r') as state_reports_export:
                reports = state_reports_export.read()
                reports = reports.format(startdate, enddate, state)
                cur.execute(reports)
                df=pd.DataFrame(data=cur.fetchall(),
                                columns=[desc.name for desc in cur.description])
                            
                if len(df)>0:
                    # If any reports were exported, all datetime columns are converted from UTC to EST and reformatted according to the input below.
                    # Then a CSV file will be created in the step 1 folder for each state.
                    df=utils.UTC2EST(df, "%m/%d/%Y %H:%M")
                    df.to_csv(filepath, index=False, encoding='utf-8')
                    print("{} reports exported from {}".format(len(df), state))
                
                else:
                    print("No reports from {}".format(state))


# Creates the three folders representing the 3-step process we are currently using. Returns the step 1 path since this script writes to that location.
def initialize_folders(now):
    folderpath = os.environ["gdrive_state_reporting_local_location"] + "/{}".format(now.strftime("%B/%Y_%m_%d"))
    step1path = folderpath+"/Step 1 State CSV Files"
    step2path = folderpath+"/Step 2 Transformed XLSX and PDF Files"
    step3path = folderpath+"/Step 3 Ready to Send"
    dirslist = [step1path,step2path,step3path]
    for path in dirslist:
        if not os.path.exists(path):
            print("Creating {}".format(path))
            os.makedirs(path)
    return step1path
            
if __name__=="__main__":
    
    now = datetime.now()
    enddate = now.strftime("%Y-%m-%d %H:%M")
    
    step1path = initialize_folders(now)        
    
    this_file = os.path.abspath("C:/Users/Jacob-Windows/Documents/Phosphorus/phosphorus-data-analytics/state-reporting/state-reporting.py")
    #this_file = os.path.abspath(__file__)
    this_dir = os.path.dirname(this_file)
    sql_file = os.path.join(this_dir, 'get_state_reports.sql')

    # Reads the timestamp indicating the last time this script was run. Assigns this timestamp to "startdate".
    with open("last_export","r") as file:
        startdate = file.read()
        
    
    print("Last export read as {}".format(startdate))
    



    
    states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
              "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
              "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
              "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
              "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]



    # Custom times and state. Used for special situations like emails complaining about invalid data. Should be defined by args later.
    """
    startdate=datetime(2020,1,1)
    enddate=now
    states=['NM]'
    """
    
    # Tries to export state reports three times per state, to account for connectivity issues.
    for state in states:
        retries = 0
        while retries<4:
            try:
                state_reports_export(state, startdate, enddate, sql_file, step1path, now)
                break
            except Exception as e:
                print("Retrying {} due to: {}".format(state,e))
                retries=retries+1
                time.sleep(2)
                continue

    # Writes the timestamp that was used as the end date this run, so that it can be used as the start date in the next run.
    with open("last_export","w") as file:
        file.write(enddate)
    print("Last export written as {}".format(enddate))
