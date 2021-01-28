# -*- coding: utf-8 -*-
"""
Created on Thu Dec 31 10:50:27 2020

@author: Jacob-Windows
"""

import os
import sys
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
sys.path.append(__location__ + '/../')
from datetime import datetime, timedelta
import pandas as pd
from common import utils
import time
import numpy as np
from dateutil.parser import parse

def state_reports_export(state, startdate, enddate, positives, sql_file, step1path, test):
         
    filepath = step1path + "/" + state + enddate.strftime("_%Y_%m_%d") + ".csv"
    
    endstr =  enddate.strftime("%Y-%m-%d %H:%M")
    # Database argument defaults to "FOLLOWER". Can be changed to "STAGING","DEV", or "PROD". Not case sensitive. Prod requires ignore_role = False.
    # Role will be set to "dist_15_application_group"
    # The query being executed here only reads, does not write.
    # Reports are marked state reported after they are confirmed to have been sent.
    with utils.get_db_connection(ignore_role=True, database="FOLLOWER") as conn:
        with conn.cursor() as cur:
            with open(sql_file, 'r') as state_reports_export:
                reports = state_reports_export.read()
                reports = reports.format(startdate, endstr, state)
                cur.execute(reports)
                df=pd.DataFrame(data=cur.fetchall(),
                                columns=[desc.name for desc in cur.description])
                            
                if len(df)>0:
                    # If any reports were exported, all datetime columns are converted from UTC to EST and reformatted according to the input below.
                    # Then a CSV file will be created in the step 1 folder for each state.
                    df=utils.UTC2EST(df, "%m/%d/%Y %H:%M")
                    
                    ##### THIS NEEDS TO BE SOLVED WITHIN THE QUERY ONE DAY #########
                    df["Patient Race"]=[x.split(",")[0] for x in df["Patient Race"]]
                    ############################################################
                    
                    if "P" in positives:
                        df = df[df["Result"].str.contains("Positive")]
                    if "N" in positives:
                        df = df[~df["Result"].str.contains("Positive")]
                    if test:
                        df = df.head(6)
                        
                    """
                    # This can be adapted to avoid making changes to the SQL (which is already a monster) if a case arises that causes blank addresses
                    for x in df["Ordering Facility Address"]:
                        if x.strip() is np.nan:
                        df["Ordering Facility Address"] = df["Ordering Facility Address"].replace(" ",np.nan).fillna("400 Plaza Drive Suite 401")
                        df["Ordering Facility City"] = df["Ordering Facility City"].replace(" ",np.nan).fillna("Secaucus")
                        df["Ordering Facility State"] = df["Ordering Facility State"].replace(" ",np.nan).fillna("NJ")
                        df["Ordering Facility Zip"] = df["Ordering Facility ZIP"].replace(" ",np.nan).fillna("07094")
                        
                    if df["Provider Address"].replace(" ",np.nan) is np.nan:
                        df["Provider Address"] = df["Provider Address"].replace(" ",np.nan).fillna("400 Plaza Drive Suite 401")
                        df["Provider City"] = df["Provider City"].replace(" ",np.nan).fillna("Secaucus")      
                        df["Provider State"] = df["Provider State"].replace(" ",np.nan).fillna("NJ")
                        df["Provider Zip"] = df["Provider Zip"].replace(" ",np.nan).fillna("07094")
                    """
                    
                    df.to_csv(filepath, index=False, encoding='utf-8')
                    print("{} reports exported from {}".format(len(df), state))
                
                else:
                    print("No reports from {}".format(state))


# Creates the three folders representing the 3-step process we are currently using. Returns the step 1 path since this script writes to that location.
def initialize_folders(enddate):
    folderpath = os.environ["gdrive_state_reporting_local_location"] + "/{}".format(enddate.strftime("%B/%Y_%m_%d"))
    step1path = folderpath+"/Step 1 State CSV Files"
    step2path = folderpath+"/Step 2 Transformed XLSX and PDF Files"
    step3path = folderpath+"/Step 3 Ready to Send"
    dirslist = [step1path,step2path,step3path]
    for path in dirslist:
        if not os.path.exists(path):
            print("Creating {}".format(path))
            os.makedirs(path)
    return step1path
         

def prompt():
    print("""
  ###########################        
  #  STATE REPORTING TOOL   #
  ###########################  
  
This tool can be used to export COVID-19 qPCR reports and send them to SFTP servers owned by state departments of health. This satisfies Phosphorus' legal requirement to report COVID-19 test results to public health authorities within 24 hours of test results being obtained.
    
This can be run on a schedule. The lack of any input will cause default behavior.
    
Default Behavior: send all unreported information from the last 7 days to state departments of health
    
To obtain different behavior, enter input now.
    
To obtain default behavior, press enter/return when prompted for input.
    
Dates should be entered as m/d/YYYY. Time should be entered as HH:MM. If no time is provided, default is midnight.
States should be entered as a two letter abbreviation.
    """)

    now = datetime.now()
    
    try:
        with open("last_export","r") as file:
            lastexport = file.read()
    except:
        print("It looks like this is your first time running this program on your machine.")
        
    states = []
    #startdate = parse(input("Starting Sent Date [m/d/YYYY]: ") or (now - timedelta(days=7)).strftime("%m/%d/%Y"))
    startdate = parse(input("Starting Sent Date [m/d/YYYY]: ") or lastexport)
    print(startdate)
    enddate = parse(input("Ending Sent Date [m/d/YYYY]: ") or now.strftime("%m/%d/%Y %H:%S"))
    print(enddate)
    states = ["{}".format(input("State: ").upper())]
    print(states)
    positives = input("Postives [P for only positives, N to omit positives]: ").upper()
    print(positives)
    print("If this is for a test, only 6 rows should be sent")
    test = input("Write TEST in all caps if this is a test:")
    if test == "TEST":
        test = True
        
    
    if states == '':
        states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
              "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
              "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
              "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
              "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]
    
    print("")
    print("Last export read as {}".format(lastexport))
    print("Getting reports from {} to {}".format(startdate,enddate))
    if len(states) <= 50:
        print("State: {}".format(states))
    else:
        print("State: all states")
    print("Positive reports: {}".format(positives))
    
    confirmation = input("Confirm you would like to proceed with the export [y/n]: ").upper()
    print("")
    if "Y" not in confirmation:
        sys.exit("User did not confirm. Exiting program. Nothing has happened.")
        
    return startdate, enddate, states, positives, lastexport, test
    
if __name__=="__main__":
    
    now = datetime.now()
    
    
    startdate, enddate, states, positives, lastexport, test = prompt()
    
    step1path = initialize_folders(enddate)        
    if test:
        step1path = os.environ["gdrive_state_reporting_local_location"] + "/{}".format(enddate.strftime("%B/")) + "test"
    
    this_file = os.path.abspath("C:/Users/Jacob-Windows/Documents/Phosphorus/phosphorus-data-analytics/state-reporting/state-reporting.py")
    #this_file = os.path.abspath(__file__)
    this_dir = os.path.dirname(this_file)
    sql_file = os.path.join(this_dir, 'get_state_reports.sql')
        
    
    # Tries to export state reports three times per state, to account for connectivity issues.
    for state in states:
        retries = 0
        while retries<3:
            try:
                state_reports_export(state, startdate, enddate, positives, sql_file, step1path, test)
                break
            except Exception as e:
                print("Retrying {} due to: {}".format(state,e))
                retries=retries+1
                time.sleep(2)
                continue

    #now=datetime(2021,1,25,7,45)
    # Writes the timestamp that was used as the end date this run, so that it can be used as the start date in the next run.
    if not test:
        with open("last_export","w") as file:
            file.write(now.strftime("%Y-%m-%d %H:%M"))
        print("Last export written as {}".format(now.strftime("%Y-%m-%d %H:%M")))
