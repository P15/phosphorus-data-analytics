# -*- coding: utf-8 -*-
"""
Created on Mon Jan 25 12:20:56 2021

@author: Jacob-Windows
"""
import os
import glob
import pandas as pd
from common.utils import colsearch
from common import utils
import numpy as np
from datetime import datetime
import phonenumbers
from state_reporting_export import prompt


def state_reports_pandas_export(state, startdate, enddate, positives, sql_file, test):
    
       
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
                    
                    print("{} reports exported from {}".format(len(df), state))
                    return df
                
                else:
                    print("No reports from {}".format(state))


def split_area_code(df, state):
    if state in ["KS","MO"]:
        areacols = colsearch(df,["area_code"])
        phonecols = colsearch(df, ["phone"])
        df[areacols] = df[phonecols].copy()
        for col in areacols:
            df[col] = [num[0:3] for num in df[col]]
        for col in phonecols:
            df[col] = [num[3:] for num in df[col]]
    return df


def abbrev_race(df, state):
    # Some states are ok with all unknown, but most complain about that and have refused to release us into production without some races/ethnicities
    # May want to eventually handle this within the SQL query
    racedict = {"European" : "W",
                "Latin American" : "W",
                "East Asian" : "A",
                "African" : "B",
                "Other" : "O",
                "Unknown" : "U",
                "Unspecified" : "U",
                " " : "U"}
    
    nstates = ["MO"]
    nothispanic = ["N" if state in nstates else "NH"][0]
    
    racecol = colsearch(df, "race")[0]
    ethcol = colsearch(df, "ethnicity")[0]
    
    
    df[ethcol] = df[racecol].copy()
    df[racecol] = df[racecol].replace(racedict).fillna("U")
    df[racecol] = ["U" if len(x) > 1 else x for x in df[racecol]]
    df[ethcol] = ["U" if x == "U" else nothispanic for x in df[ethcol]]
    return df

def phonenums(df, state, phoneform):
    phonecols = colsearch(df,"phone")
    df[phonecols] = df[phonecols].fillna("855-746-7423")
    for col in phonecols:
        if phoneform == "dashes":
            df[col] = [phonenumbers.format_number(phonenumbers.parse(num,"US") , phonenumbers.PhoneNumberFormat.INTERNATIONAL).replace("+1","").strip() for num in df[col]]
        elif phoneform == "nodashes":
            df[col] = [phonenumbers.format_number(phonenumbers.parse(num,"US") , phonenumbers.PhoneNumberFormat.E164).replace("+1","").strip() for num in df[col]]
        elif phoneform == "national":
            df[col] = [phonenumbers.format_number(phonenumbers.parse(num,"US") , phonenumbers.PhoneNumberFormat.NATIONAL).replace("+1","").strip() for num in df[col]]
    return df
        
def dates(df, state, dateform):
    if "WV" not in state:
        datecols = colsearch(df,["date", "dt", "dob"]) # West Virginia has a case that breaks things when "dt" is searched for
    else:
        datecols = colsearch(df,["date", "dob"])
        
    for col in df[datecols]:
        df[col]=pd.to_datetime(df[col]).dt.tz_localize(None)
        if "time" not in col.lower():
            df[col]=[x.strftime(dateform) if x is not pd.NaT else np.nan for x in df[col]]
        else:
            df[col]=[x.strftime(dateform + " %H:%M") if x is not pd.NaT else np.nan for x in df[col]]
    
    return df

def specimen_source(df, state):
    if state == "MO":
        df["Specimen_Source"]="OT"
    return df

def reformat(df, state, phoneform, dateform):
    df = phonenums(df, state, phoneform)
    df = split_area_code(df, state)
    df = abbrev_race(df, state)
    df = dates(df, state, dateform)
    df = specimen_source(df, state)
    return df

def get_template(state, this_dir):
    templates_dir = os.path.join(this_dir + "\\templates")
    phos_template = os.path.join(templates_dir, 'phosphorus_columns.csv')
    state_template = os.path.join(templates_dir, '{}_template.csv'.format(state.lower()))
    statecolumns = pd.read_csv(state_template).reset_index()
    phoscols = pd.read_csv(phos_template)
    template=statecolumns.merge(phoscols, on="colid")
    phoneform = template.phoneform.drop_duplicates()[0]
    dateform = template.dateform.drop_duplicates()[0]
    return template, phoneform, dateform

if __name__=="__main__":  
      
    
    startdate, enddate, states, positives, lastexport, test = prompt()
           
    if test:
        step3path = os.environ["gdrive_state_reporting_local_location"] + "/{}/Step 3 Ready to Send".format(enddate.strftime("%B/test"))
    else:
        step3path = os.environ["gdrive_state_reporting_local_location"] + "/{}/Step 3 Ready to Send".format(enddate.strftime("%B/%Y_%m_%d"))
    
    # this_file = os.path.abspath("C:/Users/Jacob-Windows/Documents/Phosphorus/phosphorus-data-analytics/state-reporting/state-reporting.py")
    this_file = os.path.abspath(__file__)
    this_dir = os.path.dirname(this_file)
    sql_file = os.path.join(this_dir, 'get_state_reports.sql')
    
    for state in states:
        export = state_reports_pandas_export(state, startdate, enddate, positives, sql_file, test).transpose()
        try:
            template, phoneform, dateform = get_template(state, this_dir)
            df = template.merge(export, left_on = "phos_colname", right_index=True, how='left').sort_values(by="index")
            
            df = df.set_index("{}_colname".format(state.lower()))\
                    .drop(columns=["index","colid","phoneform","dateform","phos_colname"]) \
                        .transpose() \
                            .replace(" ", np.nan)
                            
            df = reformat(df, state, phoneform, dateform)
            filename = "/{}_{}.csv".format(state, enddate.strftime("%Y_%m_%d"))
            df.to_csv(step3path + filename, index=False, encoding = "utf-8")
            
        except:
            print("No template for {}".format(state))

    
