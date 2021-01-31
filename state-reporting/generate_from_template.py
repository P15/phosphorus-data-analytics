# -*- coding: utf-8 -*-
"""
Created on Mon Jan 25 12:20:56 2021

@author: Jacob-Windows
"""
import os
import glob
import pandas as pd
from common.utils import colsearch
from sqlalchemy import create_engine
import numpy as np
from datetime import datetime
import phonenumbers

def update_templates():
    con = create_engine(os.environ["LOCAL_DB_URL"])
    folder = glob.glob("C:/Users/Jacob-Windows/Google Drive/State Reporting/Column IDing Project/Finished Templates/*.csv")
    for file in folder:
        name = os.path.basename(file).split(".")[0]
        pd.read_csv(file)\
            .to_sql(name,con=con,if_exists="replace")


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
                "East Asian" : "A",
                "African" : "B",
                "Other" : "O",
                "Unknown" : "U",
                " " : "U"}
    
    nstates = ["MO"]
    nothispanic = ["N" if state in nstates else "NH"][0]
    
    racecol = colsearch(df, "race")[0]
    ethcol = colsearch(df, "ethnicity")[0]
    
    df[racecol] = df[racecol].replace(racedict).fillna("U")
    df[ethcol] = df[racecol].copy()
    df[ethcol] = ["U" if x == "U" else nothispanic for x in df[ethcol]]
    return df

def phonenums(df, state, con):
    phoneform = pd.read_sql_query("select distinct phoneform from {}_template where phoneform is not null".format(state),con)["phoneform"][0]  
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
        
def dateform(df, state, con):
    dateformat = pd.read_sql_query("select distinct dateform from {}_template where dateform is not null".format(state),con)["dateform"][0]  
    if "WV" not in state:
        datecols = colsearch(df,["date", "dt", "dob"]) # West Virginia has a case that breaks things when "dt" is searched for
    else:
        datecols = colsearch(df,["date", "dob"])
        
    for col in df[datecols]:
        df[col]=pd.to_datetime(df[col]).dt.tz_localize(None)
        if "time" not in col.lower():
            df[col]=[x.strftime(dateformat) if x is not pd.NaT else np.nan for x in df[col]]
        else:
            df[col]=[x.strftime(dateformat + " %H:%M") if x is not pd.NaT else np.nan for x in df[col]]
    
    return df

def specimen_source(df, state):
    if state == "MO":
        df["Specimen_Source"]="OT"
    return df

def reformat(df, state, con):
    df = phonenums(df, state, con)
    df = split_area_code(df, state)
    df = abbrev_race(df, state)
    df = dateform(df, state, con)
    df = specimen_source(df, state)
    return df



if __name__=="__main__":

    now = datetime.now()
    
    con = create_engine(os.environ["LOCAL_DB_URL"])

    
    states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
              "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
              "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
              "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
              "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]
    
    folderpath = os.environ["gdrive_state_reporting_local_location"] + "/{}".format(now.strftime("%B/%Y_%m_%d"))
    step1path = folderpath+"/Step 1 State CSV Files/"
    step3path = folderpath+"/Step 3 Ready to Send/"
    
    for state in states:
        try:
            filename = state + now.strftime("_%Y_%m_%d") + ".csv"
            df = pd.read_csv(step1path + filename).transpose()
        except:
            print("No reports for {} on this day".format(state))
        try:
            dfcolumns = pd.read_sql_query("select s.index as stateorder, s.col as statecol, p.col as phoscol from {}_template s join phosphorus_columns p on s.id = p.id order by s.index".format(state),con)
            df = dfcolumns.merge(df, left_on="phoscol", right_index=True, how='left').sort_values(by="stateorder")
            df = df.set_index(["stateorder","statecol","phoscol"])
            df = df.transpose()
            df = df.replace(" ",np.nan)
            df.columns = df.columns.get_level_values("statecol")
            df = reformat(df, state, con)
            df.to_csv(step3path + filename, index=False, encoding = "utf-8")
            
        except:
            print("No template for {}".format(state))

    
