 # -*- coding: utf-8 -*-
"""
Created on Mon Jan 25 12:20:56 2021

@author: Jacob-Windows
"""
import os
import sys
import glob
import pandas as pd
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
sys.path.append(__location__ + '/../')
from common.utils import colsearch
from common import utils
import numpy as np
from datetime import datetime
import phonenumbers
from state_reporting_export import prompt
import send_to_sftp
from random import randint
import pdfkit


def write_csv(df, filename, fax): #any string with state abbreviation
    if fax:


        html="""<p><strong>COVID-19 RT-qPCR Test Results</strong></p>
        <p><strong>Phosphorus Diagnostics LLC</strong></p>
        <p><strong>400 Plaza Drive, 4th Floor, Secaucus, NJ 07094</strong></p>
        <p><strong>CLIA: 31D2123554</strong></p>

        
        
        """ + df.to_html(index=False)


        pdfkit.from_string(html, filename.split(".")[0] + ".pdf", options={'orientation':'Landscape', 'quiet':''})

    # Most states are fine with the "else" option here, but several states have special requirements.
    
    if "NY" in filename:
        df.to_csv(filename,sep="|",index=False,header=False)
        
    elif "TX" in filename:
        df.to_csv(filename, index=False)
        
    elif "IL" in filename:
        df.to_csv(filename, sep="~", index=False, encoding="utf-8", header=False)
    
    elif "AR" in filename:
        df=df.dropna(subset=["Patient First Name"])
        filename = filename.split(".")[0] + ".xlsx"
        df.to_excel(filename, engine='openpyxl', index=False, encoding="utf-8")
    
    elif "PA" in filename:
        df.to_csv(filename, header=False, index=False, encoding="utf-8")
        
    else:
        df.to_csv(filename, index=False, encoding="utf-8")



def initialize_folders(enddate):
    folderpath = os.environ["gdrive_state_reporting_local_location"] + "/{}".format(enddate.strftime("%B %Y/%Y_%m_%d"))
    step1path = folderpath+"/Exports from database"
    step2path = folderpath+"/PDFs to fax"
    step3path = folderpath+"/CSVs for ELR"
    dirslist = [step1path,step2path,step3path]
    for path in dirslist:
        if not os.path.exists(path):
            print("Creating {}".format(path))
            os.makedirs(path)
    return step1path, step2path, step3path

def state_reports_pandas_export(state, startdate, enddate, positives, sql_file, test):
    
    state = state.upper()
    
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
                    
                    ##### I'm confused about how this works in the SQL query so I handle it here for now #########
                    df["Patient Race"]=[x.split(",")[0] for x in df["Patient Race"]]
                    df["Patient Race"] = df["Patient Race"].fillna("Unknown")
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

def oklahoma(df):
    "Oklahoma is unique."
    now = datetime.now()
    df["File_created_date"] = now.strftime("%m/%d/%Y")
    df["flatfile_version_no"] = "V2020-07-01_Results"
    df["Patient_ID_type"]="Patient Internal ID"
    df["Patient_age_units"] =  "Years"
    df["Patient_race"] = ["Asked but no answer / unknown" if race=="Unknown" else race for race in df["Patient_race"]]
    df["Patient_ethnicity"] = ["Patient Declines" if race=="Patient Declines" else "Not Hispanic or Latino" for race in df["Patient_race"]]
    df["Patient_gender"] = ["Male" if x == "M" else x for x in df["Patient_gender"]]
    df["Patient_gender"] = ["Female" if x == "F" else "Unknown" for x in df["Patient_gender"]]
    return df

def utah(df):
    df['state'] = 'Utah'
    df["birth_sex"] = ["Male" if x == "M" else x for x in df["birth_sex"]]
    df["birth_sex"] = ["Female" if x == "F" else "Unknown" for x in df["birth_sex"]]
    df["ethnicity"] = ["Unknown" if race=="Unknown" else "Not Hispanic or Latino" for race in df["ethnicity"]]
    df["race"] = ["Unknown" if race=="Other" else race for race in df["race"]]
    df["race"] = ["White" if race=="European" else race for race in df["race"]]
    df["organism"] = "Novel Coronavirus (SARS-CoV-2)"
    df["test_type"] = "Phosphorus Diagnostics COVID-19 RT-qPCR Test"
    df["test_results"] = ["Positive / Reactive" if result.lower()=="positive" else "Negative / Non-reactive"  for result in df["test_results"]]
    df["test_status"] = "Final"
    return df

    
def split_area_code(df, state):
    if state in ["KS","MO"]:
        areacols = colsearch(df,["area_code"])
        phonecols = colsearch(df, ["phone"])
        df[areacols] = df[phonecols].copy()
        for col in areacols:
            df[col] = [num[0:3] for num in df[col]]
        for col in phonecols:
            df[col] = [num[3:] for num in df[col]]
            #df[col] = [num[1:] if num[0] == '-' else num for num in df[col]]
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
    
    nstates = ["MO","MD", "OR"]
    nothispanic = "N" if state in nstates else "NH"        
    
    racecol = colsearch(df, "race")[0]
    ethcol = colsearch(df, "ethnic")[0] # May need to change this to eth
    
    if state.upper() != "MD":
        df[racecol] = df[racecol].replace(racedict).fillna("U")
        df[racecol] = ["U" if len(x) > 1 else x for x in df[racecol]]
        df[ethcol] = df[racecol].copy()
        df[ethcol] = ["U" if x == "U" else nothispanic for x in df[ethcol]]
    else:
        df[ethcol] = df[racecol].copy()
        df[ethcol] = ["U" if x == "Unknown" else nothispanic for x in df[ethcol]]
    return df

def phonenums(df, state, phoneform):
    phonecols = colsearch(df,["phone",'dr_ph#'],exclude=["GuardianPhoneNumber","EmployerPhoneNumber"])
    
    df[phonecols] = df[phonecols].fillna("855-746-7423")
    for col in phonecols:
        df[col] = [x.split("/")[0] if x is not np.nan else x for x in df[col]]
        df[col] = [x.split(",")[0] if x is not np.nan else x for x in df[col]]
        df[col]
        if phoneform == "dashes":
            df[col] = [phonenumbers.format_number(phonenumbers.parse(num,"US") , phonenumbers.PhoneNumberFormat.INTERNATIONAL).replace("+1","").strip() for num in df[col]]
        elif phoneform == "nodashes":
            df[col] = [phonenumbers.format_number(phonenumbers.parse(num,"US") , phonenumbers.PhoneNumberFormat.E164).replace("+1","").strip() for num in df[col]]
        elif phoneform == "national":
            df[col] = [phonenumbers.format_number(phonenumbers.parse(num,"US") , phonenumbers.PhoneNumberFormat.NATIONAL).replace("+1","").strip() for num in df[col]]
    return df
        
def dates(df, state, dateform):
    if "WV" not in state.upper():
        datecols = colsearch(df,["date", "dt", "dob"]) # West Virginia has a case that breaks things when "dt" is searched for
    else:
        datecols = colsearch(df,["date", "dob"])
    
    timeform = "%I:%M %p" if state == "OK" else "%H:%M"
    
    for col in df[datecols]:
        df[col]=pd.to_datetime(df[col]).dt.tz_localize(None)
        if "time" not in col.lower():
            df[col]=[x.strftime(dateform) if x is not pd.NaT else np.nan for x in df[col]]
        else:
            df[col]=[x.strftime(dateform + " " + timeform) if x is not pd.NaT else np.nan for x in df[col]]
    
    return df

def unique_codes(df, state):
    if state == "PA":
        df["SpecimenSource"] = df["SpecimenSource"].replace("Saliva","SAL")
        df["SpecimenSource"] = df["SpecimenSource"].replace("Swab","NPNX") 
    
    if state == "KS":
        df["Specimen_Source"] = df["Specimen_Source"].replace("Saliva","SP")
    
    if state == "MO":
        df["Specimen_Source"]="OT"
    
    if state == "NJ":
        df["PATIENT_RACE"] = "2131-1"
        df = df.apply(lambda x: x.astype(str).str.upper())
        df = df.replace("NAN",np.nan)
        
        
    if state.upper() == "OH":
        hexidlist=[]
        for hexid in df['Message Control ID']:
            hexid = hex(randint(1000000, 4294967295)).split('x')[-1].upper()
            hexidlist.append(hexid)
        df['Message Control ID'] = hexidlist
        
    return df

def reformat(df, state, phoneform, dateform, fax=False):

    
    
    df = phonenums(df, state, phoneform)
    df = dates(df, state, dateform)
    if not fax:
        df = split_area_code(df, state)
    
    
    
        
        if state.upper() == 'OK':
            df = oklahoma(df)
        elif state.upper() == 'ID':
            df["Patient Race"] = df["Patient Race"].fillna("asked but unknown")
        elif state.upper() == 'UT':
            df = utah(df)
        elif state.upper() not in ["DC","ND"]:
            df = abbrev_race(df, state)
        
            
        df = unique_codes(df, state)

        
    return df

def get_template(state, this_dir, fax=False):
    if fax:
        state='fax'
    templates_dir = os.path.join(this_dir + "\\templates")
    phos_template = os.path.join(templates_dir, 'phosphorus_columns.csv')
    state_template = os.path.join(templates_dir, '{}_template.csv'.format(state.lower()))
    statecolumns = pd.read_csv(state_template).reset_index()
    phoscols = pd.read_csv(phos_template)
    template = statecolumns.merge(phoscols, on="colid")
    phoneform = template.phoneform.drop_duplicates()[0]
    dateform = template.dateform.drop_duplicates()[0]
    return template, phoneform, dateform

if __name__=="__main__":  
    
    startdate, enddate, states, positives, lastexport, test = prompt()

    
    step1path, step2path, step3path = initialize_folders(enddate)
           
    if test:
        step2path = os.environ["gdrive_state_reporting_local_location"] + "/{}/PDFs to fax".format(enddate.strftime("%B %Y/test"))
        step3path = os.environ["gdrive_state_reporting_local_location"] + "/{}/CSVs for ELR".format(enddate.strftime("%B %Y/test"))

    
    this_file = os.path.abspath("C:/Users/Jacob-Windows/Documents/Phosphorus/phosphorus-data-analytics/state-reporting/state-reporting.py")
    #this_file = os.path.abspath(__file__)
    this_dir = os.path.dirname(this_file)
    sql_file = os.path.join(this_dir, 'get_state_reports.sql')
    templates_dir = os.path.join(this_dir + "\\templates")
    templates = glob.glob(templates_dir+"/*")
    
    
    stateswtemplates = []
    for file in templates:
        state = os.path.basename(file) \
            .split(".")[0] \
            .split("_")[0]
            
        stateswtemplates.append(state)
    
    
    
    for state in states:
        export = utils.trymany(state_reports_pandas_export, state, startdate, enddate, positives, sql_file, test)
        if export is not None:
            reports_to_mark = export["Report ID"]
            export = export.transpose()
        else: continue
        
        if state.lower() not in stateswtemplates:
            print("No template for {}".format(state))
            print("Creating fax")
            fax=True
        else:
            fax=False
            
        template, phoneform, dateform = get_template(state, this_dir, fax)

        
        df = template.merge(export, left_on = "phos_colname", right_index=True, how='left').sort_values(by="index")
        
        if fax:
            templatesource = 'fax'
        else:
            templatesource = state.lower()
        
        filename = step3path + "/{}_{}.csv".format(state.upper(), enddate.strftime("%Y_%m_%d"))
            
        df = df.set_index("{}_colname".format(templatesource))\
                .drop(columns=["index","colid","phoneform","dateform","phos_colname"]) \
                    .transpose() \
                        .replace(" ", np.nan)
                        
        df = reformat(df, state, phoneform, dateform, fax)

        write_csv(df, filename, fax)
        print("to " + filename)
        # send_to_sftp.send_to_sftp(filename)
        #mark_state_reported(reports_to_mark)