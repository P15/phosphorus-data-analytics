# -*- coding: utf-8 -*-
"""
Created on Thu Jan 14 12:05:24 2021

@author: Jacob-Windows
"""


import os
import pandas as pd
import glob
import numpy as np
from datetime import datetime
import re
from common.utils import colsearch
        

def add_leading_zeros(df,file):
    
    state = os.path.basename(file) \
            .split(".")[0] \
            .split("_")[0]
    
    zipcols=colsearch(df,"zip")
    
    # Pandas' read_excel function drops the leading zero on zip codes and I can't find a way to avoid that.
    # This adds that leading zero back, if it is missing. 
    for col in zipcols:
        newcol = []
        for code in df[col]:
            if code is not np.nan:
                if len(code)==4:
                    newcol.append("0"+code)
                else:
                    newcol.append(code)
            else:
                newcol.append(code)
            if code == state:
                newcol.append(np.nan)
        df[col] = newcol
    df[zipcols] = df[zipcols].replace("0","")
    return df

    
def dates(df,file):
    # Reformats dates according to the dateformatdict. At some point I should take the time to look at every states template and re-establish
    # the correct date format.
    # Using select_dtypes is an alternate solution, but I've chosen to read the entire dataframe as strings and use this method instead.
    # This is also the way that all of the other validation methods are handled.
    if "WV" not in file:
        datecols = colsearch(df,["date", "dt", "dob"]) # West Virginia has a case that breaks things when "dt" is searched for
    else:
        datecols = colsearch(df,["date", "dob"])
    
    
    dateformatdict = {"%Y%m%d" : ["PA","OH","AZ","NJ","NM"] ,
                      "%m/%d/%Y" : ["ID","MO"] }
    
    state = os.path.basename(file) \
            .split(".")[0] \
            .split("_")[0]

    formset = False # Using this allows there to be a default format without having to add all the states to the list in the dict
    for form, states in dateformatdict.items():
        if state in states:
            dateformat = form
            formset =  True
            
    if not formset:
        dateformat = "%m/%d/%Y"
    
    
    for col in df[datecols]:
        df[col]=pd.to_datetime(df[col]).dt.tz_localize(None)
        if "time" not in col.lower():
            df[col]=[x.strftime(dateformat) if x is not pd.NaT else np.nan for x in df[col]]
        else:
            df[col]=[x.strftime(dateformat + " %H:%M") if x is not pd.NaT else np.nan for x in df[col]]
    return df


def phonenumbers(df, file):
    
    # Phone numbers are affected by a great deal of data entry error. Some states will reject an entire submission (hundreds of results)
    # over a phone number that is an incorrect length or format. Presumably, states would still like to have the data despite a single incorrect phone number,
    # and fixing data entry error is outside the scope of the mission for now.
    # Therefore, this function makes phone numbers formatted correctly, but makes incorrect numbers at least acceptable.
    
    phonecols = colsearch(df,"phone")
    state = os.path.basename(file) \
            .split(".")[0] \
            .split("_")[0]
            
    # Will be used in the future to make this cleaner
    phoneformatdict = {"ID" : "{}-{}-{}",
                       "OH" : "{}{}{}",
                       "PA" : "{}{}{}"}
    
    # Kansas has a separate columns for the area code and the rest of the phone number. For now I'm leaving Kansas completely alone.
    if "KS" not in file:
        
        # Creates helpful output in the console that allows a user to manually correct instances of data entry error that would otherwise
        # prevent the file from being accepted. The sad reality is that I sometimes go into elements and read the correct information off of
        # the req form if a particular patient comes up in every batch of records.
        for col in phonecols:
            newcol = []
            for idx, number in df[col].iteritems():
                if type(number) == str:
                    integers=re.findall("[0-9]", number)
                    if len(integers)==11:
                        oldnumber=number
                        if integers[0]=='1':
                            newnumber=number[1:]   
                        else:
                            newnumber=number[:10]  
                        newcol.append(newnumber)
                        print("""
                                 LONG PHONE NUMBER
                                 State: {} 
                                 Col: {} 
                                 Row: {}
                                 Phone number {} is converted to {}
                                 """.format(state, col, idx, oldnumber, newnumber))
                            
                    elif len(integers)==8:
                        print("""
                                 SHORT PHONE NUMBER
                                 State: {} 
                                 Col: {} 
                                 Row: {}
                                 Phone number {} is converted to {}
                                 """.format(state, col, idx, number, number+"0"))
                        newcol.append(number+"0")
                    elif ((len(integers)>11) | (len(integers)<8)):
                        print(file)
                        print(number)
                        print(col)
                        print(idx)
                        raise Exception("Take a look at {} in row {} col {} of {}".format(number,idx,col,file))
                    else:
                        newcol.append(number)
                else:
                    newcol.append(number)
            df[col] = newcol
        if any(x in file for x in ["PA","OH"]):
            for col in phonecols:
                    
                #print([re.findall('\d{4}$|\d{3}', phone_no) if type(phone_no) is str else None for phone_no in df[col]])
                
                df[col] = ['{}{}{}'.format(*(re.findall('\d{4}$|\d{3}', phone_no))) if type(phone_no) is str else None for phone_no in df[col]]
        
        else:
            for col in phonecols:
                df[col] = ['{}-{}-{}'.format(*(re.findall('\d{4}$|\d{3}', phone_no))) if type(phone_no) is str else None for phone_no in df[col]]
                
    return df



def validate(df, file):
    df = dates(df, file)
    df = phonenumbers(df, file)
    df = add_leading_zeros(df, file)
    return df    


def makedf(file):
    df = pd.read_excel(file, engine='openpyxl',dtype=str)
    accessioncol = colsearch(df,"accession")
    df = df.dropna(subset=accessioncol) # Doesn't apply to all states, but this in combination with the next two lines works for now.
    df = df.dropna(axis=0,thresh=7) # Often times the data coming out of the transformer has a lot of NA values with up to 5 or 6 "U" columns
    df = df.drop_duplicates() # Only applies to the NAs in MD
    df = validate(df, file)
    return df


def write_csv(step3path, df, file, now): #any string with state abbreviation

    # Seemed too risky to simply use
    # "NY" in file
    # as the boolean for the if statement. Some future user could accidentally put "IN" or "PASS" or something in the name of
    # a parent folder, triggering that for "IN" or "PA". I use a few lines to clarify here what the state is.

    state = os.path.basename(file) \
            .split(".")[0] \
            .split("_")[0]
        
    filename = state + now.strftime("_%Y_%m_%d")
    writepath = step3path + "/" + filename + ".csv"
    
    # Most states are fine with the "else" option here, but several states have special requirements.
    
    if state=="NY":
        df.to_csv(writepath,sep="|",encoding="ascii",index=False,header=False)
        
    elif state=="TX":
        df.to_csv(writepath, index=False)
        
    elif state=="IL":
        df.to_csv(writepath, sep="~", index=False, encoding="utf-8")
    
    elif state=="AR":
        df=df.dropna(subset=["Patient First Name"])
        df.to_excel(step3path + "/" + filename + ".xlsx", engine='openpyxl', index=False, encoding="utf-8")
    
    elif state=="PA":
        df.to_csv(writepath, header=False, index=False, encoding="utf-8")
        
    else:
        df.to_csv(writepath, index=False, encoding="utf-8")



if __name__=="__main__":
    now = datetime.now()
    
    # Used to set a custom day. Should be set by an arg in the future.
    #now=datetime(2021,1,25)
    
    folderpath = os.environ["gdrive_state_reporting_local_location"] + "/{}".format(now.strftime("%B/%Y_%m_%d"))
    step2path = folderpath + "/Step 2 Transformed XLSX and PDF Files"
    step3path = folderpath + "/Step 3 Ready to Send"
    

    xlsxfiles = glob.glob(step2path+"/*.xlsx")
    
    pdffiles = glob.glob(step2path+"/*.pdf")

    
    for file in xlsxfiles:
        df = makedf(file)
   
        write_csv(step3path, df, file, now) # Requires the name of the file in order to identify the state, since every transformed document will have a

