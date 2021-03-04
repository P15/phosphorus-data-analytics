# -*- coding: utf-8 -*-
"""
Created on Thu Jan  7 22:12:18 2021

@author: Jacob-Windows
"""
import os
import sys
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
sys.path.append(__location__ + '/../')
import pandas as pd
import glob
from datetime import datetime
import yagmail
from common.utils import liststringsearch
from dateutil.parser import parse

efaxaddress=pd.read_csv("C:/Users/Jacob-Windows/Documents/Phosphorus/Python/state-reporting/statefaxnumbers.csv", index_col=0).to_dict()['efax']
user = 'jacob@phosphorus.com'
app_password = os.environ["gmail_app_pass"]





now = datetime.now()
day = parse(input("Day to send [MM/DD/YYYY]: ") or now.strftime("%m/%d/%Y %H:%S"))
folderpath="C:/Users/Jacob-Windows/Google Drive/State Reporting/{}".format(day.strftime("%B %Y/%Y_%m_%d"))
inpath=folderpath+"/PDFs to fax"
dirslist=[folderpath,inpath]
for path in dirslist:
    if not os.path.exists(path):
        print("Creating {}".format(path))
        os.makedirs(path)


errors = pd.DataFrame()
paths = glob.glob(inpath+"/*.pdf")
folder   = [os.path.basename(x) for x in paths]
filename = [name.split(".")[0] for name in folder]  
states    = [file.split("_")[0] for file in filename]    
seenstates = []
    
if len(states)==0:
    print("No files to send in: %s" %path)
    
for state in states:
    send=True
    test=False
    try:
        if test:
            to = 'jacob@phosphorus.com'
        else:
            to = efaxaddress[state]
        
        subject = 'Phosphorus COVID-19 Testing Reports - {}'.format(state)
        pdffile = liststringsearch(paths, state)
        logo = "C:/Users/jacob/OneDrive/Phosphorus - Copy/Python/state-reporting/Logotype_Diagnostics.png"
        
        message = """
        Phosphorus COVID - 19 Testing Reports - {}
        
        Reporter:
        Jacob Bayer
        Operatations Data Analyst
        jacob@phosphorus.com
        """.format(state)
        
        content = [
            message,
            #yagmail.inline(logo), 
            pdffile]
    except KeyError:
        send=False
        print("Error finding {}".format(state))
        errors=errors.append({"state": state,
                              "error":"No PDF"}, ignore_index=True)
    
    
    if send:
        try:
            with yagmail.SMTP(user, app_password) as yag:
                yag.send(to, subject, content)
                print('Sent efax successfully - {}'.format(state))   
        except Exception as e:
            print("Error sending email")
            errors=errors.append({state:e}, ignore_index=True)
    