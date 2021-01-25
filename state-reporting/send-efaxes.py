# -*- coding: utf-8 -*-
"""
Created on Thu Jan  7 22:12:18 2021

@author: Jacob-Windows
"""
import os
import pandas as pd
import glob
from datetime import datetime
import yagmail
from common.utils import liststringsearch


efaxaddress=pd.read_csv("C:/Users/Jacob-Windows/Documents/Phosphorus/Python/state-reporting/statefaxnumbers.csv", index_col=0).to_dict()['efax']
user = 'jacob@phosphorus.com'
app_password = os.environ["gmail_app_pass"]





day=datetime.now()
#day=datetime(2021,1,8)
folderpath="C:/Users/jacob/Google Drive/State Reporting/January/{}".format(day.strftime("%Y_%m_%d"))
inpath=folderpath+"/Step 2 Transformed XLSX and PDF Files"
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
            #message,yagmail.inline(logo), 
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
    
    

"""
from PyPDF2 import PdfFileWriter, PdfFileReader

outfile = "C:/Users/Jacob-Windows/Google Drive/State Reporting/State reporting files/January/2021_01_07/Created/(TEST) DE_2021_01_07.pdf"
 
pdfReader = PdfFileReader(open(pdffile, 'rb'))
output = PdfFileWriter()


for i in range(0,pdfReader.getNumPages()):
    pageobj = pdfReader.getPage(i).getContents()
    if pageobj:
        print(i)
        output.addPage(pageobj)
output.write(open(outfile, 'wb'))
"""             
                    
                    