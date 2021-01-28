 # -*- coding: utf-8 -*-
"""
Created on Sat Jan  9 13:13:48 2021

@author: jacob
"""

import os
import pysftp
import glob
import pandas as pd
from datetime import datetime
import numpy as np
from mark_reported import mark_state_reported
import time
from dateutil.parser import parse

def renameOH(file, step3path, now):
    newfilename = step3path + "Phosphorus Diagnostics LLC_{}.csv".format(now.strftime("%Y%m%d"))
    pd.read_csv(file).to_csv(newfilename,index=False,encoding="utf-8")
    os.remove(file)
    return newfilename
    
def renameCA(file, step3path, now):
    newfilename = step3path + "PhosphorusDiagnostics_{}_1.csv".format(now.strftime("%Y%m%d"))
    pd.read_csv(file).to_csv(newfilename,index=False,encoding="utf-8")
    os.remove(file)
    return newfilename


    


if __name__=="__main__":
        
    sftpcreds=pd.read_csv(os.environ["SFTP_credfile"], index_col=0)
    
    
    this_file = os.path.abspath(__file__)
    this_dir = os.path.dirname(this_file)
    sql_file = os.path.join(this_dir, 'mark_state_reported.sql')
    
    
    now=parse(input("Day to send [MM/DD/YYYY]: ") or datetime.now())
    folderpath = os.environ["gdrive_state_reporting_local_location"] + "/{}".format(now.strftime("%B/%Y_%m_%d"))
    step1path = folderpath+"/Step 1 State CSV Files/"
    step3path = folderpath+"/Step 3 Ready to Send/"
    
    paths = glob.glob(step3path+"*")
    
    # Reads the file ~/.ssh/known_hosts
    # Only hosts with a fingerprint documented in the known_hosts file are trusted
    cnopts = pysftp.CnOpts()
    
    # Sometimes this needs to be used
    exclusionlist = ["NM"]
    
    
    
    with open("send_log","r") as logfile:
        sentfiles = logfile.read()
    
    for file in paths:
        state = os.path.basename(file)\
                  .split(".")[0]\
                  .split("_")[0]
        
        send_success = False
        if file in sentfiles:
            print("{} file seen in sentfiles".format(state))
            continue
        
        if state in sftpcreds.index:
            creds=sftpcreds.loc[state]
            
            
            if state == "OH":
                file = renameOH(file, step3path, now)
            if state == "CA":
                file = renameCA(file, step3path, now)
            
            if creds["prod"]:
                remotedir = creds.proddir
            else:
                remotedir = creds.testdir
            
            showwarning = True
            
            if showwarning:
                print("""
WARNING: THE FILE AT

{}

WILL BE SENT TO

HOST: {}
DIR: {}

AND MARKED AS STATE REPORTED
BECAUSE PROD = {}

        """.format(file,creds.hostname, remotedir, creds["prod"]))
    
                confirm = input("""
    Confirm? [y/n]:
    """)    
            if "break" in confirm.lower():
                raise Exception("User break")
            if "y" not in confirm.lower():
                print("User failed to confirm")
                time.sleep(3)
                continue


            
            
            # I have tested this for each individual state. I have followed up with email to confirm that the file was received with no issues.
            # Nevertheless, you should set up with state = 'test' to prevent you from accidentally running the script.
            # You can choose to set up a local SFTP server to run the test using this: https://www.solarwinds.com/free-tools/free-sftp-server
            

            # if state == 'test':
            # Indent from here to end to test
            try:
                # This section creates the connection to the server.
                # There are two types of authentication. One requires a password to access the server,
                # the other requires a private key file, which is locked by a password. 
                # If a credfile location is not provided, the first method will be used.
                if creds.credfile is np.nan: 
                     srv = pysftp.Connection(creds.hostname, 
                                username=creds.username, 
                                password=creds.password,
                                cnopts=cnopts)
                else:
                     srv = pysftp.Connection(creds.hostname, 
                                username=creds.username, 
                                private_key=creds.credfile,
                                private_key_pass=creds.password)
         
                # Transfers the file to the correct directory in the SFTP server.
                # send_success = True if the file is in the folder after being sent.
                # Note: For most states, the file will cease to be visible to us almost immediently after it is sent to the server,
                # but for other states the file is perpetually visible.
                with srv.cd(remotedir):
                     srv.put(file)
                     send_success = any([remotefilename in file for remotefilename in srv.listdir()])
                     
                # not sure if this is how I want to handle this
                with open("send_log","a+") as logfile:
                    logfile.write(file+"\n")
                    print("{} appended to send log".format(file))
                    
                # If something prevents the file from being sent it will probably raise an error prior to this point. This is a final check.
                if not send_success:
                     raise Exception("Send not successful")
                
                # Prints success message
                nrows=len(pd.read_csv(file))
                print("{} records from {} successfully transferred to SFTP server".format(nrows, state))
                time.sleep(5)
            
                 
            except:
                print("Error transferring {} files to server".format(state))
            
            # END INDENT
            
            # Shouldn't be necessary to check for send_success again but just to be safe, reports are only marked as state reported if the send
            # was successful (the file was found to be located on the server).
            if send_success:
                try:
                    
                    reports_to_mark = pd.read_csv(step1path + state + now.strftime("_%Y_%m_%d") + ".csv")
                    marked_reports = mark_state_reported(reports_to_mark, sql_file)
                except:
                    print("Error marking state reported")
                 
    


                with open("send_log","r") as logfile:
                    log = logfile.read()




                
                
