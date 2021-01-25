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
from common.utils import liststringsearch


if __name__=="__main__":
        
    sftpcreds=pd.read_csv("C:/Users/jacob-windows/documents/Phosphorus/Python/state-reporting/SFTPcreds.csv", index_col=0)
    
    
    this_file = os.path.abspath("C:/Users/Jacob-Windows/Documents/Phosphorus/phosphorus-data-analytics/state-reporting/mark-state-reported.py")
    #this_file = os.path.abspath(__file__)
    this_dir = os.path.dirname(this_file)
    sql_file = os.path.join(this_dir, 'mark_state_reported.sql')
    
    
    day=datetime.now()
    # If I need to send files from a different day, I use the line below. This can be controlled with an arg in the future.
    day=datetime(2021,1,22)
    folderpath="C:/Users/jacob/Google Drive/State Reporting/January/{}".format(day.strftime("%Y_%m_%d"))
    inpath=folderpath+"/Step 3 Ready to Send"
    
    record = pd.DataFrame()
    paths = glob.glob(inpath+"/*")
    folder   = [os.path.basename(x) for x in paths]
    filename = [name.split(".")[0] for name in folder]  
    states    = [file.split("_")[0] for file in filename]    
    
    # Reads the file ~/.ssh/known_hosts
    # Only hosts with a fingerprint documented in the known_hosts file are trusted
    cnopts = pysftp.CnOpts()
    
    
    # I have tested this for each individual state. I have followed up with email to confirm that the file was received with no issues.
    # Nevertheless, it is set up with state = 'test' to prevent you from accidentally running the script.
    # You can choose to set up a local SFTP server to run the test using this: https://www.solarwinds.com/free-tools/free-sftp-server
    
    state='test'
    for state in states:
        filetosend = liststringsearch(paths, state)
        if state in sftpcreds.index:
            print(state)
            creds=sftpcreds.loc[state]
            print(filetosend + " will be sent to " + creds.hostname)
            
            if state == "OH":
                raise Exception("Ohio needs to be renamed")
            if state == "CA":
                raise Exception("California needs to be renamed")
            
    
            
            if creds["prod"]:
                remotedir = creds.proddir
            else:
                remotedir = creds.testdir
            
            if state == 'test': # This line will be deleted
            # Currently prevented from running outside of the test case from here until end of file. Needs to be unindented.
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
                        srv.put(filetosend)
                        send_success = any([remotefilename in filetosend for remotefilename in srv.listdir()])
                     
                   # If something prevents the file from being sent it will probably raise an error prior to this point. This is a final check.
                   if not send_success:
                        raise Exception("Send not successful")
                   
                   # Prints success message
                   nrows=len(pd.read_csv(filetosend))
                   print("{} records from {} successfully transferred to SFTP server".format(nrows, state))
                    
            
                    
               except:
                   print("Error transferring {} files to server".format(state))
            
            # Shouldn't be necessary to check for send_success again but just to be safe, reports are only marked as state reported if the send
            # was successful (the file was found to be located on the server).
            if send_success:
                try:
                    
                    reports_to_mark = pd.read_csv(filetosend)
                    marked_reports = mark_state_reported(reports_to_mark)
                    nrows =  len(marked_reports)
                    print("{} reports from {} Marked state reported".format(nrows,state))
                except:
                    print("Error marking state reported")
                 
    









                
                