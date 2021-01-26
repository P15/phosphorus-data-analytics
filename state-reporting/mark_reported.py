# -*- coding: utf-8 -*-
"""
Created on Sun Jan 24 21:42:19 2021

@author: Jacob-Windows
"""
import os
import sys
import pandas as pd
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
sys.path.append(__location__ + '/../')
from common import utils
from datetime import timedelta

# Currently converting this to be barcodes 


def mark_state_reported(df, sql_file):
    # For now the best system seems to be to use the date range based on the exported file. In the future this should be done using barcodes instead.
    
    state = df["Patient State"][0]


    startdate = min(pd.to_datetime(df["Result Date and Time"]))
    enddate = max(pd.to_datetime(df["Result Date and Time"]))+timedelta(seconds=60)
        
    with utils.get_db_connection(ignore_role=False,database='STAGING') as conn:
      with conn.cursor() as cur:
          with open(sql_file, 'r') as file:
              reports = file.read()
              reports = reports.format(startdate, enddate, state)
              cur.execute(reports)
              marked_reported=pd.DataFrame(data=cur.fetchall(),
                              columns=[desc.name for desc in cur.description])
             
    if len(marked_reported)>0:
        print("{} reports from {} marked state reported".format(len(marked_reported), state))
    
    else:
        print("No reports from {}".format(state))

    return marked_reported

