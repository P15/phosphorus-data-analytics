# -*- coding: utf-8 -*-
"""
Created on Tue Dec 29 23:21:22 2020

@author: Jacob-Windows
"""

import os
import pandas as pd
from datetime import datetime,timedelta
from statistics import mean
import numpy as np
from sqlalchemy import create_engine
from common import utils

def reports_from_db():
    print("getting reports from db...")
    engine = create_engine(os.environ["PROD_FOLLOWER_DATABASE_URL"])
    start_date=datetime(2020,12,27) #Plan to dynamically define start_date in the future
    
    # Creates a pandas dataframe of all reports and events.
    # Includes start hour and created hour as truncated dates.
    # Includes strings of dates that may be used in google sheets as filters or axis labels
    
    sql="""
    SELECT e.*, u.username, to_char(e.created_at, 'MM/DD/YY') AS event_day, 1 AS scans
    
    FROM event_logs e
        JOIN users u ON u.id = e.user_id
    
    WHERE e.created_at > '{}' 
    AND e.event_type = 'scan_assigned_to_kit'
    
    ORDER BY
        e.created_at
    """.format(start_date)
    
    df=pd.read_sql_query(sql,engine)
    df=utils.UTC2EST(df)
    return df

df=reports_from_db()
scans_by_user=df.groupby(["event_day","username"]).sum()['scans']
scans_by_day=df.groupby(["event_day"]).sum()['scans']

