# -*- coding: utf-8 -*-
"""
Created on Wed Dec 30 13:50:24 2020

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
    SELECT e.*, u.username, 1 AS reviews_completed
    
    FROM event_logs e
        JOIN users u ON u.id = e.user_id
    
    WHERE e.created_at > '{}' 
    AND e.event_type = 'kit_data_review_complete'
    
    ORDER BY
        e.created_at
    """.format(start_date)
    
    df=pd.read_sql_query(sql,engine)
    df=utils.UTC2EST(df)
    return df

df=reports_from_db()


reviews_by_user=df.groupby(["event_day","username"]).sum()['reviews_completed']
reviews_by_day=df.groupby(["event_day"]).sum()['reviews_completed']

