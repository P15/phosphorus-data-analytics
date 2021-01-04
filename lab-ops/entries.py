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


def entries_from_db(startdate):
    print("getting scans from db...")
    engine=create_engine(os.environ["PROD_FOLLOWER_DATABASE_URL"])
    sql="""
    SELECT 
        e.id, 
        event_type, 
        CONCAT(du.first_name,' ',du.last_name) AS fullname, 
        1 AS entries,
		e.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'EST' AS created_at,
		e.created_at-k.received_at AS entries_time_after_kit_received
    
    FROM event_logs e 
        JOIN distributor_users du ON du.user_id = e.user_id
        JOIN kits k ON k.id = e.kit_id
		
    WHERE 
        e.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'EST' > '{}' 
        AND event_type = 'kit_accessioning_complete'
        AND k.received_at IS NOT NULL
    """.format(startdate)    
    
    df=pd.read_sql_query(sql, engine)
    df["day"]=[x.strftime("%m/%d/%Y") for x in df.created_at]
    return df

def entries_aggregations(startdate):
    entries=entries_from_db(startdate)
    entries_by_user=entries.groupby(["day","fullname"]).sum()["entries"]
    entries_by_day=entries.groupby(["day"]).sum()["entries"]

    # Total seconds elapsed
    entries["entries_time_after_kit_received"]=entries["entries_time_after_kit_received"].values.astype(np.int64)

    average_entries_time_of_day=entries.groupby(["day"]).mean()["entries_time_after_kit_received"]

    average_entries_time_of_day=pd.to_timedelta(average_entries_time_of_day)
    return average_entries_time_of_day, entries_by_day, entries_by_user
