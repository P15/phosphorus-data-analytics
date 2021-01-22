# -*- coding: utf-8 -*-
"""
Created on Tue Dec 29 23:21:22 2020

@author: Jacob-Windows
"""

import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

def scans2kits_from_db(startdate):
    print("getting scans from db...")
    engine=create_engine(os.environ["PROD_FOLLOWER_DATABASE_URL"])
    sql="""
    SELECT 
        e.id, 
        event_type, 
        CONCAT(du.first_name,' ',du.last_name) AS fullname, 
        1 AS scans_assigned_to_kits,
		e.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'EST' AS created_at,
		e.created_at-k.received_at AS scan_time_after_kit_received
    
    FROM event_logs e 
        JOIN distributor_users du ON du.user_id = e.user_id
        JOIN kits k ON k.id = e.kit_id
		
    WHERE 
        e.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'EST' > '{}' 
        AND event_type = 'scan_assigned_to_kit'
        AND k.received_at IS NOT NULL
    """.format(startdate)    
    
    df=pd.read_sql_query(sql, engine)
    df["day"]=[x.strftime("%m/%d/%Y") for x in df.created_at]
    return df

def scans2kits_aggregations(startdate):
    scans2kits=scans2kits_from_db(startdate)
    
    if len(scans2kits)==0:
        test=scans2kits.groupby(["day","fullname"]).sum()
        
    else:
        scans2kits_by_user=scans2kits.groupby(["day","fullname"]).sum()["scans_assigned_to_kits"]
        scans2kits_by_day=scans2kits.groupby(["day"]).sum()["scans_assigned_to_kits"]

        # Total seconds elapsed
        scans2kits["scan_time_after_kit_received"]=scans2kits["scan_time_after_kit_received"].values.astype(np.int64)
    
        average_scans2kits_time_of_day=scans2kits.groupby(["day"]).mean()["scan_time_after_kit_received"]
    
        average_scans2kits_time_of_day=pd.to_timedelta(average_scans2kits_time_of_day)
    return average_scans2kits_time_of_day, scans2kits_by_day, scans2kits_by_user
