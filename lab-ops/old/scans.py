# -*- coding: utf-8 -*-
"""
Created on Tue Dec 29 22:56:25 2020

@author: Jacob-Windows
"""
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine


def scans_from_db(startdate):
    print("getting scans from db...")
    engine=create_engine(os.environ["PROD_FOLLOWER_DATABASE_URL"])
    sql_get_scans="""
         SELECT 
         s.id, page, 1 AS pages_scanned,
         CONCAT(du.first_name,' ',du.last_name) AS fullname,
         s.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'EST' AS created_at
         
         FROM scans s
             JOIN distributor_users du on du.id = s.distributor_user_id
             
         WHERE
             s.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'EST' >'{}'     
             AND s.file_name IS NOT NULL
         ;""".format(startdate)    
    df=pd.read_sql_query(sql_get_scans, engine)
    df["day"]=[x.strftime("%m/%d/%Y") for x in df.created_at]
    return df

def scans_aggregations(startdate):
    scans=scans_from_db(startdate)
    scans_by_user=scans.groupby(["day","fullname"]).sum()["pages_scanned"]
    scans_by_day=scans.groupby(["day"]).sum()["pages_scanned"]

    #   time of day
    scans["avg_time_of_scan"]=scans["created_at"]-pd.to_datetime(scans["day"])

    scans["avg_time_of_scan"]=scans["avg_time_of_scan"].values.astype(np.int64)

    average_scans_time_of_day=scans.groupby(["day"]).mean()["avg_time_of_scan"]

    average_scans_time_of_day=pd.to_timedelta(average_scans_time_of_day)
    return average_scans_time_of_day, scans_by_day, scans_by_user


