# -*- coding: utf-8 -*-
"""
Created on Tue Dec 29 23:21:22 2020

@author: Jacob-Windows
"""

import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

def reports_from_db(startdate):
    print("getting reports from db...")
    engine = create_engine(os.environ["PROD_FOLLOWER_DATABASE_URL"])
    sql="""
         SELECT 
             r.id,
             e.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'EST' AS event_log_timestamp, 
             e.event_type,
             CONCAT(du.first_name,' ',du.last_name) AS fullname,
             1 AS samples_acccessioned
             
             
         FROM reports r
            JOIN event_logs e ON r.id = e.report_id
            LEFT JOIN distributor_users du ON du.user_id = e.user_id
            
         WHERE
             e.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'EST' >= '{}'
             AND (r.start_date) IS NOT NULL
         ORDER BY
             r.id, event_log_timestamp

         ;""".format(startdate)
    df=pd.read_sql_query(sql,engine)
    df["day"]=[x.strftime("%m/%d/%Y") for x in df.event_log_timestamp]
    return df

def accesssions_aggregations(startdate):
    df=reports_from_db(startdate)
    df['fullname']=df.fullname.shift(-1)
    df=df[df.event_type=="first_sample_data_source_linked_to_report"]
    df=df.dropna(subset=["fullname"])
    df=df.drop_duplicates(subset=['id'])

    accessions_by_user=df.groupby(["day","fullname"]).sum()["samples_acccessioned"]
    accessions_by_day=df.groupby("day").sum()["samples_acccessioned"]
    
    #time of day
    df["avg_time_accessioned"]=df["event_log_timestamp"]-pd.to_datetime(df["day"])
    
    df["avg_time_accessioned"]=df["avg_time_accessioned"].values.astype(np.int64)
    
    average_accessions_time_of_day=df.groupby(["day"]).mean()["avg_time_accessioned"]
    
    average_accessions_time_of_day=pd.to_timedelta(average_accessions_time_of_day)
    return average_accessions_time_of_day, accessions_by_day, accessions_by_user




#Justification for using the username of the next event is provided here
"""
userevents=df2[["id","event_type","event_log_timestamp","username","usernameshift"]].sort_values(by=['id','event_log_timestamp'])
userevents["event_log_timestamp"]=pd.to_datetime([x.strftime("%Y-%m-%d %H:%M") for x in userevents.event_log_timestamp])
userevents["next_timestamp"]=userevents.event_log_timestamp.shift(-1)
userevents["last_timestamp"]=userevents.event_log_timestamp.shift(1)
userevents["nextmatch"]=userevents.event_log_timestamp>=userevents.next_timestamp-timedelta(minutes=1)
userevents['lastmatch']=userevents.event_log_timestamp<=userevents.last_timestamp+timedelta(minutes=1)
first_sample=userevents[userevents.event_type=="first_sample_data_source_linked_to_report"]

sum(first_sample.nextmatch)==len(first_sample)

userevents=userevents[userevents.usernameshift.isna()]
examine=df2[df2.id==202248]
"""
