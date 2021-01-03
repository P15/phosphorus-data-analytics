# -*- coding: utf-8 -*-
"""
Created on Tue Dec 29 19:50:10 2020

@author: Jacob-Windows
"""
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine



def tickets_from_db(startdate):
    print("getting tickets from db...")
    engine=create_engine(os.environ["PROD_FOLLOWER_DATABASE_URL"])
    sql_get_tickets="""
         SELECT 
         t.id,
         t.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'EST' AS created_at,
         CONCAT(du.first_name,' ',du.last_name) AS fullname,
         1 AS "tickets_created"
         
         FROM tickets t
             JOIN distributor_users du ON du.user_id = t.user_id
             
         WHERE
             t.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'EST'> '{}'
             AND t.category IN ('Critical Missing Information',
                                'Missing Information',
                                'Redraw',
                                'Other Error',
                                'Medical Records')
             
         ;""".format(startdate)
    df=pd.read_sql_query(sql_get_tickets, engine)
    df["day"]=[x.strftime("%m/%d/%Y") for x in df.created_at]
    return df
    
def tickets_aggregations(startdate):
    tickets=tickets_from_db(startdate)

    tickets_by_user=tickets.groupby(["day","fullname"]).sum()["tickets_created"]
    tickets_by_day=tickets.groupby(["day"]).sum()["tickets_created"]
    
    #time of day
    tickets["avg_tickets_time"]=tickets["created_at"]-pd.to_datetime(tickets["day"])
    
    tickets["avg_tickets_time"]=tickets["avg_tickets_time"].values.astype(np.int64)
    
    average_tickets_time_of_day=tickets.groupby(["day"]).mean()["avg_tickets_time"]
    
    average_tickets_time_of_day=pd.to_timedelta(average_tickets_time_of_day)
    return average_tickets_time_of_day, tickets_by_day, tickets_by_user






