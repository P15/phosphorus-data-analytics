# -*- coding: utf-8 -*-
"""
Created on Tue Dec 29 19:50:10 2020

@author: Jacob-Windows
"""
import os
import pandas as pd
from datetime import datetime,timedelta
from statistics import mean
import numpy as np
from sqlalchemy import create_engine
from common import utils

def tickets_from_db():
    print("getting tickets from db")
    engine=create_engine(os.environ["PROD_FOLLOWER_DATABASE_URL"])
    sql_get_tickets="""
         SELECT 
         t.*,
         c.name AS clinic,
         d.name AS distributor,
         u.username
         
         FROM tickets t
             JOIN clinics c on t.clinic_id = c.id
             JOIN distributors d on c.distributor_id = d.id
             JOIN users u on u.id = t.user_id
         WHERE
             t.created_at> cast('2020-12-20' as date)
             --AND t.category IN ('Consent Signature', 'Critical Missing Information', 'Error', 'Missing Information', 'Other Error', 'Practitioner', 'Redraw')
             
         ;"""    
    df=pd.read_sql_query(sql_get_tickets, engine)
    return df

df=tickets_from_db()

df=utils.UTC2EST(df)

df["create_day"]=[x.strftime("%m/%d/%Y") for x in df.created_at]


list(df.category.drop_duplicates())
keepcats= ['Critical Missing Information',
           'Missing Information',
           'Redraw',
           'Other Error',
           'Medical Records']

alltickets=len(df)
df=df[df.category.isin(keepcats)]

len(df)/alltickets

df["tickets"]=1
tickets_by_user=df.groupby(["create_day","username"]).sum()["tickets"]
tickets_by_day=df.groupby(["create_day"]).sum()["tickets"]

#time of day
df["time"]=df["created_at"]-pd.to_datetime(df["create_day"])

df["time"]=df["time"].values.astype(np.int64)

tickets_average_time_of_day=df.groupby(["create_day"]).mean()["time"]

tickets_average_time_of_day=pd.to_timedelta(tickets_average_time_of_day)


mean(df["time"])






