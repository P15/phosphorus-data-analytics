# -*- coding: utf-8 -*-
"""
Created on Wed Dec 30 13:50:24 2020

@author: Jacob-Windows
"""

import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine


def reviews_from_db(startdate, enddate, event_type):
    print("getting scans from db...")
    engine=create_engine(os.environ["PROD_FOLLOWER_DATABASE_URL"])
    sql="""
        select count(e.id), event_type,
        CONCAT(du.first_name,' ',du.last_name) AS fullname, 
        e.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'EST' AS event_log_timestamp,
        e.created_at-k.received_at AS review_time_after_kit_received
        
        FROM event_logs e 
        	LEFT JOIN distributor_users du ON du.user_id = e.user_id
        	LEFT JOIN kits k ON k.id = e.kit_id
        	
        WHERE 
        	e.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'EST' > '{}'
        	AND e.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'EST' > '{}'
        	AND event_type = '{}'
        	AND k.received_at IS NOT NULL
        	
        GROUP BY 
        event_type, 
        fullname,
        event_log_timestamp,
        review_time_after_kit_received
    """.format(startdate, enddate, event_type)    
    df=pd.read_sql_query(sql, engine)
    df["day"]=[x.strftime("%m/%d/%Y") for x in df.created_at]
    return df

today = datetime.now().date()
enddate = today + timedelta(days=7-today.weekday())
startdate = today - timedelta(days=today.weekday())
daily = pd.DataFrame(index=[(startdate + timedelta(days=x)).strftime('%m/%d/%Y') for x in range((enddate-startdate).days + 1)])

event_types={"reviews": }

test=daily.merge(reviews_from_db(startdate,enddate,event_type), left_index=True, right_on="day")

tickets=test.pivot(index="username",columns="day",values="count")
thisweekreport="Accessioning - Weekly Shift Report - {}".format(startdate.strftime("%Y%m%d"))




def reviews_aggregations(startdate):
    reviews=reviews_from_db(startdate, enddate, event_type)
    reviews_by_user=reviews.groupby(["day","fullname"]).sum()["reviews"]
    reviews_by_day=reviews.groupby(["day"]).sum()["reviews"]

    # Total seconds elapsed
    reviews["review_time_after_kit_received"]=reviews["review_time_after_kit_received"].values.astype(np.int64)

    average_reviews_time_of_day=reviews.groupby(["day"]).mean()["review_time_after_kit_received"]

    average_reviews_time_of_day=pd.to_timedelta(average_reviews_time_of_day)
    return average_reviews_time_of_day, reviews_by_day, reviews_by_user