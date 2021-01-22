# -*- coding: utf-8 -*-
"""
Created on Sun Jan 17 11:09:34 2021

@author: Jacob-Windows
"""

import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from common import myutils


def missing_info_tickets(startdate,enddate):
    print("getting tickets from db...")
    engine=create_engine(os.environ["PROD_FOLLOWER_DATABASE_URL"])
    sql="""
    select count(t.id),
    CONCAT(du.first_name,' ',du.last_name) AS fullname,
    date_trunc('day',t.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'EST') as day
    
    from tickets t
    join distributor_users du on du.user_id = t.user_id
    where t.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'EST' >= '{}'
                 AND t.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'EST' <= '{}'
                 AND t.category IN ('Critical Missing Information',
                                    'Missing Information',
                                    'Redraw',
                                    'Other Error',
                                    'Medical Records')
    group by fullname, date_trunc('day',t.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'EST')
    order by date_trunc('day',t.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'EST')
    """.format(startdate,enddate)
    
    df=pd.read_sql_query(sql, engine)
    df["day"]=[x.strftime("%m/%d/%Y") for x in df.day]
    return df


if __name__=="__main__":
    today = datetime.now().date()
    today = datetime(2021,1,12)
    enddate = today + timedelta(days=7-today.weekday())
    startdate = today - timedelta(days=today.weekday())
    daily = pd.DataFrame(index=[(startdate + timedelta(days=x)).strftime('%m/%d/%Y') for x in range((enddate-startdate).days)])
    
    test=daily.merge(missing_info_tickets(startdate,enddate), left_index=True, right_on="day", how="left")
    
    tickets=test.pivot(index="fullname",columns="day",values="count")
    tickets=tickets.fillna(0)
    thisweekreports=["Accessioning - Weekly Shift Report - {}".format(startdate.strftime("%Y%m%d")),
                     "Data Entry - Weekly Shift Report - {}".format(startdate.strftime("%Y%m%d"))]
    
    for report in thisweekreports:
        myutils.pd2gs(report, "Missing Info Tickets", tickets, include_index=True)
