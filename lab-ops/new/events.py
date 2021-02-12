# -*- coding: utf-8 -*-
"""
Created on Wed Dec 30 13:50:24 2020

@author: Jacob-Windows
"""

import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from common import utils

def reviews_from_db(startdate, enddate, event_type_or_types):
    print("getting {} from db...".format(event_type_or_types))
    engine=create_engine(os.environ["FOLLOWER_DB_URL"])
    
    if type(event_type_or_types)==list:
        eventstring = "{}".format([x for x in event_type_or_types]) \
                        .replace("[","(") \
                        .replace("]",")")
    else:
        eventstring="('" + event_type_or_types + "')"
    
    
    sql="""
        select count(e.id), event_type,
        CONCAT(du.first_name,' ',du.last_name) AS fullname, 
        e.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'EST' as event_log_timestamp,
        e.created_at-k.received_at AS average_time_after_kit_received

        
        FROM event_logs e 
        	LEFT JOIN distributor_users du ON du.user_id = e.user_id
        	LEFT JOIN kits k ON k.id = e.kit_id
        	
        WHERE 
        	e.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'EST' >= '{}'
        	AND e.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'EST' < '{}'
        	AND event_type IN {}
        	--AND k.received_at IS NOT NULL
        	
        GROUP BY 
        event_type, 
        fullname,
        average_time_after_kit_received,
        event_log_timestamp
        
        order by
        event_log_timestamp
    """.format(startdate, enddate, eventstring)    
    df=pd.read_sql_query(sql, engine)
    df["day"]=[x.strftime("%m/%d/%Y") for x in df.event_log_timestamp]
    
    """
    try:
        df["avg_time_of_day_of_{}".format(event_type)]=df["event_log_timestamp"]-pd.to_datetime(df["day"])
        df["avg_time_of_day_of_{}".format(event_type)]=df["avg_time_of_day_of_{}".format(event_type)].values.astype(np.int64)

        df['average_time_after_kit_received']=df['average_time_after_kit_received'].values.astype(np.int64)
        df=df.groupby(["day","fullname","event_type"]).agg({'count':'sum', 'average_time_after_kit_received' : 'mean', 'avg_time_of_day_of_{}'.format(event_type) : 'mean'})
        df["avg_time_of_day_of_{}".format(event_type)]=pd.to_timedelta(df["avg_time_of_day_of_{}".format(event_type)])
        df['average_time_after_kit_received']=pd.to_timedelta(df['average_time_after_kit_received'])
    except:
        pass
    """
    
    return df


today = datetime.now().date()
#today = datetime(2021,1,17)
enddate = today + timedelta(days=7-today.weekday())
startdate = today - timedelta(days=today.weekday())
daily = pd.DataFrame(index=[(startdate + timedelta(days=x)).strftime('%m/%d/%Y') for x in range((enddate-startdate).days + 1)])

event_types = {"Reviews"     : "kit_data_review_complete",
               "Entries"     : "kit_accessioning_complete",
               "Scans Assigned to Kit"  : "scan_assigned_to_kit",
               "Accessioned" : "kit_received",
               "Sent"        : ["report_sent","report_sent_to_patient"],
               "Samples QC Approved" : "sample_qc_approved",
               "Reports Approved" : ["report_approved","report_electronically_signed_and_approved"],
               "Ticket Created" : "ticket_created",
               "Ticket Event Recorded" : "ticket_event_recorded",
               "Ticket Closed" : "ticket_closed",
               "Curation Assignments Completed" : "curation_assignment_completed",
               "Plates Created" : "plate_created",
               "Curation Created" : "curation_filter_created",
               "Scans Created" : "scan_created"}

Accessioning_Events = ["Accessioned",
                       "Scans Created"]

Data_Entry_Events = ["Reviews",
                     "Entries",
                     "Scans Assigned to Kit"]

accessioningreport = "Accessioning - Weekly Shift Report - {}".format(startdate.strftime("%Y%m%d"))

dataentryreport = "Data Entry - Weekly Shift Report - {}".format(startdate.strftime("%Y%m%d"))

for event, event_type in event_types.items():
    events_by_user=daily.merge(reviews_from_db(startdate,enddate,event_type), left_index=True, right_on="day").reset_index()
    if len(events_by_user)>0:
        events_by_user=events_by_user.groupby(["day","fullname"]).sum()['count'].reset_index()
        events_by_user=events_by_user.pivot(index="fullname",columns="day",values="count")
        events_by_user=events_by_user.fillna(0)
        #utils.pd2gs("Events by User",event,events_by_user,include_index=True)
        
        if event in Accessioning_Events:
            utils.pd2gs(accessioningreport,event,events_by_user,include_index=True)
        if event in Data_Entry_Events:
            utils.pd2gs(dataentryreport,event,events_by_user,include_index=True)
    else:
        print("No {} events for this date range".format(event))
    
    
thisweekreport="Accessioning - Weekly Shift Report - {}".format(startdate.strftime("%Y%m%d"))



test=["hello","world"]


