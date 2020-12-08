# -*- coding: utf-8 -*-
"""
Created on Wed Sep 30 11:57:35 2020

@author: Jacob-Windows
"""

import os
import pandas as pd
from datetime import datetime,timedelta
from statistics import mean
import numpy as np
from common import utils
from sqlalchemy import create_engine
import re

#Notes: need to fix setting with copy warning, but still runs

def reports_from_db():
    print("getting reports from db...")
    engine = create_engine(os.environ["PROD_FOLLOWER_DATABASE_URL"])
    start_date=datetime(2020,11,1) #Plan to dynamically define start_date in the future
    
    # Creates a pandas dataframe of all reports and events.
    # Includes start hour and created hour as truncated dates.
    # Includes strings of dates that may be used in google sheets as filters or axis labels
    
    sql="""
         SELECT 
             r.*,
             c.name AS clinic,
             d.name AS distributor,
             t.name AS test,
             k.tracking_number AS tracking_number,
             e.created_at AS event_log_timestamp, 
             e.event_type,
             s.received_at AS sample_recieved_at,
             date_trunc('hour',r.created_at) AS create_hour,
             date_trunc('hour',r.start_date) AS start_hour,
             r.sent_date-r.start_date AS timedelta,
             u.username,
             to_char(r.sent_date, 'MM/DD/YYY') AS sent_day,
             to_char(r.start_date, 'MM/DD/YYY') AS start_day,
             to_char(s.received_at, 'MM/DD/YYY') AS recieve_day,
             to_char(e.created_at, 'MM/DD/YYY') AS event_day
             
             
         FROM reports r
            JOIN clinics c ON r.clinic_id = c.id
            JOIN distributors d ON c.distributor_id = d.id
            JOIN report_types t ON t.id = r.report_type_id
            JOIN kits k ON r.kit_id = k.id
            JOIN event_logs e ON r.id = e.report_id
            JOIN samples s ON s.kit_id=k.id
            JOIN users u ON u.id=e.user_id
          
         WHERE
             r.start_date>= '{}'
             AND (r.sent_date,r.start_date, r.result) IS NOT NULL


         ;""".format(start_date)
    df=pd.read_sql_query(sql,engine)
    return df


def TATdash(reports):  
    print("preparing data for TAT dash...")
    
    # Filters for only COVID tests
    reports=reports[reports.test.str.contains("COVID")]
    
    # redraw=TRUE if a report ever experienced the event "report_marked_redraw"
    redrawn_reports=reports[reports.event_type=="report_marked_redraw"]
    redrawn_report_ids=set(reports.id).intersection(set(redrawn_reports.id))
    reports["redraw"]=reports.id.isin(redrawn_report_ids)
    
    # Keeps only the last event per report. Keep last or first doesn't matter
    reports=reports.drop_duplicates(subset=["id"],keep="last")
    
    # start_date_type: sample_created if a patient was pre-registered, else report_created
    reports.loc[reports.create_hour==reports.start_hour, "start_date_type"]="report_created"
    reports["start_date_type"]=reports.start_date_type.fillna("sample_created")
    

    # Creates the columns necessary for the TAT dashboard. May move more of these into SQL query in the future.
    reports['TAT'] = [round(x.total_seconds()/3600,2) for x in reports.timedelta]
    reports['start_day']=[datetime.strftime(x, "%m/%d/%Y") for x in reports.start_date]
    reports['send_day']=[datetime.strftime(x, "%m/%d/%Y") for x in reports.sent_date]
    reports['recieve_day']=[datetime.strftime(x, "%m/%d/%Y") for x in reports.sample_recieved_at]
    reports['weekday']=[x.day_name() for x in reports.start_date]
    reports['positive']=[int(x) for x in reports.result=="Positive"]
    reports['month']=[x.month_name() for x in reports.start_date]
    
    # These are used to create horizontal lines in the google sheets chart
    reports["Average Turnaround"]=mean(reports["TAT"])
    reports["72 Hours"]=72
    

    # Sort and eliminate
    reports=reports.sort_values(by=["start_day"])

    # Eliminate fringe cases
    reports=reports[(reports.TAT>6) & (reports.TAT<200)]
    
    # Fixes some problems with the tracking number. Need to go back and review this...
    reports=reports[reports.tracking_number!="N/A"]
    reports['tracking_number']=[re.sub('[^0-9]','',x) if x is not None else None for x in reports.tracking_number]
    reports['tracking_number']=reports.tracking_number.astype('float64').fillna(0).astype(np.int64).clip(lower=1000).replace({1000:None})
    
    
    new_order=['id',
               'start_date',
               'sent_date',
               'status',
               'is_cancelled',
               'expedited',
               'result',
               'clinic',
               'distributor',
               'redraw',
               'timedelta',
               'TAT',
               'start_day',
               'send_day',
               'Average Turnaround',
               '72 Hours',
               'test',
               'tracking_number',
               'start_date_type',
               'weekday',
               'positive',
               'month']
    
    reports=reports[new_order]
    
    # adds trackers
    reports=add_trackers_to_reports(reports)
    
    # Uploads to google sheets
    utils.pd2gs("COVID-19 - All Distributors TAT Dashboard","Data",reports)
    
    # Upload albertsons reports to albertsons-specific document
    albertsonsreports=reports[reports.distributor=="Albertsons"]
    utils.pd2gs("Albertsons COVID TAT Dashboard","Data",albertsonsreports)
    
    return reports

def add_trackers_to_reports(reports):
    # Uses my local pg database to get trackers that have already been seen, and therefore already had any easypost tracker object created
    engine = create_engine(os.environ["LOCAL_DB_URL"])
    old_trackers=pd.read_sql_query("select * from easypost_trackers",engine)

    seen_codes=set(reports.tracking_number).intersection(set(old_trackers.tracking_number)) 
    
    trackers_to_create=reports.tracking_number[~reports.tracking_number.isin(seen_codes)].dropna()
    
    # If there are new tracking numbers, creates easypost tracking objects. Else will simply use the trackers that already exist.
    if len(trackers_to_create)>0:
        print("adding tracking info to reports, including {} new trackers...".format(len(trackers_to_create)))
        utils.create_ep_trackers_list(trackers_to_create, "fedex")
        
        try:
            trackers = utils.get_easypost_trackers(trackers_to_create)
            updated_trackers=old_trackers.append(trackers)
            
            # Writes SQL table
            updated_trackers.to_sql("easypost_trackers",engine,if_exists='replace',index=False)
            
        except:
            print("error getting new trackers, will use old ones")
            updated_trackers=old_trackers.copy()
            
    else:
        print("no new trackers")
        updated_trackers=old_trackers.copy()
      
    updated_trackers["delivery_datetime"]=pd.to_datetime(updated_trackers.delivery_datetime)
    updated_trackers=updated_trackers.dropna(subset=["easypost_id"])
    
    # Merges trackers with reports
    reports=reports.merge(updated_trackers,on="tracking_number",how='outer').dropna(subset=["id"])
      
    # If delivery_datetime is present, a new timedelta is calculated using this as the start. Otherwise the old TAT stands.
    reports['timedelta']=(reports.sent_date-reports.delivery_datetime).fillna(reports.sent_date-reports.start_date)
    
    # Eliminates some rare cases where timedelta is negative. Need to investigate this.
    reports.loc[reports['timedelta']<timedelta(0), 'timedelta'] = reports.sent_date-reports.start_date

    reports['TAT'] = [round(x.total_seconds()/3600,2) for x in reports.timedelta]
    reports["Average Turnaround"]=mean(reports["TAT"])    
    reports=reports[(reports.TAT>6) & (reports.TAT<200)]
    
    # If delivery_datetime is now the TAT start time, start_date_type will reflect this.
    reports.loc[reports.delivery_datetime.notnull(), "start_date_type"]="delivery_date"
    return reports

def events_by_day(reports):  
    print("doing events by day...")
    
    # Clips dates
    earliest_date=datetime(2020,10,1)
    latest_date=datetime.now()
    idx = np.where((reports.event_log_timestamp>earliest_date)&(reports.event_log_timestamp<=latest_date))
    reports=reports.iloc[idx]
    
    
    # Adds a column of 1s, and sorts
    reports["occurances"]=int(1)
    reports=reports.sort_values(by=["id","event_log_timestamp"])
    
    # Aggregates events by user
    reports["user"]=reports.username.str.split("@").str[0]
    userevents=reports.groupby(["user","event_day","event_type"])["occurances"].sum().reset_index()
    userevents_day=reports.groupby(["user","event_day"])["occurances"].sum().reset_index().pivot("event_day","user","occurances")
    
    # Filters for only COVID tests
    reports=reports[reports.test.str.contains("COVID")]

    # Creates a column for the recieved event which will come in handy later
    recieved=reports.drop_duplicates(subset=["id"],keep="last")
    recieved=recieved.groupby(["recieve_day"])[["occurances"]].sum().rename(columns={"occurances":"Received"})
    
    # Events will be collected into major categories and renamed to look nicer on the chart. Everything not included will not appear in the report.
    # Key is a search term, value is what it will be replaced with 
    events_dict={"report_sent"      : "Sent",                                   #looking for report_sent & report_sent_to_patient
                 "review"           : "Review",                                 #looking for report_transitioned_to_awaiting_review
                 "_approved"        : "Approved",                               #looking for report_approved & report_electronically_signed_and_approved
                 "linked"           : "Accessioned",                            #looking for first_sample_data_source_linked_to_report
                 "result_updated"   : "Result Updated",                         #looking for report_result_updated
                 "generated"        : "Report Generated",                       #looking for report_generated
                 "report_requested" : "Report Requested",                       #looking for report_requested
                 "cancelled"        : "Cancelled",                              #looking for looking for sample_data_source_cancelled_with_report & report_cancelled
                 "rolled"           : "Rolled Back to Awaiting Results"}        #looking for report_rolled_back_to_awaiting_results
    
    

    for search_term , replacement in events_dict.items():
        reports.loc[reports.event_type.str.contains(search_term),"new_event_type"]=replacement

    
    #Eliminates the remaining event types
    reports=reports.dropna(subset=["new_event_type"])
    
    #Some reports, by the above definitions, will have been sent twice or maybe cancelled twice. Here the duplicates are removed.
    reports=reports.drop_duplicates(subset=["id","event_type"]).sort_values(by=["id","event_log_timestamp"])
    
    #########################################################################################################################
    ###Not sure if the best way to present this data is by start date of report or by the day the event occurs. I do both.###
    #########################################################################################################################
    
    # Groups events by start day, event day and pivots so that each event is a column and days are rows. This allows the right kind of bar chart to be created in google sheets.
    startevents=reports.groupby(["start_day",pd.Grouper(key="event_type")])[["occurances"]].sum().reset_index().pivot("start_day","event_type","occurances")
    events=reports.groupby(["event_day",pd.Grouper(key="event_type")])[["occurances"]].sum().reset_index().pivot("event_day","event_type","occurances")
    
    # Merges both with the recievd column
    startevents=startevents.merge(recieved,left_index=True,right_on="recieve_day").reset_index()
    events=events.merge(recieved,left_index=True,right_on="recieve_day").reset_index()
    
    # Pushes to google sheets
    utils.pd2gs("Events Report","Start Date",startevents)
    utils.pd2gs("Events Report","Event Date",events)
    utils.pd2gs("Events Report","Events by User",userevents,clear=False)
    utils.pd2gs("Events Report","Users",userevents_day)
    
def positives(reports):
    # Updates positives dashboards
    new_order=['id',
               'start_day',
               'send_day',
               'status',
               'is_cancelled',
               'expedited',
               'result',
               'redraw',
               '72 Hours',
               'test',
               'weekday',
               'distributor',
               'positive',
               'clinic']
    positives_reports=reports[new_order]
    
    utils.pd2gs("All Distributors COVID Positives Dashboard","Data",positives_reports)
    return positives_reports

if __name__=="__main__":
    try:
        # Create dataframe
        rawreports=reports_from_db()
        
        # Updates events Report and doesn't return anything. REQUIRES rawreports
        events_by_day(rawreports)
        
        # Updates TAT dashboard for ALL and for ALBERTSONS using RAWREPORTS
        tatreports=TATdash(rawreports)

        # Updates positives reports for and ALL. REQUIRES tatreports
        positives_reports=positives(tatreports)
        print("Succeeded at {}".format(datetime.now()))
        
    except Exception as e:
        print("Failed at {}".format(datetime.now()))
        print(e)
        input("Press enter to quit...")


    