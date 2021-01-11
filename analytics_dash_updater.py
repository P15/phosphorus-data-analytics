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
from common.myutils import *
from sqlalchemy import create_engine
import re

#Notes: need to fix setting with copy warning, but still runs

def reports_from_db():
    print("getting reports from db...")
    engine = create_engine(os.environ["PROD_FOLLOWER_DATABASE_URL"])
    startdate=datetime(2020,12,15) #Plan to dynamically define start_date in the future
    
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
             s.received_at AS sample_received_at,
             s.collection_date
             --u.username
             
             
         FROM reports r
            JOIN clinics c ON r.clinic_id = c.id
            JOIN distributors d ON c.distributor_id = d.id
            JOIN report_types t ON t.id = r.report_type_id
            JOIN kits k ON r.kit_id = k.id
            JOIN event_logs e ON r.id = e.report_id
            JOIN samples s ON s.kit_id=k.id
            --JOIN users u ON u.id=e.user_id
          
         WHERE
             r.start_date AT TIME ZONE 'UTC' AT TIME ZONE 'EST'>= '{}'
             AND (r.start_date) IS NOT NULL
             AND r.cached_distributor_id = 15


         ;""".format(startdate)
    df=pd.read_sql_query(sql,engine)
    df=UTC2EST(df)
    # Stupid quick fix for another problem. Need to fix this.
    df["sent_day"] = [x.strftime("%m/%d/%Y") if x is not pd.NaT else pd.NaT for x in df.sent_date]
    df["start_day"] = [x.strftime("%m/%d/%Y") if x is not pd.NaT else pd.NaT for x in df.start_date]
    df["sent_time"] = [x.strftime("%H:%M:%S") if x is not pd.NaT else pd.NaT for x in df.sent_date]
    df["start_time"] = [x.strftime("%H:%M:%S") if x is not pd.NaT else pd.NaT for x in df.start_date]
    df["receive_day"] = [x.strftime("%m/%d/%Y") if x is not pd.NaT else pd.NaT for x in df.sample_received_at]
    df["event_day"] = [x.strftime("%m/%d/%Y") if x is not pd.NaT else pd.NaT for x in df.event_log_timestamp]
    df["start_hour"] = [x.hour if x is not pd.NaT else pd.NaT for x in df.start_date]
    df["create_hour"] = [x.hour if x is not pd.NaT else pd.NaT for x in df.created_at]
    df["timedelta"] = df.sent_date-df.sample_received_at
    return df


def TATdash(reports,genomics=False):  
    print("preparing data for TAT dash...")
    
    reports=reports.dropna(subset=["sent_day"])
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

    reports['weekday']=[x.day_name() for x in reports.start_date]
    reports['month']=[x.month_name() for x in reports.start_date]


    

    
    # Sort and eliminate
    reports=reports.sort_values(by=["start_day"])


    # Filters for only COVID tests and eliminates fringe cases, plus adds columns unique to COVID
    if genomics:
        reports=reports[~reports.test.str.contains("COVID")]
        reports=reports[reports.TAT<1500]
        reports['TAT'] = [x.days for x in reports.timedelta]
    else:
        reports['TAT'] = [round(x.total_seconds()/3600,2) for x in reports.timedelta]
        reports=reports[reports.test.str.contains("COVID")]
        reports=reports[(reports.TAT>6) & (reports.TAT<200)]
        reports["72 Hours"]=72
        reports['positive']=[int(x) for x in reports.result=="Positive"]
        #Creates a goal of 24 hours for STAT reports, else 72 hours
        reports.loc[reports.expedited,"goal"]=24
        reports["goal"]=reports.goal.fillna(72)

        
    # These are used to create horizontal lines in the google sheets chart
    reports["Average Turnaround"]=mean(reports["TAT"])
    
    
    # Fixes some problems with the tracking number. Need to go back and review this...
    reports=reports[reports.tracking_number!="N/A"]
    reports['tracking_number']=[re.sub('[^0-9]','',x) if x is not None else None for x in reports.tracking_number]
    reports['tracking_number']=reports.tracking_number.astype('float64').fillna(0).astype(np.int64).clip(lower=1000).replace({1000:None})
    
    
    # adds trackers
    if not genomics:
        #reports=add_trackers_to_reports(reports)
        #If TAT is less than the goal, goal_missed is zero, else 1
        reports.loc[reports.TAT<reports.goal,"goal_missed"]=0
        reports["goal_missed"]=reports.goal_missed.fillna(1)

    
    new_order=['id',
               'delivery_datetime',
               "collection_date",
               "sample_received_at",
               'receive_day',
               'sent_day',
               'start_time',
               'sent_time',
               'weekday',
               'TAT',
               'goal',
               'goal_missed',
               'expedited',
               'result',
               'clinic',
               'distributor',
               'redraw',
               'timedelta',
               'test',
               'tracking_number',
               'start_date_type',
               'positive',
               'month',
               'public_url',
               'picked_up_city',
               'picked_up_state']
    
    reports=reports.loc[:,reports.columns.isin(new_order)]
    
    # Uploads to google sheets
    if genomics:
        pd2gs("Genomics TAT","Data",reports)
    else:
        pd2gs("COVID-19 - All Distributors TAT Dashboard","Data",reports)
        IRMSreports=reports[reports.clinic=="IRMS"]
        #pd2gs("IRMS COVID-19 TAT Dashboard","Data",IRMSreports)
        albertsons=reports[reports.distributor=="Albertsons"]
        #pd2gs("Albertsons TAT Dashboard","Data",albertsons)
        readyhealth=reports[reports.distributor=="Ready Health"]
        #pd2gs("ReadyHealth TAT Dashboard","Data",readyhealth)
        westside=reports[reports.clinic.str.contains("Westside")]
        pd2gs("WestSide Projected Refund","Data",westside)
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
        create_ep_trackers_list(trackers_to_create, "fedex")
        
        try:
            trackers = get_easypost_trackers(trackers_to_create)
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
    
    
    # Adds a column of 1s, and sorts
    reports["occurances"]=int(1)
    reports=reports.sort_values(by=["id","event_log_timestamp"])
    
    
    # Filters for only COVID tests
    reports=reports[reports.test.str.contains("COVID")]

    # Creates a column for the received event which will come in handy later
    received=reports.drop_duplicates(subset=["id"],keep="last")
    received=received.groupby(["receive_day"])[["occurances"]].sum().rename(columns={"occurances":"Received"})
    
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
    reports=reports.drop_duplicates(subset=["id","new_event_type"]).sort_values(by=["id","event_log_timestamp"])
    
    #########################################################################################################################
    ###Not sure if the best way to present this data is by start date of report or by the day the event occurs. I do both.###
    #########################################################################################################################
    
    # Groups events by start day, event day and pivots so that each event is a column and days are rows. This allows the right kind of bar chart to be created in google sheets.
    startevents=reports.groupby(["start_day",pd.Grouper(key="new_event_type")])[["occurances"]].sum().reset_index().pivot("start_day","new_event_type","occurances")
    events=reports.groupby(["event_day",pd.Grouper(key="new_event_type")])[["occurances"]].sum().reset_index().pivot("event_day","new_event_type","occurances")
    
    # Merges both with the recievd column
    startevents=startevents.merge(received,left_index=True,right_on="receive_day").reset_index()
    events=events.merge(received,left_index=True,right_on="receive_day").reset_index()
    
    # Pushes to google sheets
    pd2gs("Events Report","Start Date",startevents)
    pd2gs("Events Report","Event Date",events)

def time_in_stage(reports):
        reports["Time in Stage"]=np.nan
        for i in range(reports.shape[0] - 1):
            reports['Time in Stage'][i] = reports['event_log_timestamp'][i+1] - reports['event_log_timestamp'][i]
    
    
    
    
    
    
    
    
    
    
    
    
    
def positives(reports):
    # Updates positives dashboards
    new_order=['id',
               'receive_day',
               'sent_day',
               'expedited',
               'result',
               'redraw',
               'test',
               'weekday',
               'distributor',
               'positive',
               'clinic']
    positives_reports=reports[new_order]
    
    pd2gs("All Distributors COVID Positives Dashboard","Data",positives_reports)
    return positives_reports

if __name__=="__main__":
    try:
        # Create dataframe
        rawreports=reports_from_db()
        
        # Updates events Report and doesn't return anything. REQUIRES rawreports
        events_by_day(rawreports)
        
        # Updates TAT dashboard for ALL using rawreports
        tatreports=TATdash(rawreports)
        
        #genomics=TATdash(rawreports,genomics=True)

        # Updates positives reports for and ALL. REQUIRES tatreports
        positives_reports=positives(tatreports)
        print("Succeeded at {}".format(datetime.now()))
        input("Press enter to quit...")
        
    except Exception as e:
        print("Failed at {}".format(datetime.now()))
        print("With error: {}".format(e))
        input("Press enter to quit...")


    