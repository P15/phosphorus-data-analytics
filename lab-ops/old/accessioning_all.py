# -*- coding: utf-8 -*-
"""
Created on Tue Dec 29 22:56:25 2020

@author: Jacob-Windows


Description:
    
This script updates the Weekly Shift Reports located in GoogleDrive/Laboratory Operations/Management and Reporting/1. High-Level Oversight/Weekly Shift Reports
at https://drive.google.com/drive/folders/1BHknNiOcpuTBLugkTfLbz8zrA32KZBDx?usp=sharing

It requires three other scripts: accessions, scans, & tickets

The end result is two Pandas DataFrames, one of all activity per day, and the other of activity per user. These DataFrames are pushed to the spreadsheet.

"""
import os
import sys
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
sys.path.append(__location__ + '/../')
from common import myutils
import pandas as pd
from datetime import datetime,timedelta
import accessions
import scans
import tickets


def main(today):
      # Defines a DataFrame index with every day in the week so that days with zero activity are not missed
    enddate = today + timedelta(days = 6 - today.weekday())
    startdate = today - timedelta(days=today.weekday())
    daily = pd.DataFrame(index=[(startdate + timedelta(days=x)).strftime('%m/%d/%Y') for x in range((enddate-startdate).days + 1)])
       
    
    # Calls aggregation functions from subscripts, which return three DataFrames in a specific order each.
    average_accessions_time_of_day, accessions_by_day, accessions_by_user = accessions.accesssions_aggregations(startdate)
    
    average_tickets_time_of_day, tickets_by_day, tickets_by_user = tickets.tickets_aggregations(startdate)
    
    average_scans_time_of_day, scans_by_day, scans_by_user = scans.scans_aggregations(startdate)
    
    
    # Lists user-level DataFrames 
    user_dataframes=[scans_by_user,
                     accessions_by_user,
                     tickets_by_user]
    
    
    # Lists unique full names
    allnames = []
    for df in user_dataframes:
        allnames.extend([x for x in df.index.get_level_values('fullname')])
    allnames = pd.DataFrame({"name":allnames}).drop_duplicates()
    
    # Defines a DataFrame with every day/name combination   
    timeofday_for_names=pd.concat([daily]*len(allnames)).reset_index().rename(columns={"index":"day"})
    allnames=allnames.loc[allnames.index.repeat(len(daily))].reset_index(drop=True)
    users_and_days = pd.DataFrame({"day":timeofday_for_names.day, "name":allnames.name})
    
    # Lists day-level DataFrames
    day_dataframes=[scans_by_day,
                    average_scans_time_of_day,
                    accessions_by_day,
                    average_accessions_time_of_day,
                    tickets_by_day,
                    average_tickets_time_of_day]
    
    
    # Merges user-level DataFrames. Sorts by day and pivots.
    for x in user_dataframes:
        users_and_days=users_and_days.merge(x, left_on=["day","name"], right_index=True, how="outer").fillna(0)
    users_and_days["sortday"]=pd.to_datetime(users_and_days["day"])
    users_and_days_pivot_table=users_and_days.pivot(index=["name"],
                              columns=["sortday"],
                              values=['pages_scanned', 'samples_acccessioned','tickets_created'])
    
    # Merges day-level DataFrames.
    for x in day_dataframes:
        daily=daily.merge(x, left_index=True, right_index=True,how='left')
    
    # Handles NULL values
    for col in daily.select_dtypes(exclude=["timedelta64[ns]"]).columns:
        daily[col]=daily[col].fillna(0)
    
    # Converts timedelta to time of day. Transposes.
    daily=myutils.timedelta2time(daily).transpose()
    
    # Pushes to this week's spreadsheet
    thisweekreport="Accessioning - Weekly Shift Report - {}".format(startdate.strftime("%Y%m%d"))
    myutils.pd2gs(thisweekreport, "User Data", users_and_days_pivot_table, include_index=True)
    myutils.pd2gs(thisweekreport, "Daily Data", daily, include_index=True)
    

if __name__=="__main__":
    try:
        main(datetime(2021,1,8))
        print("Succeeded at {}".format(datetime.now()))
        input("Press enter to quit")
    except Exception as e:
        print("Error: {}".format(e))
        print("Failed at {}".format(datetime.now()))
        input("Press enter to quit")
    
    
    
    
    
    
    
    