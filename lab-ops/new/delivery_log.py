# -*- coding: utf-8 -*-
"""
Created on Wed Jan 20 09:38:21 2021

@author: Jacob-Windows
"""
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from common import myutils

today = datetime.now().date()
today = datetime(2021,1,12)
enddate = today + timedelta(days=7-today.weekday())
startdate = today - timedelta(days=today.weekday())


accessioningreport = "Accessioning - Weekly Shift Report - {}".format(startdate.strftime("%Y%m%d"))
thisweeklog = "Accessioning Logs - Week Starting {}".format(startdate.strftime("%Y%m%d"))

logs = myutils.gs2pd(thisweeklog, "Delivery Logs")

logs = logs.dropna(subset=["Day","Time"])

logs=logs.iloc[:,0:7]

logs["datetime"] = [pd.to_datetime(x + " " + y) for x,y in zip(logs.Day, logs.Time)]
logs["Date"] = [pd.to_datetime(x) for x in logs.Day]

logs["7am"] = [pd.to_datetime(x + " 7:00 AM") for x in logs.Day]
logs["Weighted Average Time"] = [pd.to_timedelta(x - y) for x,y in zip(logs.datetime,logs['7am'])]
logs['Weighted Average Time'] = logs["Weighted Average Time"].values.astype(np.int64)
logs['Weighted Average Time'] = logs["Weighted Average Time"] * logs.Packages

logs["Total Deliveries"]=1
agg=logs.groupby("Day").agg({"Total Deliveries"         : np.sum,
                             "Weighted Average Time"    : np.mean,
                             "Packages"                 : np.sum,
                             "Samples"                  : np.sum})

agg['Weighted Average Time'] = [pd.to_timedelta(x) for x in agg["Weighted Average Time"]]
agg['Weighted Average Time'] = [str(x).split(" ")[2] for x in agg["Weighted Average Time"]]


agg= agg.transpose()

myutils.pd2gs(accessioningreport, "Delivery Log", agg, include_index=True)


bins = myutils.gs2pd(thisweeklog, "Bin Logs")
myutils.pd2gs(accessioningreport, "Bin Log", bins,header=False)
