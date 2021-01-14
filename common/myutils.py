import os
import requests
import json
import pandas as pd
import gspread_dataframe as gd
import gspread
import easypost
import csv
from datetime import datetime
from sqlalchemy import create_engine
import numpy as np
import psycopg2

###################HUBSPOT##############################

def get_using_url(url,proplist,prod=False):
    if prod:
        hapikey=os.environ["PROD_HAPIKEY"]
    else:
        hapikey=os.environ["test_hapikey"]
    querystring = {"limit":"5","hapikey":hapikey,"properties":proplist,"associations":{"deals","companies"}}
    headers = {'accept': 'application/json'}
    has_more = True
    getlist=[]
    while has_more:
        r = requests.request("GET",url, headers=headers, params=querystring)
        response_dict = json.loads(r.text)
        getlist.extend(response_dict['results'])
        if len(response_dict)>1:
            paging = pd.json_normalize(response_dict["paging"])["next.after"]
            querystring.update({'after' : paging[0]})
        else:
            has_more = False
    df=pd.json_normalize(getlist)
    df=df.rename(lambda x: "dealid" if 'associations.deals' in x else x, axis=1)
    df=df.rename(lambda x: "companyid" if 'associations.comp' in x else x, axis=1)
    if any(y in url for y in ["line_items","deals?"]):
        try:
            df["dealid"]=[x[0]['id'] if x is not np.nan else np.nan for x in df.dealid]
        except:
            df["companyid"]=[x[0]['id'] if x is not np.nan else np.nan for x in df.companyid]
    else:
        try:
            df=df.drop(columns=["dealid"])
        except:
            pass

    df.columns = df.columns.str.replace('properties.', '')
    return df

def get_hs_properties(crmobject):
    url = "https://api.hubapi.com/crm/v3/properties/{}?".format(crmobject)
    parameter_dict = {'hapikey': os.environ["test_hapikey"]}
    r = requests.request("GET",url, params=parameter_dict)
    response_dict = json.loads(r.text)
    result=pd.json_normalize(response_dict["results"])
    return result

def get_hs_object(crmobject,prod=False):
    proplist=get_hs_properties(crmobject)
    proplist=list(proplist["name"])
    url = "https://api.hubapi.com/crm/v3/objects/{}?".format(crmobject)
    df = get_using_url(url,proplist,prod=prod)
    return df


def get_owners(prod=False):
    url = "https://api.hubapi.com/crm/v3/owners/"
    df = get_using_url(url,proplist=None,prod=prod)
    return df


def get_pipelines(prod=False):
    url = "https://api.hubapi.com/crm/v3/pipelines/deals"
    df = get_using_url(url,proplist=None,prod=prod)
    df=df.drop(columns=["stages"])
    return df

def get_stages(pipeline,prod=False):
    pipelines=get_pipelines(prod=prod)
    pipelineid=pipelines.id[pipelines.label==pipeline].iloc[0]
    url = "https://api.hubapi.com/crm/v3/pipelines/deals/{}/stages".format(pipelineid)
    df = get_using_url(url,proplist=None,prod=prod)
    return df



def update_db_with_hs(prod=False):
    test_engine = create_engine(os.environ["LOCAL_DB_URL"])
    hs_objects=["companies",
                "products",
                "deals",
                "line_items"]
    
    
    for obj in hs_objects:
        df=get_hs_object(obj)
        try:
            df.to_sql(obj,test_engine,if_exists='replace',index=False)
        except:
            print(obj)
            
###################END HUBSPOT##############################
        

############################################################
def pd2gs(wkbook,sheet,df,clear=True,include_index=False):
    print("pushing {} rows, {} columns, to {} sheet of {}".format(len(df),len(df.columns),sheet,wkbook))
    gc = gspread.service_account(filename=os.environ["service_account_cred"])
    ws = gc.open(wkbook).worksheet(sheet)
    if clear:
        ws.clear()
    gd.set_with_dataframe(ws, df, include_index=include_index)
    
    
def UTC2EST(df, *newformat):
    timecols=df.select_dtypes(include=["datetime64[ns]"])
    for col in timecols:
        df[col]=df[col] \
                .dt.tz_localize("UTC") \
                .dt.tz_convert("EST") \
                .dt.tz_localize(None)
        if newformat:
            df[col]=[x.strftime(newformat[0]) if x is not pd.NaT else np.nan for x in df[col]]
    return df


def timedelta2time(df):
    timecols=df.select_dtypes(include=["timedelta64[ns]"])
    for col in timecols:
        df[col]=[x.split(" ")[2].split(".")[0] if x != "NaT" else "" for x in df[col].astype(str)]
    return df


def colsearch(df,string):
    cols = df.columns[[string in col.lower() for col in df]]
    return cols

###########################################################
def execute_sql(conn, *argv):
    """ Execute any SQL statement, with returning the number of records impacted by the statement """
    with conn.cursor() as cur:
        cur.execute(*argv)
        return cur.rowcount


def get_db_connection(ignore_role=False):
    """ Create connection to the database using environment variables for the DATABASE_URL and DISTRIBUTOR_ID """
    connection = psycopg2.connect(os.environ["PROD_FOLLOWER_DATABASE_URL"])
    if not ignore_role:
        execute_sql(connection, "SET ROLE = 'dist_%(distributor_id)s_bypassrls_group';", {'distributor_id': 15})
    return connection


def colsearch(df,string):
    cols = df.columns[[string in col.lower() for col in df]]
    return cols
        

#################### EASYPOST ###########################################################
#Creates easypost objects (trackers) using a list of tracking codes in csv format. Can also return a list of those trackers.
def create_trackers_from_csv(file_location):
    easypost.api_key= os.environ['easypost_api_key']
    results=[]
    with open(file_location, newline='',encoding='utf-8-sig') as csvfile:
        for row in csv.reader(csvfile):
            results.append(int(row[0]))
            easypost.Tracker.create(tracking_code=row[0],carrier="fedex")
    print("{} tracking numbers added to easypost".format(len(results)))
    return results

def create_ep_trackers_list(list_of_ids,carrier):
    easypost.api_key= os.environ['easypost_api_key']
    for x in list_of_ids:
        if x != None:
            easypost.Tracker.create(tracking_code=x,carrier=carrier)

#Returns a list of trackers from a csv
def get_ids_from_csv(file_location):
    results=[]
    with open(file_location, newline='',encoding='utf-8-sig') as csvfile:
        for row in csv.reader(csvfile):
            results.append(int(row[0]))
    print("{} tracking numbers added to easypost".format(len(results)))
    return results  
    
#Returns a pandas dataframe with inbound shipments of samples
def get_easypost_trackers(list_of_trackers,get_all=False):
    easypost.api_key= os.environ['easypost_api_key']
    if get_all:
        
        
        # window cannot be more than 31 days
        start_datetime = '2020-10-01T00:00:00Z'
        
        # these are the objects that support retrieval and pagination
        # comment out the types you do not want to retrieve
        for key, obj_type in (
                              #('batches', easypost.Batch), 
                              #('scan_forms', easypost.ScanForm), 
                              #('shipments', easypost.Shipment),
                              ('trackers', easypost.Tracker),
                              ):
        
            # make our initial query
            data_obj = obj_type.all(start_datetime=start_datetime, 
                                    page_size=1)
            
            # get the list of items returned from the query
            data = data_obj.get(key)
        
            # create list to gather our results in
            results = []
        
            # determine if we have additional data; used for the loop below
            has_more = data_obj.has_more
            
            while has_more:
                # store the data we just retrieved
                for item in data:
                    results.append(item)
        
                # see if we have additional data available
                has_more = data_obj.has_more
                
                # continue to query the next page if we have additional data, 
                # setting the `before_id` parameter to the last ID seen
                if has_more:
                    data_obj = obj_type.all(start_datetime=start_datetime, page_size=100, before_id=results[-1].id)
                    data = data_obj.get(key)
        
            # ensure that we get the data when we're at the end
            for item in data:
                results.append(item)
                
        
            # ensure that the results are returned in ascending order
            results.sort(key=lambda x: x.created_at)
            
            #SOMETHING I ADDED TO TURN IT INTO A LIST OF DICTS
            results_dicts = []
            for item in results:
                results_dicts.append(item.to_dict())
            
            
            # print out our results; the type and number returned
            # along with the created_at data and item ID
            """
            print(key, len(results))
            for i in results:
                print('\t' + f'{i.created_at} {i.id}')
            if results:
                print()
            """
    else:
        results_dicts=[]
        for x in list_of_trackers:
            if x != None:        
                tracker=easypost.Tracker.all(tracking_code=x)
                results_dicts.append(tracker.to_dict()["trackers"])
        results_dicts=[x[0] for x in results_dicts if x != []]
                
        
        #Reorganizes into pandas dataframe with useful columns
    trackers=pd.DataFrame(results_dicts)
    
    columns_to_create=["destination",
                       "origin",
                       "initial_delivery_attempt",
                       "delivery_date",
                       "picked_up_city",
                       "picked_up_state",
                       "delivery_datetime"]
    
    for column in columns_to_create:
        trackers[column]=None
    
    for x in trackers.carrier_detail:
        if x != None:
            trackers["destination"]                 =   x["destination_location"]
            trackers["origin"]                      =   x["origin_location"]
            trackers["initial_delivery_attempt"]    =   x["initial_delivery_attempt"]

    
    for y in trackers.tracking_details:
        if y != []:
            trackers["delivery_date"]               =   y[-1]["datetime"]
            trackers["picked_up_city"]              =   y[0]['tracking_location']["city"]
            trackers["picked_up_state"]             =   y[0]['tracking_location']["state"]
    
    #Boolean for delivered
    trackers['delivered']=trackers.status=="delivered"
    
    #Datetime object based on delivery date. Not sure what else I might do with the delivery date
    trackers["delivery_datetime"]=[datetime.strptime(x,"%Y-%m-%dT%H:%M:%SZ") if x!=None else None for x in trackers.delivery_date]
           
    #Handles cases where delivery date is actually the last date in tracking_details, but the package is not delivered
    trackers.loc[trackers.status != 'delivered', 'delivery_date'] = None

    #Reorganizes the dataframe
    trackerscolumns=[
        'id',
        'carrier',
        'status',
        'status_detail',
        'created_at',
        'est_delivery_date',
        'destination',
        'origin',
        'delivery_datetime',
        'delivery_date',
        'initial_delivery_attempt',
        'public_url',
        'signed_by',
        'tracking_code',
        'updated_at',
        'picked_up_city',
        'picked_up_state',
        'weight'
    ]
    
    trackerscolnames=[
        'easypost_id',
        'carrier',
        'tracking_status',
        'tracking_status_detail',
        'created_at',
        'est_delivery_date',
        'destination',
        'origin',
        'delivery_datetime',
        'delivery_date',
        'initial_delivery_attempt',
        'public_url',
        'signed_by',
        'tracking_number',
        'updated_at',
        'picked_up_city',
        'picked_up_state',
        'weight'
    ]
    
    trackers=trackers[trackerscolumns]
    trackers.columns=trackerscolnames
    
    #Get only return shipments, which have fedex tracking number (tracking code) starting with 8
    if get_all:
        trackers=trackers[trackers.tracking_number.str.startswith("8")]
    trackers=trackers.dropna(subset=["origin"])
    trackers=trackers[~trackers.tracking_number.str.startswith("`")]
    keeps=[len(x)<17 for x in trackers.tracking_number]
    trackers=trackers[keeps]
    trackers=trackers.dropna(subset=["tracking_number"])
    trackers=trackers[trackers.tracking_number!="N/A"]
    trackers=trackers.astype({'tracking_number':'int64'}).sort_values(by=["delivery_datetime"])
    return trackers


#################### END EASYPOST ###########################################################
