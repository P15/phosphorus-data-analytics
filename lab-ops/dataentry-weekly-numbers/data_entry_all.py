# -*- coding: utf-8 -*-
"""
Created on Sat Jan  2 18:19:57 2021

@author: Jacob-Windows
"""

def reviews_from_db():
    print("getting reports from db...")
    engine = create_engine(os.environ["PROD_FOLLOWER_DATABASE_URL"])
    start_date=datetime(2020,12,27) #Plan to dynamically define start_date in the future
    
    # Creates a pandas dataframe of all reports and events.
    # Includes start hour and created hour as truncated dates.
    # Includes strings of dates that may be used in google sheets as filters or axis labels
    
    sql="""
    SELECT e.*, u.username, 1 AS reviews_completed
    
    FROM event_logs e
        JOIN users u ON u.id = e.user_id
    
    WHERE e.created_at > '{}' 
    --AND e.event_type = 'kit_data_review_complete'
    
    ORDER BY
        e.created_at
    """.format(start_date)
    
    df=pd.read_sql_query(sql,engine)
    df=utils.UTC2EST(df)
    df["event_day"]=[x.strftime("%m/%d/%y") for x in df.created_at]
    return df




def data_entries_from_db():
    print("getting reports from db...")
    engine = create_engine(os.environ["PROD_FOLLOWER_DATABASE_URL"])
    start_date=datetime(2020,12,27) #Plan to dynamically define start_date in the future
    
    # Creates a pandas dataframe of all reports and events.
    # Includes start hour and created hour as truncated dates.
    # Includes strings of dates that may be used in google sheets as filters or axis labels
    
    sql="""
    SELECT e.*, u.username, 1 AS entries_completed
    
    FROM event_logs e
        JOIN users u ON u.id = e.user_id
    
    WHERE e.created_at > '{}' 
    AND e.event_type = 'kit_accessioning_complete'
    
    ORDER BY
        e.created_at
    """.format(start_date)
    
    df=pd.read_sql_query(sql,engine)
    df=utils.UTC2EST(df)
    return df


reviews=reviews_from_db()
entries=data_entries_from_db()
                    