# -*- coding: utf-8 -*-
"""
Created on Sun Jan 17 12:46:13 2021

@author: Jacob-Windows
"""

from sqlalchemy import create_engine
import pandas as pd
from common import myutils
import os

# I signed in on 1-17-2021 at 12:41 pm

engine=create_engine(os.environ["PROD_FOLLOWER_DATABASE_URL"])
    sql="""
select event_type, e.created_at at time zone 'utc' at time zone 'est' as event_log_timestamp
from event_logs e 
join users u on u.id = e.user_id 
where username like '%%jacob%%' and e.created_at::date = '1-17-2021'
order by event_log_timestamp
    
    
    """
    
 df=pd.read_sql_query(sql, engine)
        dfconverted=myutils.UTC2EST(df)