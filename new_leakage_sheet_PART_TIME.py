#!/usr/bin/env python
# coding: utf-8

# In[14]:


from Ashok import *
import pandas as pd
import datetime as dt
import requests
ROCKESET_API_KEY="Sqp5v7CpD0LNGu7Y9h4rkhbp7m3AkcgpvxCCDWStU1ihpUN8X9cOjAODXTZYuEbj"
ROCKESET_QUERY_URL="https://api.rs2.usw2.rockset.com/v1/orgs/self/queries"
#batchID="6cca28b0-3a02-4e01-8c1e-a75e4129599d"
headers = {
        "Authorization": "ApiKey {}".format(ROCKESET_API_KEY),
        "Content-Type": "application/json"
    }

query="""select a.id as appId,a.applicantJourneyDates from commons.Application a left join commons.Batch b on a.batchID = b.id where b.startDate = '2022-02-07'
-- INNER JOIN commons.Candidate ON User.id = Candidate.id
-- INNER JOIN commons.Interview ON Interview.applicationID = Application.id
    -- AND
    --Application.assessmentAttempts = 0
    -- AND
    -- Application.status NOT in ('TEST_FAILED', 'TEST_TO_BE_TAKEN')
    -- AND
    -- Application.status in ('DOCUMENTATION_COMPLETED', 'ADMITTED')
    -- AND
    -- Application.status in ('INTERVIEW_FAILED')
    --AND
    --Application.status in ('APPLICATION_CLOSED')
    
;
""".format(str(dt.date.today()-dt.timedelta(days=20)))



body = {
        "sql": {
            "query": query
        }
    }
rocksetResponse = requests.post(ROCKESET_QUERY_URL, json=body, headers=headers)
import pandas as pd
import time as tm
data=pd.DataFrame(rocksetResponse.json()['results'])


# In[15]:


data


# In[16]:


l=[]
for index,row in data.iterrows():
    temp=pd.DataFrame(row['applicantJourneyDates'])
    temp['appId']=row['appId']
    l.append(temp)
    #print(index)
final=pd.concat(l)
final=final.sort_values('date',ascending=False)
final


# In[17]:


final


# In[18]:


import datetime as dt
from datetime import datetime
final['date'] = pd.to_datetime(final.date).dt.tz_localize(None)
final['date'] = final['date']+dt.timedelta(minutes = 330)
final['date'] = final['date'].dt.date


# In[19]:


final.sort_values(by = ['date'])


# In[20]:


df1  = final.groupby('date')['status'].value_counts().unstack().fillna(0).astype(int).sort_values(by = ['date'],ascending = False)


# In[ ]:





# In[21]:


df1 = df1[['TEST_TO_BE_TAKEN','TEST_FAILED','METTL_TO_BE_TAKEN','METTL_STARTED','METTL_DESCISION_PENDING','METTL_PASSED','METTL_FAILED','DOCUMENTATION_COMPLETED','APPLICATION_CLOSED','ADMITTED']]


# In[22]:


df2 = df1.rename(columns = {'TEST_TO_BE_TAKEN':'Test to be Taken','TEST_FAILED':'Test Failed','METTL_TO_BE_TAKEN':'Mettl to be Taken','METTL_STARTED':'Mettl Started','METTL_DESCISION_PENDING':'Mettl Decision Pending ','METTL_PASSED':'Mettl Passed','METTL_FAILED':'Mettl Failed','DOCUMENTATION_PENDING':'Documentation Pending','DOCUMENTATION_COMPLETED':'Documentation Completed','APPLICATION_CLOSED':'Application Closed','ADMITTED':'Admitted'})


# In[23]:


df2


# In[24]:


ws = client.open_by_url("https://docs.google.com/spreadsheets/d/1nguaslcW8hc8Ij2On-Sf4bgvswp6kOA5159fuMlvutA/edit#gid=2108431856").worksheet("PART_TIME_BATCH")


# In[25]:


set_with_dataframe(ws,df2,row = 5,col = 1,include_index = True)


# In[61]:


import pandas as pd
import datetime as dt
import requests
ROCKESET_API_KEY="Sqp5v7CpD0LNGu7Y9h4rkhbp7m3AkcgpvxCCDWStU1ihpUN8X9cOjAODXTZYuEbj"
ROCKESET_QUERY_URL="https://api.rs2.usw2.rockset.com/v1/orgs/self/queries"
#batchID="6cca28b0-3a02-4e01-8c1e-a75e4129599d"
headers = {
        "Authorization": "ApiKey {}".format(ROCKESET_API_KEY),
        "Content-Type": "application/json"
    }

query="""select  u.createdAt from commons.User u 
-- INNER JOIN commons.Candidate ON User.id = Candidate.id
-- INNER JOIN commons.Interview ON Interview.applicationID = Application.id

WHERE
    PARSE_TIMESTAMP(
        '%Y-%m-%dT%H:%M:%E*SZ',
        '{}T00:00:00.000Z'
    ) <= PARSE_TIMESTAMP(
        '%Y-%m-%dT%H:%M:%E*SZ',
        u.createdAt
    )
    -- AND
    --Application.assessmentAttempts = 0
    -- AND
    -- Application.status NOT in ('TEST_FAILED', 'TEST_TO_BE_TAKEN')
    -- AND
    -- Application.status in ('DOCUMENTATION_COMPLETED', 'ADMITTED')
    -- AND
    -- Application.status in ('INTERVIEW_FAILED')
    --AND
    --Application.status in ('APPLICATION_CLOSED')
    
;
""".format(str(dt.date.today()-dt.timedelta(days=20)))



body = {
        "sql": {
            "query": query
        }
    }
rocksetResponse = requests.post(ROCKESET_QUERY_URL, json=body, headers=headers)
import pandas as pd
import time as tm
df2=pd.DataFrame(rocksetResponse.json()['results'])


# In[62]:


df2


# In[63]:


df2


# In[64]:


import datetime as dt
from datetime import datetime
df2['createdAt'] = pd.to_datetime(df2.createdAt).dt.tz_localize(None)
df2['createdAt'] = df2['createdAt']+dt.timedelta(minutes = 330)
df2['createdAt'] = df2['createdAt'].dt.date


# In[65]:


df2


# In[66]:


df3 = df2.value_counts().sort_index(ascending = False).reset_index().rename(columns = {0:'Registration','createdAt':'Date'})


# In[67]:


df3


# In[68]:


ws = client.open_by_url("https://docs.google.com/spreadsheets/d/1nguaslcW8hc8Ij2On-Sf4bgvswp6kOA5159fuMlvutA/edit#gid=0").worksheet("Total Registration")


# In[69]:


set_with_dataframe(ws,df3,row = 5,col = 1)


# In[ ]:





# In[ ]:





# In[ ]:




