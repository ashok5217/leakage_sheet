#!/usr/bin/env python
# coding: utf-8

# In[1]:


from Ashok import *


# In[2]:


df1 = pd.read_sql("select *from course",con)


# In[3]:


df1 = df1[['id','course_type']]


# In[4]:


df2 = pd.read_sql("select *from batch",con)


# In[5]:


df2.rename(columns = {'id':'batch_id'},inplace = True)


# In[6]:


df3 = pd.merge(df1,df2,left_on = ['id'],right_on = ['course_id'],how = 'outer')


# In[7]:


df3 = df3[['id','course_type','batch_id','course_id']]


# In[8]:


batch = df3[df3['course_id']==1]['batch_id'].max()


# In[9]:


batch


# In[ ]:





# In[ ]:





# In[10]:


df = pd.read_sql("""select p.name,p.email,p.created_at profile_created_at,a.onboarding_session_one_time_link, a.slug,a.masai_foundation_program,a.onboarding_session_one_time_link, j.new_status,j.application_slug,j.created_at as journey_date,b.batch_id,b.course_specialization_id,batch.start_date as batch_start_date,onboarding_session.start_date_time,JSON_UNQUOTE(JSON_EXTRACT(a.reject_reason, '$."0"')) as reject_reason from application a  left join  funnel_entry_date j  on a.slug = j.application_slug left join profile as p  on  a.profile_slug = p.slug left join batch_campus b on a.batch_campus_id = b.id left join batch on batch.id = b.batch_id  left join onboarding_session on onboarding_session.application_slug = a.slug where b.batch_id = {}""".format(batch),con)


# In[11]:


df['batch_start_date'].value_counts()


# In[12]:


df['journey_date'] = (df['journey_date']+pd.Timedelta(minutes = 330)).dt.date
#df['journey_date'] = (df['journey_date']).dt.date


# In[13]:


df.rename(columns = {'new_status':'status','start_date_time':'onboarding_start_time'},inplace = True)


# In[14]:




#df['Eligible Application'] = df['status'].isin(['METTL_TO_BE_TAKEN',  'METTL_FAILED','ONBOARDING_COMPLETE', 'ONBOARDING_PENDING','METTL_DESCISION_PENDING', 'ONBOARDING_STARTED', 'METTL_PASSED','APPLICATION_CLOSED', 'METTL_STARTED','FELLOW'])
df['Profile Unfilled'] = df['status']=='PROFILE_UNFILLED'
df['Ineligible'] = df['status']=='INELIGIBLE'
#df['Eligible & Profile Unfilled'] = df['status'].isin(['METTL_TO_BE_TAKEN',  'METTL_FAILED','ONBOARDING_COMPLETE', 'ONBOARDING_PENDING','METTL_DESCISION_PENDING', 'ONBOARDING_STARTED', 'METTL_PASSED','APPLICATION_CLOSED', 'METTL_STARTED'])
df['Mettl to be taken'] = df['status']== 'METTL_TO_BE_TAKEN'
df['mettl_attempted']=df['status'].isin([ 'METTL_STARTED'])
#df['mettl_cleared']=df['status'].isin(['METTL_PASSED'])
df['verification_pending']=df['status']=='METTL_PASSED'
df['verification_complete']=df['status'].isin(['ONBOARDING_PENDING'])
df['Mettl Started'] = df['status']== 'METTL_STARTED'
df['Mettl Failed'] = df['status']== 'METTL_FAILED'
#df['Mettl Passed'] = df['status']== 'METTL_PASSED'
df['Application Closed'] = df['status']== 'APPLICATION_CLOSED'
df['Mettl Decesion Pending'] = df['status']== 'METTL_DESCISION_PENDING'
df['Onboarding Started'] = df['status']== 'ONBOARDING_STARTED'
df['Onboarding Pending'] = df['status']== 'ONBOARDING_PENDING'
df['Onboarding Complete']= df['status']== 'ONBOARDING_COMPLETE'


# In[15]:


df1 = df.groupby('journey_date').agg({'Ineligible':sum,'Profile Unfilled':sum,'Mettl to be taken':sum,'mettl_attempted':sum,'Mettl Failed':sum,'verification_pending':sum,'verification_complete':sum,'Application Closed':sum}).sort_index(ascending = False)


# In[16]:


df1


# In[17]:


df2 = pd.read_sql("""select a.created_at,JSON_UNQUOTE(JSON_EXTRACT(a.reject_reason, '$."0"')) as reject_reason from application as a""",con)


# In[ ]:





# In[18]:


df2['created_at'] = (df2['created_at']+pd.Timedelta(minutes = 330)).dt.date
#df['journey_date'] = (df['journey_date']).dt.date


# In[19]:


df2 = df2.groupby('created_at')['reject_reason'].value_counts().unstack()


# In[20]:


df2 = df2[['To be eligible for this course you must be graduating before 2022','You must be between 18 and 28 years old for Software Development courses','You must be ready to take a job after graduation']].sort_values('created_at',ascending = False).fillna(0).astype(int)


# In[ ]:





# In[21]:


df2.rename(columns = {'To be eligible for this course you must be graduating before 2022':'graduation Year','You must be between 18 and 28 years old for Software Development courses':'Age','You must be ready to take a job after graduation':'Not ready for job after Graduation'},inplace = True)


# In[22]:


df2 = df2.reset_index()
df2.head()


# In[ ]:





# In[ ]:





# In[23]:


df3 = pd.read_sql("select application.created_at from application left join batch_campus on application.batch_campus_id = batch_campus.id where batch_campus.batch_id  = {}".format(batch),con)


# In[24]:


df3['created_at'] = (df3['created_at']+pd.Timedelta(minutes = 330)).dt.date
#df['journey_date'] = (df['journey_date']).dt.date


# In[25]:


df3 = df3.value_counts()


# In[26]:


df3 = df3.reset_index().rename(columns = {'created_at':'date',0:'Application'})


# In[27]:


df3.head()


# In[28]:


df4 = pd.read_sql("select profile.created_at from profile",con)


# In[29]:


df4['created_at'] = (df4['created_at']+pd.Timedelta(minutes = 330)).dt.date
#df['journey_date'] = (df['journey_date']).dt.date


# In[30]:


df4 = df4['created_at'].value_counts()


# In[31]:


df4 = df4.reset_index()


# In[32]:


df4 = df4.rename(columns = {'index':'date','created_at':'Registration'})


# In[33]:


df4.head()


# In[34]:


df3.head(2)


# In[35]:


df4.head(2)


# In[36]:


df5 = pd.merge(df4,df3,how = 'left',on  = ['date'])


# In[37]:


df5.head(2)


# In[38]:


df2.head(2)


# In[39]:


df6 = pd.merge(df5,df2,how= 'left',left_on = ['date'],right_on = ['created_at'])


# In[40]:


df6.head()


# In[41]:


df1.head(2)


# In[42]:


df8 = pd.merge(df6,df1,left_on = ['date'],right_on = ['journey_date'])


# In[43]:


df8.head()


# In[ ]:





# In[44]:


df8['date'] = pd.to_datetime(df8['date'])


# In[45]:


df8 = df8[df8['date']>'2022-02-15']


# In[46]:


df8.drop(columns = ['created_at'],axis =1,inplace = True)


# In[47]:


df8.columns = [i.title() for i in df8.columns]
df8.columns = [i.replace("_"," ") for i in df8.columns]
df8.sort_values(['Date'],inplace = True,ascending = False)


# In[48]:


df8


# In[ ]:





# In[49]:


ws = client.open_by_url("https://docs.google.com/spreadsheets/d/1YmI9DI1gJAEsGu5cbBSTJhxVa__iWs8RMzpqPTDohdc/edit?pli=1#gid=1865274434").worksheet("j_ft_raw")


# In[50]:


set_with_dataframe(ws,df8,row = 10,col = 1)


# In[ ]:





# In[ ]:




#!/usr/bin/env python
# coding: utf-8

# In[1]:


from Ashok import *


# In[2]:


df1 = pd.read_sql("select *from course",con)


# In[3]:


df1 = df1[['id','course_type']]


# In[4]:


df2 = pd.read_sql("select *from batch",con)


# In[5]:


df2.rename(columns = {'id':'batch_id'},inplace = True)


# In[6]:


df3 = pd.merge(df1,df2,left_on = ['id'],right_on = ['course_id'],how = 'outer')


# In[7]:


df3 = df3[['id','course_type','batch_id','course_id']]


# In[8]:


batch = df3[df3['course_id']==2]['batch_id'].max()


# In[9]:


batch


# In[ ]:





# In[ ]:





# In[10]:


df = pd.read_sql("""select p.name,p.email,p.created_at profile_created_at,a.onboarding_session_one_time_link, a.slug,a.masai_foundation_program,a.onboarding_session_one_time_link, j.new_status,j.application_slug,j.created_at as journey_date,b.batch_id,b.course_specialization_id,batch.start_date as batch_start_date,onboarding_session.start_date_time,JSON_UNQUOTE(JSON_EXTRACT(a.reject_reason, '$."0"')) as reject_reason from application a  left join  funnel_entry_date j  on a.slug = j.application_slug left join profile as p  on  a.profile_slug = p.slug left join batch_campus b on a.batch_campus_id = b.id left join batch on batch.id = b.batch_id  left join onboarding_session on onboarding_session.application_slug = a.slug where b.batch_id = {}""".format(batch),con)


# In[11]:


df['batch_start_date'].value_counts()


# In[12]:


df['journey_date'] = (df['journey_date']+pd.Timedelta(minutes = 330)).dt.date
#df['journey_date'] = (df['journey_date']).dt.date


# In[13]:


df.rename(columns = {'new_status':'status','start_date_time':'onboarding_start_time'},inplace = True)


# In[14]:




#df['Eligible Application'] = df['status'].isin(['METTL_TO_BE_TAKEN',  'METTL_FAILED','ONBOARDING_COMPLETE', 'ONBOARDING_PENDING','METTL_DESCISION_PENDING', 'ONBOARDING_STARTED', 'METTL_PASSED','APPLICATION_CLOSED', 'METTL_STARTED','FELLOW'])
df['Profile Unfilled'] = df['status']=='PROFILE_UNFILLED'
df['Ineligible'] = df['status']=='INELIGIBLE'
#df['Eligible & Profile Unfilled'] = df['status'].isin(['METTL_TO_BE_TAKEN',  'METTL_FAILED','ONBOARDING_COMPLETE', 'ONBOARDING_PENDING','METTL_DESCISION_PENDING', 'ONBOARDING_STARTED', 'METTL_PASSED','APPLICATION_CLOSED', 'METTL_STARTED'])
df['Mettl to be taken'] = df['status']== 'METTL_TO_BE_TAKEN'
df['mettl_attempted']=df['status'].isin([ 'METTL_STARTED'])
#df['mettl_cleared']=df['status'].isin(['METTL_PASSED'])
df['verification_pending']=df['status']=='METTL_PASSED'
df['verification_complete']=df['status'].isin(['ONBOARDING_PENDING'])
df['Mettl Started'] = df['status']== 'METTL_STARTED'
df['Mettl Failed'] = df['status']== 'METTL_FAILED'
#df['Mettl Passed'] = df['status']== 'METTL_PASSED'
df['Application Closed'] = df['status']== 'APPLICATION_CLOSED'
df['Mettl Decesion Pending'] = df['status']== 'METTL_DESCISION_PENDING'
df['Onboarding Started'] = df['status']== 'ONBOARDING_STARTED'
df['Onboarding Pending'] = df['status']== 'ONBOARDING_PENDING'
df['Onboarding Complete']= df['status']== 'ONBOARDING_COMPLETE'


# In[15]:


df1 = df.groupby('journey_date').agg({'Ineligible':sum,'Profile Unfilled':sum,'Mettl to be taken':sum,'mettl_attempted':sum,'Mettl Failed':sum,'verification_pending':sum,'verification_complete':sum,'Application Closed':sum}).sort_index(ascending = False)


# In[16]:


df1


# In[17]:


df2 = pd.read_sql("""select a.created_at,JSON_UNQUOTE(JSON_EXTRACT(a.reject_reason, '$."0"')) as reject_reason from application as a""",con)


# In[ ]:





# In[18]:


df2['created_at'] = (df2['created_at']+pd.Timedelta(minutes = 330)).dt.date
#df['journey_date'] = (df['journey_date']).dt.date


# In[19]:


df2 = df2.groupby('created_at')['reject_reason'].value_counts().unstack()


# In[20]:


df2 = df2[['To be eligible for this course you must be graduating before 2022','You must be between 18 and 28 years old for Software Development courses','You must be ready to take a job after graduation']].sort_values('created_at',ascending = False).fillna(0).astype(int)


# In[ ]:





# In[21]:


df2.rename(columns = {'To be eligible for this course you must be graduating before 2022':'graduation Year','You must be between 18 and 28 years old for Software Development courses':'Age','You must be ready to take a job after graduation':'Not ready for job after Graduation'},inplace = True)


# In[22]:


df2 = df2.reset_index()
df2.head()


# In[ ]:





# In[ ]:





# In[23]:


df3 = pd.read_sql("select application.created_at from application left join batch_campus on application.batch_campus_id = batch_campus.id where batch_campus.batch_id  = {}".format(batch),con)


# In[24]:


df3['created_at'] = (df3['created_at']+pd.Timedelta(minutes = 330)).dt.date
#df['journey_date'] = (df['journey_date']).dt.date


# In[25]:


df3 = df3.value_counts()


# In[26]:


df3 = df3.reset_index().rename(columns = {'created_at':'date',0:'Application'})


# In[27]:


df3.head()


# In[28]:


df4 = pd.read_sql("select profile.created_at from profile",con)


# In[29]:


df4['created_at'] = (df4['created_at']+pd.Timedelta(minutes = 330)).dt.date
#df['journey_date'] = (df['journey_date']).dt.date


# In[30]:


df4 = df4['created_at'].value_counts()


# In[31]:


df4 = df4.reset_index()


# In[32]:


df4 = df4.rename(columns = {'index':'date','created_at':'Registration'})


# In[33]:


df4.head()


# In[34]:


df3.head(2)


# In[35]:


df4.head(2)


# In[36]:


df5 = pd.merge(df4,df3,how = 'left',on  = ['date'])


# In[37]:


df5.head(2)


# In[38]:


df2.head(2)


# In[39]:


df6 = pd.merge(df5,df2,how= 'left',left_on = ['date'],right_on = ['created_at'])


# In[40]:


df6.head()


# In[41]:


df1.head(2)


# In[42]:


df8 = pd.merge(df6,df1,left_on = ['date'],right_on = ['journey_date'])


# In[43]:


df8.head()


# In[ ]:





# In[44]:


df8['date'] = pd.to_datetime(df8['date'])


# In[45]:


df8 = df8[df8['date']>'2022-02-15']


# In[46]:


df8.drop(columns = ['created_at'],axis =1,inplace = True)


# In[47]:


df8.columns = [i.title() for i in df8.columns]
df8.columns = [i.replace("_"," ") for i in df8.columns]
df8.sort_values(['Date'],inplace = True,ascending = False)


# In[48]:


df8


# In[ ]:





# In[49]:


ws = client.open_by_url("https://docs.google.com/spreadsheets/d/1YmI9DI1gJAEsGu5cbBSTJhxVa__iWs8RMzpqPTDohdc/edit?pli=1#gid=1865274434").worksheet("j_pt_raw")


# In[50]:


set_with_dataframe(ws,df8,row = 10,col = 1)


# In[ ]:





# In[ ]:




