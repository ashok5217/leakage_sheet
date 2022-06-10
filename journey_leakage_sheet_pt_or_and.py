#Journey Full time
#journey part time
#full time batch movmwents
#part time batch time part movemnts
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


df = pd.read_sql("""select p.name,p.email,p.created_at profile_created_at,a.onboarding_session_one_time_link, a.slug,a.masai_foundation_program,a.onboarding_session_one_time_link, j.new_status,j.application_slug,j.created_at as journey_date,b.batch_id,b.course_specialization_id,batch.start_date as batch_start_date,onboarding_session.start_date_time from application a  left join  funnel_entry_date j  on a.slug = j.application_slug left join profile as p  on  a.profile_slug = p.slug left join batch_campus b on a.batch_campus_id = b.id left join batch on batch.id = b.batch_id  left join onboarding_session on onboarding_session.application_slug = a.slug where b.batch_id = {}""".format(batch),con)


# In[11]:


df = df.drop_duplicates()


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


status = df.groupby('journey_date').agg({'Profile Unfilled':sum,'Ineligible':sum,'Mettl to be taken':sum,'mettl_attempted':sum,'Mettl Failed':sum,'verification_pending':sum,'Onboarding Started':sum,'Onboarding Pending':sum,'Onboarding Complete':sum,'Application Closed':sum})


# In[16]:


df1 = pd.read_sql("""select a.slug,JSON_UNQUOTE(JSON_EXTRACT(a.reject_reason, '$."0"')) as reject_reason from application as a left join batch_campus on a.batch_campus_id = batch_campus.id left join batch on batch_campus.batch_id = batch.id where batch.id = {} and a.reject_reason is not null""".format(batch),con)


# In[ ]:





# In[17]:


#df1 = pd.read_sql("""select a.profile_slug,JSON_UNQUOTE(JSON_EXTRACT(a.reject_reason, '$."0"')) as reject_reason from application a where JSON_UNQUOTE(JSON_EXTRACT(a.reject_reason, '$."0"'))  is not null""",con)


# In[18]:


df.head(2)


# In[19]:


df2 = pd.merge(df,df1,on = ['slug'],how = 'left')


# In[ ]:





# In[20]:


df2 = df2[(df2['reject_reason'].notnull()==True) & (df2['status']=='INELIGIBLE')]


# In[21]:


df2['Graduation before 2022'] = df2['reject_reason']=='To be eligible for this course you must be graduating before 2022'
df2['12 Pass'] = df2['reject_reason']=='You must have passed the 12th grade'
df2['Age Not Between 18-28'] = df2['reject_reason']=='You must be between 18 and 28 years old for Software Development courses'
df2['Graduation before 2025'] = df2['reject_reason']=='To be eligible for this course you must be graduating before 2025'



# In[22]:


df2['reject_reason'].value_counts(ascending = True)


# In[23]:


df2 = df2.groupby("journey_date").agg({'Graduation before 2022':sum,'12 Pass':sum,'Age Not Between 18-28':sum,'Graduation before 2025':sum})


# In[24]:


application = pd.merge(status,df2,on = ['journey_date'])


# In[25]:


df4 = pd.read_sql("select profile.created_at from profile",con)


# In[26]:


df4['created_at'] = (df4['created_at']+pd.Timedelta(minutes = 330)).dt.date
#df['journey_date'] = (df['journey_date']).dt.date


# In[27]:


df4 = df4['created_at'].value_counts()


# In[28]:


df4 = df4.reset_index()


# In[29]:


df4 = df4.rename(columns = {'index':'journey_date','created_at':'Registration'})


# In[30]:


df4


# In[31]:


reg = pd.merge(df4,application,on = ['journey_date'],how = 'outer')


# In[32]:


reg['journey_date'] = pd.to_datetime(reg['journey_date'])


# In[33]:


reg = reg[reg['journey_date']>'2022-02-20']


# In[34]:


reg


# In[35]:


df5 = pd.read_sql("""select a.created_at from application as a left join batch_campus on a.batch_campus_id = batch_campus.id left join batch on batch_campus.batch_id = batch.id where batch.id = {} """.format(batch),con)


# In[36]:


df5['created_at'] = (df5['created_at']+pd.Timedelta(minutes = 330)).dt.date
#df['journey_date'] = (df['journey_date']).dt.date


# In[37]:


df5


# In[38]:


#df5.rename(columns = {'created_at':'Application'},inplace = True)


# In[39]:


df5 = df5.value_counts().reset_index().rename(columns = {'created_at':'journey_date',0:'Application'})


# In[40]:


df5['journey_date'] = pd.to_datetime(df5['journey_date'])


# In[41]:


df8 = pd.merge(reg,df5,how = 'inner',on = ['journey_date'])


# In[42]:


df8


# In[43]:


df8['journey_date'] = pd.to_datetime(df8['journey_date'])


# In[ ]:





# In[44]:


df8.columns = [i.title() for i in df8.columns]
df8.columns = [i.replace("_"," ") for i in df8.columns]


# In[45]:


df8.sort_values(['Journey Date'],ascending = False,inplace = True)


# In[46]:


df8


# In[47]:


df8['Not ready to take job after graduation ']  = df8['Ineligible'] -df8['Graduation Before 2022'] -df8['12 Pass'] -df8['Age Not Between 18-28']-df8['Graduation Before 2025']


# In[48]:


df8


# In[49]:


df8 = df8.iloc[:,[0,1,16,2,3,12,13,14,15,17,4,5,6,7,8,9,10,11]]


# In[50]:


df8


# In[51]:


ws = client.open_by_url("https://docs.google.com/spreadsheets/d/1YmI9DI1gJAEsGu5cbBSTJhxVa__iWs8RMzpqPTDohdc/edit?pli=1#gid=1865274434").worksheet("j_ft_raw")


# In[52]:


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


df = pd.read_sql("""select p.name,p.email,p.created_at profile_created_at,a.onboarding_session_one_time_link, a.slug,a.masai_foundation_program,a.onboarding_session_one_time_link, j.new_status,j.application_slug,j.created_at as journey_date,b.batch_id,b.course_specialization_id,batch.start_date as batch_start_date,onboarding_session.start_date_time from application a  left join  funnel_entry_date j  on a.slug = j.application_slug left join profile as p  on  a.profile_slug = p.slug left join batch_campus b on a.batch_campus_id = b.id left join batch on batch.id = b.batch_id  left join onboarding_session on onboarding_session.application_slug = a.slug where b.batch_id = {}""".format(batch),con)


# In[11]:


df = df.drop_duplicates()


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


status = df.groupby('journey_date').agg({'Profile Unfilled':sum,'Ineligible':sum,'Mettl to be taken':sum,'mettl_attempted':sum,'Mettl Failed':sum,'verification_pending':sum,'Onboarding Started':sum,'Onboarding Pending':sum,'Onboarding Complete':sum,'Application Closed':sum})


# In[16]:


df1 = pd.read_sql("""select a.slug,JSON_UNQUOTE(JSON_EXTRACT(a.reject_reason, '$."0"')) as reject_reason from application as a left join batch_campus on a.batch_campus_id = batch_campus.id left join batch on batch_campus.batch_id = batch.id where batch.id = {} and a.reject_reason is not null""".format(batch),con)


# In[ ]:





# In[17]:


#df1 = pd.read_sql("""select a.profile_slug,JSON_UNQUOTE(JSON_EXTRACT(a.reject_reason, '$."0"')) as reject_reason from application a where JSON_UNQUOTE(JSON_EXTRACT(a.reject_reason, '$."0"'))  is not null""",con)


# In[18]:


df.head(2)


# In[19]:


df2 = pd.merge(df,df1,on = ['slug'],how = 'left')


# In[ ]:





# In[20]:


df2 = df2[(df2['reject_reason'].notnull()==True) & (df2['status']=='INELIGIBLE')]


# In[21]:


df2['Graduation before 2022'] = df2['reject_reason']=='To be eligible for this course you must be graduating in 2022'
df2['12 Pass'] = df2['reject_reason']=='You must have passed the 12th grade'
df2['Age Not Between 18-28'] = df2['reject_reason']=='You must be between 18 and 28 years old for Software Development courses'
df2['Graduation before 2025'] = df2['reject_reason']=='To be eligible for this course you must be graduating before 2025'
df2['Not ready for job after Course Completion'] = df2['reject_reason'] == 'You must be ready to take a job after graduation'


# In[22]:


df2['reject_reason'].value_counts(ascending = True)


# In[23]:


df2 = df2.groupby("journey_date").agg({'Graduation before 2022':sum,'12 Pass':sum,'Age Not Between 18-28':sum,'Graduation before 2025':sum,'Not ready for job after Course Completion':sum})


# In[24]:


application = pd.merge(status,df2,on = ['journey_date'])


# In[25]:


df4 = pd.read_sql("select profile.created_at from profile",con)


# In[26]:


df4['created_at'] = (df4['created_at']+pd.Timedelta(minutes = 330)).dt.date
#df['journey_date'] = (df['journey_date']).dt.date


# In[27]:


df4 = df4['created_at'].value_counts()


# In[28]:


df4 = df4.reset_index()


# In[29]:


df4 = df4.rename(columns = {'index':'journey_date','created_at':'Registration'})


# In[30]:


df4


# In[31]:


reg = pd.merge(df4,application,on = ['journey_date'],how = 'outer')


# In[32]:


reg['journey_date'] = pd.to_datetime(reg['journey_date'])


# In[33]:


reg = reg[reg['journey_date']>'2022-02-20']


# In[34]:


reg


# In[35]:


df5 = pd.read_sql("""select a.created_at from application as a left join batch_campus on a.batch_campus_id = batch_campus.id left join batch on batch_campus.batch_id = batch.id where batch.id = {} """.format(batch),con)


# In[36]:


df5['created_at'] = (df5['created_at']+pd.Timedelta(minutes = 330)).dt.date
#df['journey_date'] = (df['journey_date']).dt.date


# In[37]:


df5


# In[38]:


#df5.rename(columns = {'created_at':'Application'},inplace = True)


# In[39]:


df5 = df5.value_counts().reset_index().rename(columns = {'created_at':'journey_date',0:'Application'})


# In[40]:


df5['journey_date'] = pd.to_datetime(df5['journey_date'])


# In[41]:


df8 = pd.merge(reg,df5,how = 'inner',on = ['journey_date'])


# In[42]:


df8


# In[43]:


df8['journey_date'] = pd.to_datetime(df8['journey_date'])


# In[ ]:





# In[44]:


df8.columns = [i.title() for i in df8.columns]
df8.columns = [i.replace("_"," ") for i in df8.columns]


# In[45]:


df8.sort_values(['Journey Date'],ascending = False,inplace = True)


# In[46]:


df8


# In[47]:


df8['other']  = df8['Ineligible'] -df8['Graduation Before 2022'] -df8['12 Pass'] -df8['Age Not Between 18-28']-df8['Graduation Before 2025']-df8['Not Ready For Job After Course Completion']


# In[48]:


df8


# In[49]:


df8 = df8.iloc[:,[0,1,17,2,3,13,12,14,16,18,4,5,6,7,8,9,10,11]]


# In[50]:


df8


# In[51]:


ws = client.open_by_url("https://docs.google.com/spreadsheets/d/1YmI9DI1gJAEsGu5cbBSTJhxVa__iWs8RMzpqPTDohdc/edit?pli=1#gid=1865274434").worksheet("j_pt_raw")


# In[52]:


set_with_dataframe(ws,df8,row = 10,col = 1)


# In[ ]:





# In[ ]:











#!/usr/bin/env python
# coding: utf-8

# In[1]:


from Ashok import *


# In[ ]:





# In[2]:


#df = pd.read_sql("select profile.created_at,profile.email,application.status,batch_campus.batch_id,profile.otp_verified,application.onboarding_session_one_time_link,application.reject_reason->'$."0"'  from  profile left join application on application.profile_slug = profile.slug left join batch_campus on application.batch_campus_id = batch_campus.id",con)


# In[3]:


#df= pd.read_sql("""select profile.created_at,profile.email,application.status,batch_campus.batch_id,profile.otp_verified,application.onboarding_session_one_time_link,JSON_UNQUOTE(JSON_EXTRACT(application.reject_reason, '$."0"')) as reject_reason  from  profile left join application on application.profile_slug = profile.slug left join batch_campus on application.batch_campus_id = batch_campus.id""",con)


# In[4]:


df = pd.read_sql("""select profile.created_at,profile.email,application.status,batch_campus.batch_id,profile.otp_verified,application.onboarding_session_one_time_link,JSON_UNQUOTE(JSON_EXTRACT(application.reject_reason, '$."0"')) as reject_reason,onboarding_session.start_date_time as onboarding_start_time  from  profile left join application on application.profile_slug = profile.slug left join batch_campus on application.batch_campus_id = batch_campus.id  left join  onboarding_session on onboarding_session.application_slug = application.slug left join batch on batch.id = batch_campus.batch_id left join course on course.id = batch.course_id where course.id = 1""",con)


# In[5]:


#df['onboarding_start_time'] = df['onboarding_start_time'].apply(lambda x: x if x.str.len()>4 else None)
#df['onboarding_start_time']  = df['onboarding_start_time'].fillna("empty")
df


# In[6]:


def fun(x):
    if len(str(x))>7:
        return str(x)
    else:
        return None
df['onboarding_start_time'] = df['onboarding_start_time'].apply(fun)


# In[7]:


#pd.read_sql("select JSON_EXTRACT(application.reject_reason, '$."0"')  from  profile left join application on application.profile_slug = profile.slug left join batch_campus on application.batch_campus_id = batch_campus.id WHERE JSON_EXTRACT(application.reject_reason, '$."0"') IS NOT NULL",con)


# In[8]:


df['date'] = (pd.to_datetime(df['created_at'])+pd.Timedelta(minutes = 330)).dt.date


# In[9]:


df['Total Application'] = df['status'].notnull()


# In[10]:


df['status'].unique()


# In[11]:


df['Eligible Application'] = df['status'].isin(['METTL_TO_BE_TAKEN',  'METTL_FAILED','ONBOARDING_COMPLETE', 'ONBOARDING_PENDING','METTL_DESCISION_PENDING', 'ONBOARDING_STARTED', 'METTL_PASSED','APPLICATION_CLOSED', 'METTL_STARTED'])


# In[12]:


df


# In[13]:


df['status'].unique()


# In[14]:


df['Registration'] = df['email'].notnull()
df['Profile Complete'] = df['otp_verified']==1.00
#df['Application & Profile Complete'] = df['Profile Complete'] + df['Total Application']
df['Ineligible'] = df['status']== 'INELIGIBLE'
df['Graduation'] = df['reject_reason'].isin(['To be eligible for this course you must be graduating in 2022','To be eligible for this course you must be graduating before 2022'])
df['Age'] = df['reject_reason']=='You must be between 18 and 28 years old for Software Development courses'
df['Not ready for job after graduation'] = df['reject_reason'] == 'You must be ready to take a job after graduation'
df['12 Pass'] = df['reject_reason']=='You must have passed the 12th grade'

#df['Unsucessfull Application'] = df['Ineligible']-df['Graduation'] -df['Age']-df['Not ready for job after graduation']

df['Eligible Application'] = df['status'].isin(['METTL_TO_BE_TAKEN',  'METTL_FAILED','ONBOARDING_COMPLETE', 'ONBOARDING_PENDING','METTL_DESCISION_PENDING', 'ONBOARDING_STARTED', 'METTL_PASSED','APPLICATION_CLOSED', 'METTL_STARTED','FELLOW'])
df['Profile Unfilled'] = df['status']=='PROFILE_UNFILLED'
#df['Eligible & Profile Unfilled'] = df['status'].isin(['METTL_TO_BE_TAKEN',  'METTL_FAILED','ONBOARDING_COMPLETE', 'ONBOARDING_PENDING','METTL_DESCISION_PENDING', 'ONBOARDING_STARTED', 'METTL_PASSED','APPLICATION_CLOSED', 'METTL_STARTED'])
df['Mettl to be taken'] = df['status']== 'METTL_TO_BE_TAKEN'
df['mettl_attempted']=df['status'].isin([ 'METTL_STARTED','METTL_DESCISION_PENDING', 'METTL_FAILED','METTL_PASSED','ONBOARDING_PENDING','ONBOARDING_STARTED','ONBOARDING_COMPLETE','FELLOW', 'APPLICATION_CLOSED'])
df['mettl_cleared']=df['status'].isin(['METTL_PASSED','ONBOARDING_PENDING','ONBOARDING_STARTED','ONBOARDING_COMPLETE','FELLOW', 'APPLICATION_CLOSED'])
df['verification_pending']=df['status']=='METTL_PASSED'
df['verification_complete']=df['status'].isin(['ONBOARDING_PENDING','ONBOARDING_STARTED','ONBOARDING_COMPLETE','FELLOW'])
#df['Mettl Started'] = df['status']== 'METTL_STARTED'
#df['Mettl Failed'] = df['status']== 'METTL_FAILED'
#df['Mettl Passed'] = df['status']== 'METTL_PASSED'
df['Mettl Decesion Pending'] = df['status']== 'METTL_DESCISION_PENDING'
df['Self onboarding pending']=(df['status'].isin(['ONBOARDING_PENDING','ONBOARDING_STARTED'])) & (pd.isna(df['onboarding_start_time']))
df['Self onboarding completed']=(df['status'].isin(['ONBOARDING_COMPLETE','FELLOW'])) & (pd.isna(df['onboarding_start_time']))
df['Assisted onboarding booked']= pd.isna(df['onboarding_start_time'])==False
df['Assisted onboarding pending']=(df['status'].isin(['ONBOARDING_PENDING','ONBOARDING_STARTED'])) & (pd.isna(df['onboarding_start_time'])==False)
df['Assisted onboarding completed']=(df['status'].isin(['ONBOARDING_COMPLETE','FELLOW'])) & (pd.isna(df['onboarding_start_time'])==False)
df['Application Closed'] = df['status']== 'APPLICATION_CLOSED'


# In[15]:


df['Ineligible'].value_counts()


# In[ ]:





# In[16]:


df  = df.groupby('date').agg({'Registration':sum,'Total Application':sum,'Eligible Application':sum,'Profile Complete':sum,'Profile Unfilled':sum,'Ineligible':sum,'Age':sum,'Graduation':sum,'Not ready for job after graduation':sum,'12 Pass':sum,'Mettl to be taken':sum,'mettl_attempted':sum,'mettl_cleared':sum,'verification_pending':sum,'verification_complete':sum,'Mettl Decesion Pending':sum,'Self onboarding pending':sum,'Self onboarding completed':sum,'Assisted onboarding booked':sum,'Assisted onboarding pending':sum,'Assisted onboarding completed':sum,'Application Closed':sum})


# In[17]:


#df.groupby('date').agg({'Registration':sum,'Total Application':sum,'Profile Complete':sum,'Profile Unfilled':sum,'Ineligible':sum,'Mettl to be taken':sum,'msat_attempted':sum,'msat_cleared':sum,'verification_pending':sum,'verification_complete':sum,'Mettl Decesion Pending':sum,'self_onboarding_pending':sum,})


# In[18]:


df.columns = [i.title() for i in df.columns]
df.columns = [i.replace("_"," ") for i in df.columns]
df = df.sort_index(ascending = False)


# In[19]:


df.columns


# In[20]:


#df['Unsuccessfull registration'] = df['Ineligible']-df['Age']-df['Graduation']-df['Not Ready For Job After Graduation']


# In[21]:


df = df[[
 'Total Application',
 'Eligible Application',
 'Profile Complete',
 'Profile Unfilled',
 'Ineligible',
 'Age',
 'Graduation',
 'Not Ready For Job After Graduation',
 '12 Pass',
 'Mettl To Be Taken',
 'Mettl Attempted',
 'Mettl Cleared',
 'Verification Pending',
 'Verification Complete',
 'Mettl Decesion Pending',
 'Self Onboarding Pending',
 'Self Onboarding Completed',
 'Assisted Onboarding Booked',
 'Assisted Onboarding Pending',
 'Assisted Onboarding Completed',
 'Application Closed'
 ]]


# In[22]:


df


# In[23]:


df2 = pd.read_sql("select profile.created_at from profile",con)


# In[24]:


df2


# In[25]:


df2['date'] = (pd.to_datetime(df2['created_at'])+pd.Timedelta(minutes = 330)).dt.date
df2 = df2['date'].value_counts().reset_index().rename(columns = {'index':'date','date':'Registration'})


# In[26]:


df2.head()


# In[27]:


df  = pd.merge(df2,df,on = ['date'],how = 'inner' )


# In[28]:


df.sort_values('date',ascending = False,inplace = True)
df


# In[29]:


ws = client.open_by_url("https://docs.google.com/spreadsheets/d/1YmI9DI1gJAEsGu5cbBSTJhxVa__iWs8RMzpqPTDohdc/edit?pli=1#gid=303745821").worksheet("New_Leakage_Sheet_FULL_Time")


# In[30]:


set_with_dataframe(ws,df,row = 10,col = 1,include_index = False)


# In[ ]:





# In[ ]:












#!/usr/bin/env python
# coding: utf-8

# In[1]:


from Ashok import *


# In[ ]:





# In[2]:


#df = pd.read_sql("select profile.created_at,profile.email,application.status,batch_campus.batch_id,profile.otp_verified,application.onboarding_session_one_time_link,application.reject_reason->'$."0"'  from  profile left join application on application.profile_slug = profile.slug left join batch_campus on application.batch_campus_id = batch_campus.id",con)


# In[3]:


#df= pd.read_sql("""select profile.created_at,profile.email,application.status,batch_campus.batch_id,profile.otp_verified,application.onboarding_session_one_time_link,JSON_UNQUOTE(JSON_EXTRACT(application.reject_reason, '$."0"')) as reject_reason  from  profile left join application on application.profile_slug = profile.slug left join batch_campus on application.batch_campus_id = batch_campus.id""",con)


# In[4]:


df = pd.read_sql("""select profile.created_at,profile.email,application.status,batch_campus.batch_id,profile.otp_verified,application.onboarding_session_one_time_link,JSON_UNQUOTE(JSON_EXTRACT(application.reject_reason, '$."0"')) as reject_reason,onboarding_session.start_date_time as onboarding_start_time  from  profile left join application on application.profile_slug = profile.slug left join batch_campus on application.batch_campus_id = batch_campus.id  left join  onboarding_session on onboarding_session.application_slug = application.slug left join batch on batch.id = batch_campus.batch_id left join course on course.id = batch.course_id where course.id = 2""",con)


# In[6]:


def fun(x):
    if len(str(x))>7:
        return str(x)
    else:
        return None
df['onboarding_start_time'] = df['onboarding_start_time'].apply(fun)


# In[7]:


#pd.read_sql("select JSON_EXTRACT(application.reject_reason, '$."0"')  from  profile left join application on application.profile_slug = profile.slug left join batch_campus on application.batch_campus_id = batch_campus.id WHERE JSON_EXTRACT(application.reject_reason, '$."0"') IS NOT NULL",con)


# In[8]:


df['date'] = (pd.to_datetime(df['created_at'])+pd.Timedelta(minutes = 330)).dt.date


# In[9]:


df['Total Application'] = df['status'].notnull()


# In[10]:


df['status'].unique()


# In[11]:


df['Eligible Application'] = df['status'].isin(['METTL_TO_BE_TAKEN',  'METTL_FAILED','ONBOARDING_COMPLETE', 'ONBOARDING_PENDING','METTL_DESCISION_PENDING', 'ONBOARDING_STARTED', 'METTL_PASSED','APPLICATION_CLOSED', 'METTL_STARTED'])


# In[12]:


df


# In[13]:


df['status'].unique()


# In[14]:


df['Registration'] = df['email'].notnull()
df['Profile Complete'] = df['otp_verified']==1.00
#df['Application & Profile Complete'] = df['Profile Complete'] + df['Total Application']
df['Ineligible'] = df['status']== 'INELIGIBLE'
df['Graduation'] = df['reject_reason'].isin(['To be eligible for this course you must be graduating in 2022','To be eligible for this course you must be graduating before 2022'])
df['Age'] = df['reject_reason']=='You must be between 18 and 28 years old for Software Development courses'
df['Not ready for job after graduation'] = df['reject_reason'] == 'You must be ready to take a job after graduation'
df['12 Pass'] = df['reject_reason']=='You must have passed the 12th grade'
#df['Unsucessfull Application'] = df['Ineligible']-df['Graduation'] -df['Age']-df['Not ready for job after graduation']

df['Eligible Application'] = df['status'].isin(['METTL_TO_BE_TAKEN',  'METTL_FAILED','ONBOARDING_COMPLETE', 'ONBOARDING_PENDING','METTL_DESCISION_PENDING', 'ONBOARDING_STARTED', 'METTL_PASSED','APPLICATION_CLOSED', 'METTL_STARTED','FELLOW'])
df['Profile Unfilled'] = df['status']=='PROFILE_UNFILLED'
#df['Eligible & Profile Unfilled'] = df['status'].isin(['METTL_TO_BE_TAKEN',  'METTL_FAILED','ONBOARDING_COMPLETE', 'ONBOARDING_PENDING','METTL_DESCISION_PENDING', 'ONBOARDING_STARTED', 'METTL_PASSED','APPLICATION_CLOSED', 'METTL_STARTED'])
df['Mettl to be taken'] = df['status']== 'METTL_TO_BE_TAKEN'
df['mettl_attempted']=df['status'].isin([ 'METTL_STARTED','METTL_DESCISION_PENDING', 'METTL_FAILED','METTL_PASSED','ONBOARDING_PENDING','ONBOARDING_STARTED','ONBOARDING_COMPLETE','FELLOW', 'APPLICATION_CLOSED'])
df['mettl_cleared']=df['status'].isin(['METTL_PASSED','ONBOARDING_PENDING','ONBOARDING_STARTED','ONBOARDING_COMPLETE','FELLOW', 'APPLICATION_CLOSED'])
df['verification_pending']=df['status']=='METTL_PASSED'
df['verification_complete']=df['status'].isin(['ONBOARDING_PENDING','ONBOARDING_STARTED','ONBOARDING_COMPLETE','FELLOW'])
#df['Mettl Started'] = df['status']== 'METTL_STARTED'
#df['Mettl Failed'] = df['status']== 'METTL_FAILED'
#df['Mettl Passed'] = df['status']== 'METTL_PASSED'
df['Mettl Decesion Pending'] = df['status']== 'METTL_DESCISION_PENDING'
df['Self onboarding pending']=(df['status'].isin(['ONBOARDING_PENDING','ONBOARDING_STARTED'])) & (pd.isna(df['onboarding_start_time']))
df['Self onboarding completed']=(df['status'].isin(['ONBOARDING_COMPLETE','FELLOW'])) & (pd.isna(df['onboarding_start_time']))
df['Assisted onboarding booked']= pd.isna(df['onboarding_start_time'])==False
df['Assisted onboarding pending']=(df['status'].isin(['ONBOARDING_PENDING','ONBOARDING_STARTED'])) & (pd.isna(df['onboarding_start_time'])==False)
df['Assisted onboarding completed']=(df['status'].isin(['ONBOARDING_COMPLETE','FELLOW'])) & (pd.isna(df['onboarding_start_time'])==False)
df['Application Closed'] = df['status']== 'APPLICATION_CLOSED'


# In[17]:


df = df.groupby('date').agg({'Registration':sum,'Total Application':sum,'Eligible Application':sum,'Profile Complete':sum,'Profile Unfilled':sum,'Ineligible':sum,'Age':sum,'Graduation':sum,'Not ready for job after graduation':sum,'12 Pass':sum,'Mettl to be taken':sum,'mettl_attempted':sum,'mettl_cleared':sum,'verification_pending':sum,'verification_complete':sum,'Mettl Decesion Pending':sum,'Self onboarding pending':sum,'Self onboarding completed':sum,'Assisted onboarding booked':sum,'Assisted onboarding pending':sum,'Assisted onboarding completed':sum,'Application Closed':sum})


# In[18]:


#df.groupby('date').agg({'Registration':sum,'Total Application':sum,'Profile Complete':sum,'Profile Unfilled':sum,'Ineligible':sum,'Mettl to be taken':sum,'msat_attempted':sum,'msat_cleared':sum,'verification_pending':sum,'verification_complete':sum,'Mettl Decesion Pending':sum,'self_onboarding_pending':sum,})


# In[19]:


df.columns = [i.title() for i in df.columns]
df.columns = [i.replace("_"," ") for i in df.columns]
df = df.sort_index(ascending = False)


# In[20]:


df.columns


# In[21]:


#df['Unsuccessfull registration'] = df['Ineligible']-df['Age']-df['Graduation']-df['Not Ready For Job After Graduation']


# In[23]:


df = df[[
 'Total Application',
 'Eligible Application',
 'Profile Complete',
 'Profile Unfilled',
 'Ineligible',
 'Age',
 'Graduation',
 'Not Ready For Job After Graduation',
 '12 Pass',   
 'Mettl To Be Taken',
 'Mettl Attempted',
 'Mettl Cleared',
 'Verification Pending',
 'Verification Complete',
 'Mettl Decesion Pending',
 'Self Onboarding Pending',
 'Self Onboarding Completed',
 'Assisted Onboarding Booked',
 'Assisted Onboarding Pending',
 'Assisted Onboarding Completed',
 'Application Closed'
 ]]


# In[24]:


df


# In[25]:


df2 = pd.read_sql("select profile.created_at from profile",con)


# In[26]:


df2


# In[27]:


df2['date'] = (pd.to_datetime(df2['created_at'])+pd.Timedelta(minutes = 330)).dt.date
df2 = df2['date'].value_counts().reset_index().rename(columns = {'index':'date','date':'Registration'})


# In[28]:


df2.head()


# In[29]:


df  = pd.merge(df2,df,on = ['date'],how = 'inner' )


# In[30]:


df.sort_values('date',ascending = False,inplace = True)
df


# In[31]:


df.columns


# In[32]:


ws = client.open_by_url("https://docs.google.com/spreadsheets/d/1YmI9DI1gJAEsGu5cbBSTJhxVa__iWs8RMzpqPTDohdc/edit?pli=1#gid=303745821").worksheet("New_Leakage_Sheet_Part_Time")


# In[33]:


set_with_dataframe(ws,df,row = 10,col = 1,include_index = False)


# In[ ]:




