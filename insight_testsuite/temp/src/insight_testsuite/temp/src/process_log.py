# coding: utf-8

#! /usr/bin/env python
from __future__ import division
import sys
import pandas as pd
import numpy as np
import re
import time
from datetime import datetime
#import matplotlib.pyplot as plt
#import seaborn as sns
#sns.set_style('whitegrid')
#get_ipython().magic(u'matplotlib inline')

#defining input and output variables
log             = sys.argv[1] # input data name
hosts           = sys.argv[2] # output data name
hours           = sys.argv[3] # output data name
resources       = sys.argv[4] # output data name
blocked         = sys.argv[5] # output data name
cols=['Host','Time','Request','Code','Bytes'] # for the dataframe
regex = '(.*?) - - \[(.*?)\] "(.*?)" (\d+) (.*)' # regular expression to parse from the data
data =[] # define a variable to collect the read data
with open(log) as f: #opening input data
    for line in f: 
        data.append(re.match(regex, line).groups()) # reading line by line matching the regular expression
df=pd.DataFrame(data,columns=cols) # creating the dataframe for all the lines data
df = df[cols] # ordering columns
df = df.dropna(how='all') # drop if all columns are nan value
df['Bytes']=df['Bytes'].replace(to_replace=['-'], value=['0'], regex=True ) # replace '-' by '0'.

#Feature 1:
#List the top 10 most active host/IP addresses that have accessed the site.
df['Host'].value_counts().iloc[:20].to_csv(hosts, encoding='utf-8')

df['Bytes']=df['Bytes'].astype(str).astype(int)
#Feature 2:
#Identify the 10 resources that consume the most bandwidth on the site
A = pd.DataFrame(df['Bytes'].groupby(df['Request']).sum())
A['count'] = pd.DataFrame(df['Request'].groupby(df['Request']).count())
A=A.sort_values(by=['Bytes','count'],ascending=[False,False])
x=pd.DataFrame(A.index[:20])
x=x.replace(to_replace=['GET'], value=[''], regex=True )
x.to_csv(resources, encoding='utf-8',index=None,header=False)

df[['Timestamp','Timezone']] = pd.DataFrame(df.Time.str.split(' -').tolist()) # spliting time columns to timestamp and timezone
df['Timestamp'] = pd.to_datetime(df.Timestamp, format='%d/%b/%Y:%H:%M:%S')  #Time object to timestamp conversion
df['deltatime']=((df['Timestamp']- df['Timestamp'].shift()).fillna(0)).astype('timedelta64[s]') # time event differences
df['sumtime']=df['deltatime'].astype('int').cumsum() # summing time difference between events (duration of events from start to the time we are in)
# Feature 3:
#  List the top 10 busiest (or most frequently visited) 60-minute periods
busy3=[]
busy=[] # creating empty variables
busy1=[]
t_window=60 #time window in second
duration = range(0,df.sumtime.max(),60*t_window) # creating time window in 1 hour intervals to check busy time 
for j in range(len(duration)-1):
    A = df[(df.sumtime > duration[j]) & (df.sumtime <= duration[j+1])] 
    x = pd.DataFrame(A['Time'].groupby(A['Time']).count())
    x=x.sort_values(by='Time', ascending=False)
    busy.append(x.iloc[0].values)
    busy1.append(A['Time'].iloc[0])
busy3=pd.DataFrame(busy,columns=['Count'])
busy3['Time'] = busy1
busy3=busy3.sort_values(by='Count', ascending=False)
busy3[['Time','Count']].iloc[:20].to_csv(hours, encoding='utf-8',columns=None,header=False,index=None)
'''
Feature 4:
Detect patterns of three failed login attempts from the same IP address over 20 seconds 
so that all further attempts to the site can be blocked for 5 minutes. Log those possible security breaches.
'''
#function to find if two events are consquetively occured, it increases by one if the events ocurred one next to other. If two events oucured,
#the first one become 1 and the next become 2, and the third become 3. So, I can select events occured at the third as 3 value filter.
def rolling_count(val): 
    if val == rolling_count.previous:
        rolling_count.count +=1
    else:
        rolling_count.previous = val
        rolling_count.count = 1
    return rolling_count.count
rolling_count.count = 0 #static variable
rolling_count.previous = None #static variable
df['count'] = df['Code'].apply(rolling_count) # applying the function on HTPP reply code.
# selecting all events occured consquetively three times and are not successful login (HTTP reply code not 200) 
blocked_site = df[(df['count']==3) & (df['Code']!='200')] # this only selects three consquetive failed attempt but does not check the same 
# Ip address or time difference b/n the first third attempt.
Uniqueip=[]
difftime =[]
for i in range(len(blocked_site)):
    A = df.iloc[blocked_site.index[i]-2:blocked_site.index[i]+1] # finding the three consquetive failed attempts
    Uniqueip.append(len(A.Host.unique())) # check if its unique ip (1 : unique, more than 1: multiple ip addresses)
    difftime.append((A.Timestamp.iloc[-1] - A.Timestamp.iloc[0]).total_seconds()) # find total time diffence b/n the first and the third failed attempt.
blocked_site = blocked_site.copy()
blocked_site.loc[:,'Uniqueip']=Uniqueip
blocked_site.loc[:,'difftime']=difftime
blocked_site=blocked_site[(blocked_site['Uniqueip']==1) & (blocked_site['difftime'] <= 20.0)] #selecting both unique ip address and 200 time duration 
blocked_site=blocked_site[['Host','Time','Request','Code','Bytes']]
blocked_site.to_csv(blocked,sep=b'\t',encoding='utf-8',index=None, header=False)
