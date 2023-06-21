#!/usr/bin/env python
# coding: utf-8

# # Importing Library 

# In[1]:


import pandas as pd
import numpy as np
from datetime import datetime as dt


# # Importing Datasets

# In[2]:


User_game_play = pd.read_csv("User Game play.csv")
Deposit_data = pd.read_csv('Deposit Data.csv')
Withrawl_data = pd.read_csv("Withrawl Data.csv")


# # Merging 3 data set on the basis of User Id

# In[3]:


#Merging all the Three sheets data on the basis of User ID as it is a primary Keys
User_Withrawl = (pd.merge(left=User_game_play,right=Withrawl_data,left_on='User ID',right_on='User Id',
                          how='left',copy=False,suffixes=('_User', '_Withrawl')))

User_Withrawl.drop(columns = ['User Id'],inplace=True)

df = (pd.merge(left=User_Withrawl,right=Deposit_data,left_on='User ID',right_on='User Id',
               how='left',copy=False,suffixes=('_Withrawl', '_Deposit')))

df.drop(columns=['User Id','Datetime_Withrawl','Datetime'],inplace=True)

#Renaming Some column after dropping some column which are common after merging
df = df.rename(columns={
          'Datetime_User':'User Date',
          'Amount_Withrawl':'Withraw Amount',
          'Amount_Deposit':'Deposit Amount'})


# # Exploring Data

# In[4]:


#Checking shape of data set after merging
df.shape
# 116021389


# In[5]:


#Checking Data set for each columns
df.dtypes


# In[6]:


#As we can see DataType of User Date is object
#We have to convert it into Datetime
df['User Date'] = pd.to_datetime(df['User Date'])
    
df.dtypes


# In[7]:


#No of Distinct User
df['User ID'].nunique()


# In[8]:


#Checking for Null value
df.isna().sum()


# In[9]:


#Filling of null value
df.fillna(0,inplace=True)


# In[10]:


#Creating hour column from extracting hour,Month,Day from datetime
df['Hour']  = df['User Date'].dt.hour
df['Month'] = df['User Date'].dt.month_name()
df['Day'] = df['User Date'].dt.day


# # Part A - Calculating loyalty points

# #### Grouping Dataset into two group S1 & S2 as specified

# In[11]:


# On each day, there are 2 slots for each of which the loyalty points are to be calculated:
# S1 from 12am to 12pm 
# S2 from 12pm to 12am"
'''This functoin will divide our dataset Hour column into two group as 
    S1 from 12am to 12pm S2 from 12pm to 12am"'''

df['group'] = df['Hour'].map(lambda x: 'S2' if 12 <= x <= 24 else 'S1')


# In[12]:


'''Grouping & Agrregating our dataset and renaming it as 
final for calculating Loyalty points'''

final = (df[['User ID', 'Games Played','Withraw Amount',
       'Deposit Amount','Month', 'Day', 'group']]
         .groupby(['User ID','Month','Day', 'group'])
         [['Withraw Amount','Deposit Amount','Games Played']]
         .aggregate(Withraw_Count= ('Withraw Amount','count'),
                    Withraw_Amount= ('Withraw Amount','sum'),
                    Deposit_Count= ('Deposit Amount','count'),
                    Deposit_Amount= ('Deposit Amount','sum'),
                    Games_Played=('Games Played','sum')))


# In[13]:


final.reset_index(inplace=True)


# In[14]:


def max_value(x):
    '''This function will return max value of 
    diff(Deposit count & Withrawl count) or 0'''
    return max(x['Deposit_Count'] - x['Withraw_Count'],0)

final['Loyalty points'] = (0.01*final['Deposit_Amount'] + 0.005*final['Withraw_Amount'] 
                           + 0.001*final.apply(max_value,axis=1) + 0.2 * final['Games_Played'])


# #### 1. Find Playerwise Loyalty points earned by Players in the following slots:-
# 

# In[15]:


#2nd October Slot S1 (Loyalty Points)
final[((final['Month']=='October')&(final['Day']==2)
       &(final['group']=='S1'))][['User ID','Loyalty points']]


# In[16]:


#16th October Slot S2(Loyalty Points)
final[((final['Month']=='October')&(final['Day']==16)
       &(final['group']=='S2'))][['User ID','Loyalty points']]


# In[17]:


#18th October Slot S1 (Loyalty Points)
final[((final['Month']=='October')&(final['Day']==18)
       &(final['group']=='S1'))][['User ID','Loyalty points']]


# In[18]:


#26th October Slot S2 (Loyalty Points)
final[((final['Month']=='October')&(final['Day']==26)
       &(final['group']=='S2'))][['User ID','Loyalty points']]


# #### 2. Calculate overall loyalty points earned and rank players on the basis of loyalty points in the month of October. 
# Note: In case of tie, number of games played should be taken as the next criteria for ranking.

# In[19]:


a = (final[final['Month']=='October']
     [['User ID','Loyalty points','Games_Played']]
     .groupby('User ID')
     .sum())
a.sort_values(by=['Loyalty points','Games_Played'],axis=0,ascending=False,inplace=True)
a.reset_index(inplace=True)
a['Rank'] = a.index+1
a[['User ID','Loyalty points','Rank']]


# #### 3. What is the average deposit amount?

# In[20]:


amt = round(final['Deposit_Amount'].mean(),3)
print(f'Average Deposit Amount is :{amt}')


# #### 4. What is the average deposit amount per user in a month?

# In[21]:


(final[['User ID','Month','Deposit_Amount']]
 .groupby(['Month','User ID'])
 .mean().reset_index())


# #### 5. What is the average number of games played per user?"

# In[22]:


(final[['User ID','Games_Played']]
 .groupby('User ID')
 .mean().reset_index())


# # Part B - How much bonus should be allocated to leaderboard players?

# ####  top 50 player for Every MONTH

# In[23]:


a = (final[['User ID', 'Month','Loyalty points']]
 .groupby(['User ID', 'Month']).sum()
 .sort_values(['Loyalty points','Month'],ascending=False).reset_index())

month = ['January','February', 'March','April','May','June','July',
       'August', 'September','October','November','December']

def give_top_50_every_month(x):
    '''This function will return top 50 User
    for each & every Month on basis of Top Loyalty point & also a dictionary(User)
    which consist of Top 50 User Id in every months'''
    User = {}
    for mnth in month:
        k = ((x[x['Month']==mnth][['User ID','Loyalty points']]
         .sort_values('Loyalty points',ascending=False)[:50])
             .reset_index().drop(columns='index'))
        k.index.names = ['Rank']
        k.reset_index(inplace=True)
        k['Rank'] = k['Rank']+1
        k.set_index('Rank',inplace=True)
        User[mnth] = k['User ID'].values
        print(f'Top 50 for {mnth} Month are :----')
        print()
        print(k.reset_index())   
        print('**'*20)
        
    return User
    
    
top_50_User = give_top_50_every_month(a)
top_50_User


# #### Money distribution for top 50 Loyal Players

# - Money can be distributed on the basis of Weihted Loyalty points 
# - person with highest Loyalty point will be having highest Rank & will haver higher weight
# - Peson with highest weight will get higest money

# In[25]:


amount  = 50000
for mn in month:
    k1 = (a[a['Month']==mn][['User ID','Loyalty points']]
         .sort_values('Loyalty points',ascending=False)[:50])
    k1['Weights'] = k1['Loyalty points']/np.sum(k1['Loyalty points'])
    k1['Bonus Money'] = round(k1['Weights'] * amount,2)
    print(f'Money distribution in {mn} Month...')
    print()
    print(k1)
    print('**'*20)


# # Part C

# #### Would you say the loyalty point formula is fair or unfair?

# **Loyalty Point = (0.01 * deposit) + (0.005 * Withdrawal amount) + (0.001 * (maximum of (#deposit - #withdrawal) or 0)) + (0.2 * Number of games played)**
# 
# - Loyalty point formula was fair but weight provided for Deposit & game played were less

# #### Can you suggest any way to make the loyalty point formula more robust?

# - We cold have increased the weight for Deposit amount to 5% bcoz higher weight will signifies the Player is willing to play more by Depositing Amount for more Games & it will be more loyal to Game Platform
# - Game Played weight should also be increased bcoz it signifies the User is willing to play More that mean there is a Chance for Game Platform to earn more as there will me more Transaction to the company in form of deduction from (Withrawl & Deposit Charges) so it will be win-win situation for both company & User
# 

# In[ ]:




