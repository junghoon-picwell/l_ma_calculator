
# coding: utf-8

# In[1]:

get_ipython().magic(u'load_ext autotime')


# In[2]:

import json
import time

from etltools import s3
from test_client import LambdaCalculatorTestClient


# In[3]:

client = LambdaCalculatorTestClient({'profile_name': 'sandbox'})


# # Test Cost Breakdown

# In[4]:

uid = '1000101'
pids = ['2820028008119', '2820088001036']

client.calculate_breakdown(uid, pids)


# # Test Batch Calculation

# In[5]:

uids = s3.read_json('s3n://picwell.sandbox.medicare/samples/philadelphia-2015')

print '{} uids read'.format(len(uids))


# In[6]:

uids[:10]


# In[ ]:

requests_per_second = 100

for uid in uids:
#     client.calculate_async(uid, months=['01'])
    client.calculate_async(uid)
    time.sleep(1.0/requests_per_second)  


# In[ ]:



