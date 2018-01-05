
# coding: utf-8

# In[1]:

get_ipython().magic(u'load_ext autotime')


# In[2]:

import boto3
import json
import time

from etltools import s3


# In[3]:

aws_info = {
    'profile_name': 'sandbox',
}

uid = '1000101'


# # Test ConfigInfo

# In[11]:

from lambda_client.config_info import ConfigInfo

configs = ConfigInfo('lambda_client/lambda.cfg')

print configs.claims_bucket
print configs.claims_path
print
print configs.benefits_bucket
print configs.benefits_path
print
print configs.claims_table


# In[12]:

all_states = configs.all_states

print '{} states'.format(len(all_states))
print all_states


# # Test ClaimsClient

# In[ ]:

from lambda_client import ClaimsClient


# In[ ]:

# Test S3:
client = ClaimsClient(aws_info, 
                      s3_bucket=configs.claims_bucket,
                      s3_path=configs.claims_path)

client.get(uid)


# In[ ]:

# Test DynamoDB:
client = ClaimsClient(aws_info,
                      table_name=configs.claims_table)

client.get(uid)


# In[ ]:

# Test configuration file:
client = ClaimsClient(aws_info)

client.get(uid)


# # Test BenefitsClient

# In[13]:

from lambda_client import BenefitsClient


# In[14]:

client = BenefitsClient(aws_info)

print client.all_states


# In[5]:

plans = client._get_one_state('01')
print '{} plans read for state 01'.format(len(plans))

plans = client._get_one_state('04')
print '{} plans read for state 04'.format(len(plans))


# In[6]:

plans = client.get_by_state(['01', '04'])
print '{} plans read'.format(len(plans))


# In[7]:

plans = client.get_all()
print '{} plans read'.format(len(plans))


# In[8]:

# Compare the timing against reading the entire file:
from lambda_client.storage_utils import _read_json

session = boto3.Session(**aws_info)
resource = session.resource('s3')


# In[9]:

all_plans = _read_json('picwell.sandbox.medicare', 'ma_benefits/cms_2018_pbps_20171005.json', resource)

print '{} plans read'.format(len(plans))


# In[10]:

# Ensure that the same plans are read:
sort_key = lambda plan: plan['picwell_id']
assert sorted(all_plans, key=sort_key) == sorted(plans, key=sort_key)


# # Test Cost Breakdown

# In[ ]:

from test_client import LambdaCalculatorTestClient

client = LambdaCalculatorTestClient(aws_info)


# In[ ]:

pids = ['2820028008119', '2820088001036']

client.calculate_breakdown(uid, pids)


# # Test Batch Calculation

# In[ ]:

uids = s3.read_json('s3n://picwell.sandbox.medicare/samples/philadelphia-2015')

print '{} uids read'.format(len(uids))


# In[ ]:

uids[:10]


# In[ ]:

requests_per_second = 100

for uid in uids:
#     client.calculate_async(uid, months=['01'])
    client.calculate_async(uid)
    time.sleep(1.0/requests_per_second)  


# In[ ]:



