
# coding: utf-8

# In[1]:

get_ipython().magic(u'load_ext autotime')


# In[2]:

import boto3
import json
import pickle
import pytest
import time

from etltools import s3


# In[3]:

aws_info = {
    'profile_name': 'sandbox',
}

uids = s3.read_json('s3://picwell.sandbox.medicare/samples/philadelphia-2015-1k-sample')
pids = ['2820028008119', '2820088001036']


# # Test ConfigInfo

# In[4]:

from lambda_client.config_info import ConfigInfo

configs = ConfigInfo('lambda_client/lambda.cfg')

print configs.claims_bucket
print configs.claims_path
print
print configs.benefits_bucket
print configs.benefits_path
print
print configs.claims_table


# In[5]:

all_states = configs.all_states

print '{} states'.format(len(all_states))
print all_states


# # Test ClaimsClient

# In[6]:

from lambda_client import ClaimsClient


# In[7]:

# Test S3:
client = ClaimsClient(aws_info, 
                      s3_bucket=configs.claims_bucket,
                      s3_path=configs.claims_path)

client.get(uids[0])


# In[8]:

# Test DynamoDB:
client = ClaimsClient(aws_info,
                      table_name=configs.claims_table)

client.get(uids[0])


# In[9]:

# Test configuration file:
client = ClaimsClient(aws_info)

client.get(uids[0])


# In[10]:

# The object should not be pickled.
with pytest.raises(Exception, match='ClaimsClient object cannot be pickled.'):
    pickle.dumps(client)


# # Test BenefitsClient

# In[11]:

from lambda_client import BenefitsClient


# In[12]:

client = BenefitsClient(aws_info)

print client.all_states


# In[13]:

plans = client._get_one_state('01')
print '{} plans read for state 01'.format(len(plans))

plans = client._get_one_state('04')
print '{} plans read for state 04'.format(len(plans))


# In[14]:

plans = client.get_by_state(['01', '04'])
print '{} plans read'.format(len(plans))


# In[15]:

plans = client.get_all()
print '{} plans read'.format(len(plans))


# In[16]:

# Compare the timing against reading the entire file:
from lambda_client.storage_utils import _read_json

session = boto3.Session(**aws_info)
resource = session.resource('s3')


# In[17]:

all_plans = _read_json('picwell.sandbox.medicare', 'ma_benefits/cms_2018_pbps_20171005.json', resource)

print '{} plans read'.format(len(plans))


# In[18]:

# Ensure that the same plans are read:
sort_key = lambda plan: plan['picwell_id']
assert sorted(all_plans, key=sort_key) == sorted(plans, key=sort_key)


# In[19]:

# The object should not be pickled.
with pytest.raises(Exception, match='BenefitsClient object cannot be pickled.'):
    pickle.dumps(client)


# # Test Cost Breakdown

# In[20]:

from lambda_client import CalculatorClient

client = CalculatorClient(aws_info)


# In[21]:

responses = client._get_one_breakdown(uids[0], pids, '01')

print '{} responses returned'.format(len(responses))
responses[0]


# In[22]:

responses = client.get_breakdown(uids[:3], pids)

print '{} responses returned'.format(len(responses))


# In[23]:

# Let's try something larger:
responses = client.get_breakdown(uids, pids)

print '{} responses returned'.format(len(responses))


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



