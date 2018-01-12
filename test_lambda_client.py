
# coding: utf-8

# In[1]:

get_ipython().magic(u'load_ext autotime')


# In[2]:

import boto3
import datetime
import logging
import json
import pickle
import pytest
import sys
import time

from etltools import s3

reload(logging)  # get around notebook problem


# In[3]:

logging.basicConfig(
    level=logging.INFO, 
    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler(filename='mylog.log', mode='w'),
#         logging.StreamHandler(sys.stdout),
#     ]
)


# In[4]:

# Test whether logging works:
logger = logging.getLogger()
logger.info('TEST INFO')


# In[5]:

aws_info = {
    'profile_name': 'sandbox',
}

uids = s3.read_json('s3://picwell.sandbox.medicare/samples/philadelphia-2015-1k-sample')
pids = ['2820028008119', '2820088001036']


# # Test ConfigInfo

# In[ ]:

from lambda_client.config_info import ConfigInfo

configs = ConfigInfo('lambda_client/lambda.cfg')

print configs.claims_bucket
print configs.claims_path
print
print configs.benefits_bucket
print configs.benefits_path
print
print configs.claims_table


# In[ ]:

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

people = client.get(uids[:1])
print 'claims of {} people retrieved'.format(len(people))


# In[ ]:

person = people[0]
print person.keys()
{
    'uid': person['uid'],
    'medical_claims': person['medical_claims'][:5]
}


# In[ ]:

# Test DynamoDB:
client = ClaimsClient(aws_info,
                      table_name=configs.claims_table)

people = client.get(uids[:1])
print 'claims of {} people retrieved'.format(len(people))


# In[ ]:

# Test configuration file and retrieving multiple people:
client = ClaimsClient(aws_info)

people = client.get(uids[:5])
print 'claims of {} people retrieved'.format(len(people))


# In[ ]:

# Let's try something larger:
people = client.get(uids)
print 'claims of {} people retrieved'.format(len(people))


# In[ ]:

# The object should not be pickled.
with pytest.raises(Exception, match='ClaimsClient object cannot be pickled.'):
    pickle.dumps(client)


# # Test BenefitsClient

# In[ ]:

from lambda_client import BenefitsClient


# In[ ]:

client = BenefitsClient(aws_info)

print client.all_states


# In[ ]:

plans = client._get_one_state('01')
print '{} plans read for state 01'.format(len(plans))

plans = client._get_one_state('04')
print '{} plans read for state 04'.format(len(plans))


# In[ ]:

plans = client.get_by_state(['01', '04'])
print '{} plans read'.format(len(plans))


# In[ ]:

plans = client.get_all()
print '{} plans read'.format(len(plans))


# In[ ]:

# Compare the timing against reading the entire file:
from lambda_client.shared_utils import _read_json

session = boto3.Session(**aws_info)
resource = session.resource('s3')


# In[ ]:

all_plans = _read_json('picwell.sandbox.medicare', 'ma_benefits/cms_2018_pbps_20171005.json', resource)

print '{} plans read'.format(len(plans))


# In[ ]:

# Ensure that the same plans are read:
sort_key = lambda plan: plan['picwell_id']
assert sorted(all_plans, key=sort_key) == sorted(plans, key=sort_key)


# In[ ]:

# The object should not be pickled.
with pytest.raises(Exception, match='BenefitsClient object cannot be pickled.'):
    pickle.dumps(client)


# # Test Cost Breakdown

# In[6]:

from lambda_client import CalculatorClient

client = CalculatorClient(aws_info)


# In[8]:

responses = client.get_breakdown(uids[:1], pids, verbose=True)

print '{} responses returned'.format(len(responses))
responses[0]


# In[ ]:

# It seems like
#
# (a) the time it takes to issue threads increases linearly; and
# (b) the time it takes for the last thread to finish is about 5 seconds all the time. 
for num_people in range(2, 15):
    start = datetime.datetime.now()
    responses = client.get_breakdown(uids[:num_people], pids)
    elapsed = (datetime.datetime.now() - start).total_seconds()

    print '{} responses returned ({} seconds)'.format(len(responses), elapsed)


# In[9]:

# Test recursive call:
responses = client.get_breakdown(uids[:10], pids, max_uids_to_calculate=10, verbose=True)

print '{} responses returned'.format(len(responses))


# In[14]:

responses = client.get_breakdown(uids[:10], pids, max_uids_to_calculate=1, max_lambda_calls=3, verbose=True)

print '{} responses returned'.format(len(responses))


# In[27]:

# Let's try something larger:
responses = client.get_breakdown(uids, pids, max_uids_to_calculate=1, max_lambda_calls=10)

print '{} responses returned'.format(len(responses))


# In[ ]:

# Run calculcations locally for comparison:
from lambda_package.calc.calculator import calculate_oop

claims_client = ClaimsClient(aws_info)
people = claims_client.get(uids)

benefits_client = BenefitsClient(aws_info)
plans = benefits_client.get_by_pid(pids)

costs = []
for person in people:
    claims = person['medical_claims']
    
    for plan in plans:
        cost = calculate_oop(claims, plan)
        cost.update({
            'uid': person['uid'],
            'picwell_id': str(plan['picwell_id']),
        })
        
        costs.append(cost)
        
print '{} costs calculated'.format(len(costs))


# In[38]:

# Try a sample size more relevant to commercial:
responses = client.get_breakdown(uids[:300], pids, max_uids_to_calculate=1, max_lambda_calls=10)

print '{} responses returned'.format(len(responses))


# # Test Batch Calculation

# In[ ]:

# uids = s3.read_json('s3n://picwell.sandbox.medicare/samples/philadelphia-2015')

# print '{} uids read'.format(len(uids))


# In[ ]:

# uids[:10]


# In[ ]:

# requests_per_second = 100

# for uid in uids:
# #     client.calculate_async(uid, months=['01'])
#     client.calculate_async(uid)
#     time.sleep(1.0/requests_per_second)  


# In[ ]:



