
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

from lambda_client import (
    ClaimsClient,
    BenefitsClient,
    CalculatorClient,
)

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

# Let's try something larger:
people = client.get(uids)
print 'claims of {} people retrieved'.format(len(people))


# In[ ]:

# Test DynamoDB:
client = ClaimsClient(aws_info,
                      table_name=configs.claims_table)

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

# Let's try something larger:
people = client.get(uids)
print 'claims of {} people retrieved'.format(len(people))


# In[ ]:

# Test configuration file and retrieving multiple people:
client = ClaimsClient(aws_info)

people = client.get(uids[:5])
print 'claims of {} people retrieved'.format(len(people))


# In[ ]:

# The object should not be pickled.
with pytest.raises(Exception, match='ClaimsClient object cannot be pickled.'):
    pickle.dumps(client)


# # Test BenefitsClient

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

# In[ ]:

client = CalculatorClient(aws_info)


# In[ ]:

responses = client.get_breakdown(uids[:1], pids, verbose=True)

print '{} responses returned'.format(len(responses))
responses[0]


# In[ ]:

responses = client.get_breakdown(uids[:1], pids, use_s3_for_claims=False, verbose=True)

print '{} responses returned'.format(len(responses))


# In[ ]:

# Test recursive call:
responses = client.get_breakdown(uids[:10], pids, max_calculated_uids=10)

print '{} responses returned'.format(len(responses))


# In[ ]:

responses = client.get_breakdown(uids[:10], pids, max_lambda_calls=2)

print '{} responses returned'.format(len(responses))


# In[ ]:

responses = client.get_breakdown(uids[:10], pids)

print '{} responses returned'.format(len(responses))


# In[ ]:

# Check whether DynamoDB reduces latency:
responses = client.get_breakdown(uids[:10], pids, use_s3_for_claims=False, max_calculated_uids=10)

print '{} responses returned'.format(len(responses))


# In[ ]:

# Let's try something larger:
responses = client.get_breakdown(uids, pids)

print '{} responses returned'.format(len(responses))


# In[ ]:

responses = client.get_breakdown(uids, pids, use_s3_for_claims=False)

print '{} responses returned'.format(len(responses))


# In[ ]:

# Runs into memory issue if all 1000 people are calculated once:
responses = client.get_breakdown(uids, pids, use_s3_for_claims=False, max_calculated_uids=100, max_lambda_calls=10)

print '{} responses returned'.format(len(responses))


# In[ ]:

from lambda_package.calc.calculator import calculate_oop

def run_locally(people, plans, oop_only):
    costs = []
    
    for person in people:
        claims = person['medical_claims']
    
        for plan in plans:

            cost = calculate_oop(claims, plan)
            if oop_only:
                cost = {
                    'oop': cost['oop']
                }

            cost.update({
                'uid': person['uid'],
                'picwell_id': str(plan['picwell_id']),
            })

            costs.append(cost)
    
    return costs


# In[ ]:

# Run calculcations locally for comparison:
# claims_client = ClaimsClient(aws_info, 
#                              s3_bucket=configs.claims_bucket,
#                              s3_path=configs.claims_path)
claims_client = ClaimsClient(aws_info, 
                             table_name=configs.claims_table)
people = claims_client.get(uids)

benefits_client = BenefitsClient(aws_info)
plans = benefits_client.get_by_pid(pids)

costs = run_locally(people, plans, False)

print '{} costs calculated'.format(len(costs))


# In[ ]:

# benefits_client = BenefitsClient()
benefits_client = BenefitsClient(aws_info)
plans_CA = benefits_client.get_by_state(['06'])
pids_CA = [plan['picwell_id'] for plan in plans_CA]

print '{} plans identified'.format(len(pids_CA))


# In[ ]:

# Try a sample size more relevant to commercial:
responses = client.get_oop(uids[:300], pids_CA)

print '{} responses returned'.format(len(responses))


# In[ ]:

responses = client.get_oop(uids[:300], pids_CA, use_s3_for_claims=False)

print '{} responses returned'.format(len(responses))


# In[ ]:

# Increasing the amount of computation at the terminal nodes increases the time. This is probably
# because there are many plans.
responses = client.get_oop(uids[:300], pids_CA, use_s3_for_claims=False, max_calculated_uids=3)

print '{} responses returned'.format(len(responses))


# In[ ]:

claims_client = ClaimsClient(aws_info, 
                             table_name=configs.claims_table)
people = claims_client.get(uids[:300])

benefits_client = BenefitsClient(aws_info)
plans = benefits_client.get_by_pid(pids_CA)

costs = run_locally(people, plans, True)

print '{} costs calculated'.format(len(costs))


# # Test Batch Calculation

# In[6]:

uids = s3.read_json('s3n://picwell.sandbox.medicare/samples/philadelphia-2015')

print '{} uids read'.format(len(uids))


# In[7]:

client = CalculatorClient(aws_info)


# In[8]:

response = client.run_batch(uids[:1], months=['01', '02', '03'], states=['01', '06'], verbose=True)

print response


# In[17]:

response = client.run_batch(uids[:2], states=['01', '06', '36'], max_calculated_uids=2)

print response


# In[24]:

# Test recursive call:
response = client.run_batch(uids[:2], states=['01', '06', '36'])

print response


# In[29]:

# This only runs for large enough Lambda, e.g. 512 MB, and fails for 256 MB Lambda. 
# Memory determines how fast one can run.
response = client.run_batch(uids[:100], months=['01'], states=['01', '06', '36'])

print response


# In[ ]:



