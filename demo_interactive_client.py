
# coding: utf-8

# In[1]:

get_ipython().magic(u'load_ext autotime')


# In[2]:

from etltools import s3
from lambda_client import (
    ClaimsClient,
    BenefitsClient,
    CalculatorClient,
    run_batch_on_schedule,
)
from lambda_client.config_info import ConfigInfo

aws_info = {
    'profile_name': 'sandbox',
}


# In[3]:

uids = s3.read_json('s3://picwell.sandbox.medicare/samples/philadelphia-2015-1k-sample')

print '{} UIDs read'.format(len(uids))


# In[4]:

benefits_client = BenefitsClient(aws_info)
plans = benefits_client.get_by_state(['06'])  # CA
pids = [plan['picwell_id'] for plan in plans]

print '{} plans identified'.format(len(pids))


# In[5]:

configs = ConfigInfo('lambda_client/lambda.cfg')


# # Breakdown Demo

# In[6]:

client = CalculatorClient(aws_info)


# In[7]:

responses = client.get_breakdown(uids, pids[:2])

print '{} responses returned'.format(len(responses))


# In[8]:

responses = client.get_breakdown(uids, pids[:2], use_s3_for_claims=False, max_calculated_uids=100)

print '{} responses returned'.format(len(responses))


# # Scenario Similar to Commercial

# In[9]:

responses = client.get_oop(uids[:300], pids, use_s3_for_claims=False)

print '{} responses returned'.format(len(responses))


# # S3 versus DynamoDB as Claims Storage

# In[10]:

# Use S3:
claims_client = ClaimsClient(aws_info, 
                             s3_bucket=configs.claims_bucket,
                             s3_path=configs.claims_path)
people = claims_client.get(uids[:300])

print 'claims for {} people read'.format(len(people))


# In[11]:

person = people[0]
print person.keys()
{
    'uid': person['uid'],
    'medical_claims': person['medical_claims'][:5]
}


# In[12]:

# Use DynamoDB:
claims_client = ClaimsClient(aws_info, 
                             table_name=configs.claims_table)
people = claims_client.get(uids[:300])

print 'claims for {} people read'.format(len(people))


# In[ ]:



