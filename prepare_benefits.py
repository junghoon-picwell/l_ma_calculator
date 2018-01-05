
# coding: utf-8

# In[1]:

get_ipython().magic(u'load_ext autotime')


# In[2]:

import json
import os

from etltools import s3


# In[3]:

benefit_file = 's3://picwell.sandbox.medicare/ma_benefits/cms_2018_pbps_20171005.json'
# benefit_dir = 's3://picwell.sandbox.analytics/junghoon/lambda_calculator_benefits'
benefit_dir = '/Users/junghoonlee/code/TEMP/EMP/lambda_calculator_benefits'


# In[4]:

plans = s3.read_json(benefit_file)

print '{} plans read'.format(len(plans))


# In[5]:

# Identify all the FIPS codes used:
states = set(str(plan['picwell_id'])[-2:] for plan in plans)

print '{} states identified'.format(len(states))
# states


# In[6]:

# Break up the plans by state and save:
plans_by_state = {}
for state in states:
    filtered_plans = filter(lambda plan: str(plan['picwell_id'])[-2:] == state, plans)
    plans_by_state[state] = filtered_plans
    
print 'total {} plans'.format(sum(len(v) for v in plans_by_state.itervalues()))


# In[7]:

for state, filtered_plans in plans_by_state.iteritems():
    print '{}: {} plans'.format(state, len(filtered_plans))


# In[8]:

# Write to s3:
for state, filtered_plans in plans_by_state.iteritems():
    file_name = os.path.join(benefit_dir, '{}.json'.format(state))
    s3.write_json(filtered_plans, file_name)


# In[ ]:



