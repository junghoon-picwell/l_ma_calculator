
# coding: utf-8

# In[1]:

get_ipython().magic(u'load_ext autotime')


# In[2]:

from etltools import s3
from lambda_client import (
    CalculatorClient,
    run_batch_on_schedule,
)
from lambda_client.config_info import ConfigInfo

aws_info = {
    'profile_name': 'sandbox',
}


# In[3]:

configs = ConfigInfo('lambda_client/lambda.cfg')

# 06: CA
# 36: NY
print configs.all_states

states = ['01', '04', '05', '06', '36']


# In[4]:

uids = s3.read_json('s3n://picwell.sandbox.medicare/samples/philadelphia-2015')

print '{} uids read'.format(len(uids))


# In[5]:

client = CalculatorClient(aws_info)

# This only runs for large enough Lambda, e.g. 512 MB, and fails for 256 MB Lambda. 
# Memory determines how fast one can run.
response = client.run_batch(uids[:20], months=['01'], states=states)

print response


# In[6]:

# Not sure why an adjustment factor is needed:
factor = 6

responses = run_batch_on_schedule(lambda uids: client.run_batch(uids, months=['01'], states=states),
                                  uids[:10000], num_writes_per_uid=5, mean_runtime=30, 
                                  min_writes=100*factor, max_writes=10000*factor, verbose=True)


# In[7]:

print 'number of responses: {}'.format(len(responses))

writes = 0
for response in responses:
    writes += response[1]
print 'number of writes: {}'.format(writes)

