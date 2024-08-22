# Library
import pandas as pd
import random
from datetime import datetime as dt
import numpy as np
import pytz

# Load Data
lead_log = pd.read_csv('DE Dataset/lead_log.csv')
paid_transactions = pd.read_csv('DE Dataset/paid_transactions.csv')
referral_rewards = pd.read_csv('DE Dataset/referral_rewards.csv')
user_logs = pd.read_csv('DE Dataset/user_logs.csv')
user_referral_logs = pd.read_csv('DE Dataset/user_referral_logs.csv')
user_referral_statuses = pd.read_csv('DE Dataset/user_referral_statuses.csv')
user_referrals = pd.read_csv('DE Dataset/user_referrals.csv')


# Data Profiling
table_list = ['lead_log', 'paid_transactions', 'referral_rewards', 'user_logs', 'user_referral_logs', 'user_referral_statuses', 'user_referrals']

profiling_dict = {
    'table_name': list(),
    'column_name': list(),
    'column_data_type': list(),
    'sample_data': list(),
    'sample_data_type': list(),
    'number_of_unique': list(),
    'number_of_null': list(),
    'column_total_records': list(),
    'table_total_records': list()
}

for table_name in table_list:
    table = pd.read_csv(f'DE Dataset/{table_name}.csv')

    for column in table.columns:
        profiling_dict['table_name'].append(table_name)
        profiling_dict['column_name'].append(column)
        profiling_dict['column_data_type'].append(table[column].dtypes)
        profiling_dict['sample_data'].append(random.choice(list(table[table[column].notnull()][column])))
        profiling_dict['sample_data_type'].append(type(random.choice(list(table[table[column].notnull()][column]))))
        profiling_dict['number_of_unique'].append(table[column].nunique())
        profiling_dict['number_of_null'].append(sum(table[column].isna()))
        profiling_dict['column_total_records'].append(table[column].count())
        profiling_dict['table_total_records'].append(table.shape[0])

profiling_df = pd.DataFrame(profiling_dict)
profiling_df.to_csv('output_data/data_profiling_result.csv', index=False)


# Data Cleaning
# Based on data profiling, we have several things to do for data cleaning:
#     1. Convert column with datetime value to datetime column type
# 
# Notes: I maintain the null value in this stage because i think the null values is expected and have meaning in this stage

# 1. Convert timestamp column to datetime data type
lead_log['created_at'] = lead_log['created_at'].apply(lambda x: dt.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ'))
paid_transactions['transaction_at'] = paid_transactions['transaction_at'].apply(lambda x: dt.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ'))
referral_rewards['created_at'] = referral_rewards['created_at'].apply(lambda x: dt.strptime(x, '%Y-%m-%dT%H:%M:%SZ'))
user_logs['membership_expired_date'] = user_logs['membership_expired_date'].apply(lambda x: dt.strptime(x, '%Y-%m-%d'))
user_referral_logs['created_at'] = user_referral_logs['created_at'].apply(lambda x: dt.strptime(x, '%Y-%m-%dT%H:%M:%SZ'))
user_referral_statuses['created_at'] = user_referral_statuses['created_at'].apply(lambda x: dt.strptime(x, '%Y-%m-%dT%H:%M:%SZ'))
user_referrals['referral_at'] = user_referrals['referral_at'].apply(lambda x: dt.strptime(x, '%Y-%m-%dT%H:%M:%SZ'))
user_referrals['updated_at'] = user_referrals['updated_at'].apply(lambda x: dt.strptime(x, '%Y-%m-%dT%H:%M:%SZ'))


# Data Processing
# In this stage, i will do the process based on this order:
#     1. Join tables
#     2. Handling null
#     3. String adjustment
#     4. Time adjustment
#     5. Source category
#     6. Business logic

# 1. Join tables
## user_referrals <- user_referral_logs
user_referral_logs = user_referral_logs.sort_values('created_at', ascending=False).drop_duplicates(subset=['user_referral_id'], keep='first')[['id', 'user_referral_id', 'created_at', 'is_reward_granted']] # Get latest details data
user_referral_logs['reward_granted_at'] = user_referral_logs.apply(lambda x: x['created_at'] if x['is_reward_granted'] else None, axis=1)
user_referral_logs = user_referral_logs[['id', 'user_referral_id', 'reward_granted_at', 'created_at']]
user_referral_logs.columns = ['referral_details_id', 'referral_id', 'reward_granted_at', 'created_at_1']

merge_df = user_referrals.merge(user_referral_logs, how='left', on='referral_id')

## merge_df <- lead_logs
lead_log = lead_log.sort_values('id', ascending=False).drop_duplicates(subset=['lead_id'], keep='first')[['lead_id', 'source_category', 'timezone_location', 'created_at']] # Get latest status
temp_merge_df = user_referrals[user_referrals['referral_source'] == 'Lead'].merge(lead_log, how='left', left_on='referee_id', right_on='lead_id')[['referral_id', 'source_category', 'timezone_location', 'created_at']]
merge_df = merge_df.merge(temp_merge_df, how='left', on='referral_id')

## merge_df <- user_logs
user_logs = user_logs[user_logs['is_deleted'] == False].sort_values('id', ascending=False).drop_duplicates(subset=['user_id'], keep='first')[['user_id', 'name', 'phone_number', 'homeclub', 'timezone_homeclub', 'membership_expired_date']] # Get latest version user data
user_logs.columns = ['referrer_id', 'referrer_name', 'referrer_phone_number', 'referrer_homeclub', 'timezone_homeclub', 'membership_expired_date']
merge_df = merge_df.merge(user_logs, how='left', on='referrer_id')

## merge_df <- user_referral_statuses
user_referral_statuses = user_referral_statuses[['id', 'description']]
user_referral_statuses.columns = ['user_referral_status_id', 'referral_status']
merge_df = merge_df.merge(user_referral_statuses, how='left', on='user_referral_status_id')

## merge_df <- referral_rewards
referral_rewards['reward_value'] = referral_rewards['reward_value'].apply(lambda x: int(x.split(' ')[0]))
referral_rewards = referral_rewards[['id', 'reward_value']]
referral_rewards.columns = ['referral_reward_id', 'num_reward_days']
merge_df = merge_df.merge(referral_rewards, how='left', on='referral_reward_id')

## merge_df <- paid_transactions
paid_transactions = paid_transactions[['transaction_id', 'transaction_status', 'transaction_at', 'transaction_location', 'transaction_type', 'timezone_transaction']]
merge_df = merge_df.merge(paid_transactions, how='left', on='transaction_id')

# 2. Handling null
## Based on the join table result, no need to handling null values

# 3. String adjustment
string_adjustment_columns = ['referee_name', 'referral_source', 'source_category', 'referrer_name',
                             'referral_status', 'transaction_status', 'transaction_location', 'transaction_type']

for column in string_adjustment_columns:
    merge_df[column] = merge_df[column].apply(lambda x: x.title() if pd.notnull(x) else x)

# 4. Time adjustment
## create timezone column filled using timezone_location, timezone_transaction, and timezone_homeclub in order
## there is 5 row that do not have timezone value
merge_df['timezone'] = merge_df.apply(lambda x: x['timezone_location'] if pd.notnull(x['timezone_location']) else x['timezone_transaction'] if pd.notnull(x['timezone_transaction']) else x['timezone_homeclub'], axis=1)
merge_df = merge_df.drop(['timezone_location', 'timezone_transaction', 'timezone_homeclub'], axis=1)

def utc_to_local(dt_value, timezone_value):
    dt_utc = dt_value.replace(tzinfo=pytz.UTC)

    timezone = pytz.timezone(timezone_value)
    dt_local = dt_utc.astimezone(timezone)

    return dt_local

datetime_columns = ['referral_at', 'updated_at', 'reward_granted_at', 'transaction_at', 'created_at', 'created_at_1', 'membership_expired_date']

for column in datetime_columns:
    merge_df[column] = merge_df.apply(lambda x: utc_to_local(x[column], x['timezone']) if pd.notnull(x[column]) and pd.notnull(x['timezone']) else x[column], axis=1)

# 5. Source category
merge_df['referral_source_category'] = merge_df.apply(lambda x: 'Online' if x['referral_source'] == 'User Sign Up' else
                     'Offline' if x['referral_source'] == 'Draft Transaction' else
                     x['source_category'] if x['referral_source'] == 'Lead' else None, axis=1)

# 6. Business logic
## VALID - Condition 1
merge_df['is_business_logic_valid'] = merge_df.apply(lambda x: True if x['num_reward_days'] > 0 and
                     x['referral_status'] == 'Berhasil' and
                     pd.notnull(x['transaction_id']) and
                     x['transaction_status'] == 'Paid' and
                     x['transaction_type'] == 'New' and
                     (x['created_at'] < x['transaction_at'] or x['created_at_1'] < x['transaction_at']) and
                     (dt.strftime(x['created_at_1'], '%Y-%m') == dt.strftime(x['transaction_at'], '%Y-%m') if pd.notnull(x['created_at_1']) else dt.strftime(x['created_at'], '%Y-%m') == dt.strftime(x['transaction_at'], '%Y-%m') if pd.notnull(x['created_at']) else None) and
                     (x['membership_expired_date'] > x['transaction_at']) and
                     pd.notnull(x['referrer_name']) and
                     pd.notnull(x['reward_granted_at']) else False, axis=1)

## VALID - Condition 2
merge_df['is_business_logic_valid'] = merge_df.apply(lambda x: True if (x['referral_status'] == 'Menunggu' or x['referral_status'] == 'Tidak Berhasil') and
                     pd.notnull(x['num_reward_days']) == False else x['is_business_logic_valid'], axis=1)

## INVALID - Condition 1
merge_df['is_business_logic_valid'] = merge_df.apply(lambda x: False if x['num_reward_days'] > 0 and
                     x['referral_status'] != 'Berhasil' else x['is_business_logic_valid'], axis=1)

## INVALID - Condition 2
merge_df['is_business_logic_valid'] = merge_df.apply(lambda x: False if x['num_reward_days'] > 0 and
                     pd.notnull(x['transaction_id']) == False else x['is_business_logic_valid'], axis=1)

## INVALID - Condition 3
merge_df['is_business_logic_valid'] = merge_df.apply(lambda x: False if pd.notnull(x['num_reward_days']) == False and
                     pd.notnull(x['transaction_id']) and
                     x['transaction_status'] == 'Paid' and
                     (x['created_at'] < x['transaction_at'] or x['created_at_1'] < x['transaction_at']) else x['is_business_logic_valid'], axis=1)

## INVALID - Condition 4
merge_df['is_business_logic_valid'] = merge_df.apply(lambda x: False if x['referral_status'] == 'Berhasil' and
                     (pd.notnull(x['num_reward_days']) == False or x['num_reward_days'] == 0) else x['is_business_logic_valid'], axis=1)

## INVALID - Condition 5
merge_df['is_business_logic_valid'] = merge_df.apply(lambda x: False if (x['created_at'] > x['transaction_at'] or x['created_at_1'] > x['transaction_at']) else x['is_business_logic_valid'], axis=1)

# Output
output_columns = ['referral_details_id',
'referral_id',
'referral_source',
'referral_source_category',
'referral_at',
'referrer_id',
'referrer_name',
'referrer_phone_number',
'referrer_homeclub',
'referee_id',
'referee_name',
'referee_phone',
'referral_status',
'num_reward_days',
'transaction_id',
'transaction_status',
'transaction_at',
'transaction_location',
'transaction_type',
'updated_at',
'reward_granted_at',
'is_business_logic_valid']

merge_df = merge_df[output_columns]
merge_df.to_csv('output_data/data_processing_result.csv', index=False)