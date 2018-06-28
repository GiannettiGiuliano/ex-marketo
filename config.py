import math as m
from datetime import date

import pandas as pd
from keboola import docker

from marketorestpython.client import MarketoClient

# Load user paramsq
cfg = docker.Config('/data/')
params = cfg.get_parameters()

# Driver and API parameters
chunk_days = params.get('chunk_days', 7)  # maximum is 31, but the default of 7 reduces memory requirements
short_dt_format = '%Y-%m-%d'
inter_query_sleep_time = params.get('inter_query_sleep_time', 5)

# Keboola config based parameters
destination_bucket = params['destination_bucket']
incremental = params['incremental']

# Authentication
munchkin_id = params['munchkin_id']
client_id = params['client_id']
client_secret = params['client_secret']

# Data params
days = params['days']

leads_date_filter_type = params.get('leads_date_filter_type', "updatedAt")
leads_fields = params['leads_fields']
leads_primary_key = params.get('leads_primary_key')

activities_type_ids = params.get('activities_type_ids')
activities_fields = params['activities_fields']
activities_primary_key = params.get('activities_primary_key')

# Initialize MarketoClient
mc = MarketoClient(munchkin_id, client_id, client_secret)

# Date chunk setup
start_day = date.today() - pd.Timedelta(days=days - 1)

full_chunks = m.floor(days / chunk_days)
leftover_days = days % chunk_days

chunk_list = []

chunk_start = start_day

for i in range(int(full_chunks)):
    chunk_range = pd.date_range(chunk_start, periods=chunk_days)
    chunk_list.append(chunk_range)
    chunk_start += pd.Timedelta(days=chunk_days)

if leftover_days > 0:
    chunk_range = pd.date_range(chunk_start, periods=leftover_days)
    chunk_list.append(chunk_range)


def date_range_to_from_to_params(date_range, dt_format):
    date_list = date_range.tolist()
    return {'from': date_list[0].strftime(dt_format),
            'to': date_list[-1].strftime(dt_format)}
