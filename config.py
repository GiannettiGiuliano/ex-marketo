import math as m
from datetime import date

import pandas as pd
from keboola import docker

from marketorestpython.client import MarketoClient

# API specification based parameters
chunk_days = 7  # maximum is 31, but this reduces memory requirements
short_dt_format = '%Y-%m-%d'
inter_query_sleep_time = 5

# Load user paramsq
cfg = docker.Config('/data/')
params = cfg.get_parameters()

# Keboola config based parameters
destination_bucket = params.get('destination_bucket')
incremental = params.get('incremental')

# Authentication
munchkin_id = params.get('munchkin_id')
client_id = params.get('client_id')
client_secret = params.get('client_secret')

# Data params
days = params.get('days')

leads_date_filter_type = params.get('leads_date_filter_type')
leads_fields = params.get('leads_fields')

activities_type_ids = params.get('activities_type_ids')
activities_fields = params.get('activities_fields')

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
