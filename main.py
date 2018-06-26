import math as m
import time
from datetime import date

import pandas as pd
from keboola import docker

from classes import LeadsExportJob, ActivitiesExportJob
from marketorestpython.client import MarketoClient

# API specification based parameters
chunk_days = 7  # maximum is 31, but this reduces memory requirements
short_dt_format = '%Y-%m-%d'

# Load user params
cfg = docker.Config('/data/')
params = cfg.get_parameters()

# Authentication
munchkin_id = params['munchkin_id']
client_id = params['client_id']
client_secret = params['client_secret']

# Keboola config based parameters
days = params['days']
leads_fields = params['leads_fields']
activities_fields = params['activities_fields']

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


chunks_params = [date_range_to_from_to_params(date_range, short_dt_format)
                 for date_range in chunk_list]

# Work with API

mc = MarketoClient(munchkin_id, client_id, client_secret)

leads_export_jobs = [LeadsExportJob(marketo_client=mc, fields=leads_fields, filter_type='updatedAt',
                                    start_at=chunk_params['from'], end_at=chunk_params['to'])
                     for chunk_params in chunks_params]

activities_export_jobs = [ActivitiesExportJob(marketo_client=mc, fields=activities_fields, filter_type="createdAt",
                                              start_at=chunk_params['from'], end_at=chunk_params['to'])
                          for chunk_params in chunks_params]

export_jobs = leads_export_jobs + activities_export_jobs

for export_job in export_jobs:
    export_job.enqueue()

running_export_jobs = export_jobs
while len(running_export_jobs) > 0:
    for export_job in running_export_jobs:
        export_job.sync_state()
        if export_job.status not in ("Queued", "Processing"):
            export_job.write_file()
            running_export_jobs.remove(export_job)
    time.sleep(10)
