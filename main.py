import time

from classes import LeadsExportJob, ActivitiesExportJob
from config import *

chunks_params = [date_range_to_from_to_params(date_range, short_dt_format)
                 for date_range in chunk_list]

# Work with API
leads_export_jobs = [LeadsExportJob(marketo_client=mc,
                                    fields=leads_fields, date_filter_type=leads_date_filter_type,
                                    start_at=chunk_params['from'], end_at=chunk_params['to'],
                                    destination_bucket=destination_bucket, incremental=incremental,
                                    primary_key=leads_primary_key)
                     for chunk_params in chunks_params]

activities_export_jobs = [ActivitiesExportJob(marketo_client=mc,
                                              fields=activities_fields, activities_type_ids=activities_type_ids,
                                              start_at=chunk_params['from'], end_at=chunk_params['to'],
                                              destination_bucket=destination_bucket, incremental=incremental,
                                              primary_key=activities_primary_key)
                          for chunk_params in chunks_params]

export_jobs = leads_export_jobs + activities_export_jobs

for export_job in export_jobs:
    export_job.enqueue()

print("Export jobs created and enqueued.")

running_export_jobs = export_jobs
while len(running_export_jobs) > 0:
    for export_job in running_export_jobs:
        export_job.sync_state()
        if export_job.status == "Completed":
            export_job.write_file()
            running_export_jobs.remove(export_job)
    time.sleep(inter_query_sleep_time)
