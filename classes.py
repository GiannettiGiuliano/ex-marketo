import json
import os
from abc import ABC
from typing import List

from marketorestpython.client import MarketoClient


class ExportJob(ABC):
    entity = None
    manifesto_file_written = False

    @property
    def id(self):
        return self.state['exportId']

    @property
    def status(self):
        return self.state['status']

    def __init__(self, fields: List[str], start_at: str, end_at: str, marketo_client: MarketoClient, filter_type: str):
        self.fields = fields
        self.startAt = start_at
        self.endAt = end_at
        self.marketo_client = marketo_client
        self.filter_type = filter_type
        self.state = None
        self.create()

    def create(self):
        self.state = self.marketo_client.execute(method='create_' + self.entity + '_export_job', fields=self.fields,
                                                 filters={self.filter_type:
                                                              {'endAt': self.endAt, 'startAt': self.startAt}})[0]
        return self

    def enqueue(self):
        self.state = self.marketo_client.execute(method='enqueue_' + self.entity + '_export_job', job_id=self.id)[0]
        return self

    def sync_state(self):
        self.state = self.marketo_client.execute(method='get_' + self.entity + '_export_job_status', job_id=self.id)[0]
        return self

    def get_file_contents(self):
        return self.marketo_client.execute(method='get_' + self.entity + '_export_job_file', job_id=self.id)

    def write_manifesto_file(self):  # TODO: use KBC library
        if not self.__class__.manifesto_file_written:
            os.makedirs('/data/out/tables/' + self.entity + '.csv', exist_ok=True)
            with open('/data/out/tables/' + self.entity + '.csv.manifesto', 'wt') as file:
                file.write(json.dumps({"columns": self.fields,
                                       "destination": "in.c-marketo." + self.entity}))
                self.__class__.manifesto_file_written = True
        return self

    def write_file(self, include_header=False):
        self.write_manifesto_file()
        file_contents = self.get_file_contents()
        if not include_header:
            file_contents = file_contents[file_contents.index(b'\n') + 1:]
        slice_name = str(self.id)
        with open('/data/out/tables/' + self.entity + '.csv/' + slice_name, 'wb') as file:
            file.write(file_contents)
        return self


class LeadsExportJob(ExportJob):
    entity = "leads"


class ActivitiesExportJob(ExportJob):
    entity = "activities"
