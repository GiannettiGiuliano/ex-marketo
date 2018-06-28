import os
from abc import ABC, abstractmethod
from typing import List

from config import cfg
from marketorestpython.client import MarketoClient
from marketorestpython.helper.exceptions import MarketoException


class ExportJob(ABC):
    entity = None
    manifesto_file_written = False

    @property
    def id(self):
        return self.state['exportId']

    @property
    def status(self):
        return self.state['status']

    @abstractmethod
    def __init__(self, fields: List[str], start_at: str, end_at: str, marketo_client: MarketoClient,
                 destination_bucket: str, incremental: bool, primary_key: List[str]):
        self.fields = fields
        self.startAt = start_at
        self.endAt = end_at
        self.marketo_client = marketo_client
        self.state = None
        self.destination_bucket = destination_bucket
        self.incremental = incremental
        self.primary_key = primary_key

    @abstractmethod
    def create(self):
        return self

    def enqueue(self) -> bool:
        try:
            self.state = self.marketo_client.execute(method='enqueue_' + self.entity + '_export_job', job_id=self.id)[0]
        except MarketoException as e:
            if e.code == '1029':
                return False
            else:
                raise e
        return True

    def sync_state(self):
        self.state = self.marketo_client.execute(method='get_' + self.entity + '_export_job_status', job_id=self.id)[0]
        return self

    def get_file_contents(self):
        return self.marketo_client.execute(method='get_' + self.entity + '_export_job_file', job_id=self.id)

    def write_manifesto_file(self):
        if not self.__class__.manifesto_file_written:
            os.makedirs('/data/out/tables/' + self.entity + '.csv', exist_ok=True)
            cfg.write_table_manifest('/data/out/tables/' + self.entity + '.csv',
                                     destination=self.destination_bucket + '.' + self.entity,
                                     columns=self.fields,
                                     incremental=self.incremental,
                                     primary_key=self.primary_key)
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

    def __init__(self, fields: List[str], start_at: str, end_at: str, marketo_client: MarketoClient,
                 date_filter_type: str, destination_bucket: str, incremental: bool, primary_key: List[str]):
        super().__init__(fields, start_at, end_at, marketo_client, destination_bucket, incremental, primary_key)
        self.date_filter_type = date_filter_type
        self.create()

    def create(self):
        filters = {self.date_filter_type: {'endAt': self.endAt, 'startAt': self.startAt}}
        self.state = self.marketo_client.execute(method='create_' + self.entity + '_export_job', fields=self.fields,
                                                 filters=filters)[0]
        return self


class ActivitiesExportJob(ExportJob):
    entity = "activities"

    def __init__(self, fields: List[str], start_at: str, end_at: str, marketo_client: MarketoClient,
                 activities_type_ids: List[int], destination_bucket: str, incremental: bool, primary_key: List[str]):
        super().__init__(fields, start_at, end_at, marketo_client, destination_bucket, incremental, primary_key)
        self.activity_type_ids = activities_type_ids
        self.create()

    def create(self):
        filters = ({"createdAt": {'endAt': self.endAt, 'startAt': self.startAt}} if self.activity_type_ids is None else
                   {"createdAt": {'endAt': self.endAt, 'startAt': self.startAt},
                    "activityTypeIds": self.activity_type_ids})
        self.state = self.marketo_client.execute(method='create_' + self.entity + '_export_job', fields=self.fields,
                                                 filters=filters)[0]
        return self
