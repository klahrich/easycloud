from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import bigquery_storage_v1beta1
import os
from typing import Dict

class BigQuery:
    ''' 
    A simple wrapper over google bigquery api. 
    You need to set a GOOGLE_APPLICATION_CREDENTIALS environment variable to point to your service account key.
    '''
    credentials = None
    client = None
    key_path = None
    project = None

    def __init__(self):
        credentials = service_account.Credentials.from_service_account_file(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
        self.project = credentials.project_id
        self.client = bigquery.Client(credentials=credentials,
                                      project=credentials.project_id)
        self.bqstorage_client = bigquery_storage_v1beta1.BigQueryStorageClient(credentials=credentials)

    def query(self, sql, use_bqstorage=True):
        '''
        Args:
            sql (str): the SQL query you want to run
            use_bqstorage (bool): set to True to download big data, will be faster

        Returns:
            A pandas dataframe
        '''
        res = self.client.query(sql)
        if not use_bqstorage:
            return res.to_dataframe()
        else:
            return res.to_dataframe(bqstorage_client=self.bqstorage_client)

    def list_rows(self, dataset: str, table: str, fields: Dict[str, str] = None, use_bqstorage=True):
        '''
        Retrieve all rows form a big query table.

        Args:
            dataset (str): name of the dataset
            table (str): name of the table
            fields (dict): dict of {"field_name": "field_type"}. If None, all columns are returned
            use_bqstorage (bool): set to True to download big data, will be faster

        Returns:
            A pandas dataframe.
        '''
        selected_fields = [bigquery.SchemaField(k, v) for k,v in fields.items()] if fields is not None else None
        table_path = '.'.join([self.project, dataset, table])
        rows = self.client.list_rows(table_path, selected_fields)
        if not use_bqstorage:
            return rows.to_dataframe()
        else:
            return rows.to_dataframe(bqstorage_client=self.bqstorage_client)

    def upload_csv(self, filepath: str, dataset: str, table: str, overwrite: bool = False):
        '''
        Upload a CSV file to a big query table.

        Args:
            filepath (str): full path of the CSV local file
            dataset (str): name of the dataset
            table (str): name of the table
            overwrite(bool): if True, will overwrite. Otherwise will append.
            use_bqstorage (bool): set to True to download big data, will be faster
        '''
        dataset_ref = self.client.dataset(dataset)
        table_ref = dataset_ref.table(table)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 1
        job_config.autodetect = True
        if overwrite:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

        with open(filepath, "rb") as source_file:
            job = self.client.load_table_from_file(source_file, table_ref, job_config=job_config)

        job.result()
        print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset, table))

