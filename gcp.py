from google.cloud.exceptions import NotFound
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import bigquery_storage_v1beta1
import os
from typing import Dict
import pandas as pd
from google.cloud.bigquery.table import Table

class BigQuery:
    ''' 
    A simple wrapper over google bigquery api. 
    You need to set a GOOGLE_APPLICATION_CREDENTIALS environment variable to point to your secret file.
    '''

    client = None
    project = None
    bqstorage_client = None

    def __init__(self):
        credentials = service_account.Credentials.from_service_account_file(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
        self.project = credentials.project_id
        self.client = bigquery.Client(credentials=credentials,
                                      project=credentials.project_id)
        self.bqstorage_client = bigquery_storage_v1beta1.BigQueryStorageClient(credentials=credentials)

    def table_exists(self, dataset: str, table: str) -> bool:
        ''' Check if a table exists.
        
        Args:
            dataset (str): name of the dataset on BigQuery
            table (str): name of the table on BigQuery
            
        Returns: True or False
        '''
        dataset_ref = self.client.dataset(dataset)
        table_ref = dataset_ref.table(table)
        try:
            self.client.get_table(table_ref)
            return True
        except NotFound:
            return False

    def table_info(self, dataset: str, table: str) -> Table:
        '''
        See https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.table.Table.html
        '''
        dataset_ref = self.client.dataset(dataset)
        table_ref = dataset_ref.table(table)
        return self.client.get_table(table_ref)

    def query(self, sql, use_bqstorage=True) -> pd.DataFrame:
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

    def create_table(self, sql, dataset, table, overwrite=False) -> None:
        '''
        Create a BigQuery table from sql query.

        Args:
            sql (str): the sql query to run (e.g. "SELECT * FROM some_dataset.some_table")
            dataset (str): name of destination dataset
            table (str): name of destination table
            overwrite (bool): if False and the table already exists, the function will do nothing
        '''
        dataset_ref = self.client.dataset(dataset)
        table_ref = dataset_ref.table(table)
        job_config = bigquery.QueryJobConfig()
        job_config.destination = table_ref
        
        if self.table_exists(dataset, table):
            if overwrite:
                self.client.delete_table(table)
            else:
                print(f"Table {dataset}:{table} already exists. Skipping.")
                return
        
        query_job = self.client.query(sql, job_config=job_config)

        query_job.result()

    def list_rows(self, dataset: str, table: str, fields: Dict[str, str] = None, use_bqstorage=True) -> pd.DataFrame:
        '''
        Retrieve all rows form a big query table.

        Args:
            table (str): Full table name, "project_id.dataset.tablename"
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

    def upload_csv(self, filepath: str, dataset: str, table: str, overwrite: bool = False) -> None:
        '''
        Upload a local CSV file to a BigQuery table
        
        Args:
            filepath (str): full path to the CSV file
            dataset (str): name of the dataset on BigQuery
            table (str): name of the table on BigQuery
            overwrite (bool): True = overwrite, False = append
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

