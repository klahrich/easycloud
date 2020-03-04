from google.cloud.exceptions import NotFound
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import bigquery_storage_v1beta1
import os
from typing import Dict
import pandas as pd
from google.cloud.bigquery.table import Table
import os
import pytz
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from pathlib import Path


class Bigquery:
    '''
    A simple wrapper over google bigquery api.
    You need to set a GOOGLE_APPLICATION_CREDENTIALS environment variable to point to your secret file.
    '''

    def __init__(self, timezone='US/Eastern', env_var='GOOGLE_APPLICATION_CREDENTIALS'):
        self.timezone = timezone
        credentials = service_account.Credentials.from_service_account_file(os.environ[env_var])
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


    def create_table(self, sql, dataset, table, overwrite=False, append=True, **inputs) -> None:
        '''
        Create a BigQuery table from sql query.

        Args:
            sql (str): either the sql query to run (e.g. "SELECT * FROM some_dataset.some_table") or the path to a file containing it.
            dataset (str): name of destination dataset
            table (str): name of destination table
            overwrite (bool): True = overwrite
            append (bool): only considered if overwrite=False, True = append
            inputs (dict): your sql string can have placeholder variables such as {table1}, {table2}, etc;
                           you can set the values of those placeholders here, i.e. {'table1': 'name_of_table1', {'table2': 'name_of_table2'}. Note that the table names (name_of_table1, name_of_table2) must be complete table names (project.dataset.table).
        '''
        dataset_ref = self.client.dataset(dataset)
        table_ref = dataset_ref.table(table)
        job_config = bigquery.QueryJobConfig()
        job_config.destination = table_ref

        if overwrite:
            job_config.write_disposition = "WRITE_TRUNCATE"
        elif append:
            job_config.write_disposition = "WRITE_APPEND"
        else:
            job_config.write_disposition = "WRITE_EMPTY"

        p = Path(sql)
        if p.is_file():
            with open(sql, 'r') as f:
                sql = f.read()

        if inputs is not None:
            sql = sql.format(**inputs)

        query_job = self.client.query(sql, job_config=job_config)

        query_job.result()


    def list_rows(self, dataset: str, table: str, fields: Dict[str, str] = None, start_index: int = None, nrows: int = None, use_bqstorage=True) -> pd.DataFrame:
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
        rows = self.client.list_rows(table=table_path, selected_fields=selected_fields, max_results=nrows, start_index=start_index)

        if not use_bqstorage or (nrows is not None):
            return rows.to_dataframe()
        else:
            return rows.to_dataframe(bqstorage_client=self.bqstorage_client)


    def upload_csv(self, filepath: str, dataset: str, table: str, overwrite: bool = False, append:bool = True) -> None:
        '''
        Upload a local CSV file to a BigQuery table

        Args:
            filepath (str): full path to the CSV file
            dataset (str): name of the dataset on BigQuery
            table (str): name of the table on BigQuery
            overwrite (bool): True = overwrite
            append (bool): only considered if overwrite=False, True = append
        '''
        dataset_ref = self.client.dataset(dataset)
        table_ref = dataset_ref.table(table)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 1
        job_config.autodetect = True

        if overwrite:
            job_config.write_disposition = "WRITE_TRUNCATE"
        elif append:
            job_config.write_disposition = "WRITE_APPEND"
        else:
            job_config.write_disposition = "WRITE_EMPTY"

        with open(filepath, "rb") as source_file:
            job = self.client.load_table_from_file(source_file, table_ref, job_config=job_config)

        job.result()
        print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset, table))


    def upload_dataframe(self, df: pd.DataFrame, dataset: str, table: str, overwrite: bool = False, append: bool = True) -> None:
        '''
        Upload a dataframe to a BigQuery table

        Args:
            df: a Pandas dataframe
            dataset (str): name of the dataset on BigQuery
            table (str): name of the table on BigQuery
            overwrite (bool): True = overwrite
            append (bool): only considered if overwrite=False, True = append
        '''
        dataset_ref = self.client.dataset(dataset)
        table_ref = dataset_ref.table(table)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.PARQUET
        job_config.autodetect = True

        if overwrite:
            job_config.write_disposition = "WRITE_TRUNCATE"
        elif append:
            job_config.write_disposition = "WRITE_APPEND"
        else:
            job_config.write_disposition = "WRITE_EMPTY"

        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)

        job.result()
        print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset, table))

    def read_csv(self, filepath: str, dataset: str, table: str) -> pd.DataFrame:
        '''
        A warpper around `list_rows` that saves a Bigquery table to local CSV and reads from it.
        If the table changes, we update the CSV.

        Args:
            filepath (str): full path to the CSV file
            dataset (str): name of the dataset on BigQuery
            table (str): name of the table on BigQuery
        '''
        if os.path.isfile(filepath):
            info = self.table_info(dataset, table)
            date_bq = info.modified.astimezone(pytz.timezone('UTC'))
            date_csv = os.path.getmtime(filepath)
            date_csv = datetime.fromtimestamp(date_csv)
            date_csv = pytz.timezone(self.timezone).localize(date_csv).astimezone(pytz.timezone('UTC'))
            if date_bq > date_csv:
                print("Reading from bigquery")
                df = self.list_rows(dataset, table)
                df.to_csv(filepath, index=False)
                return df
            else:
                print("Reading from csv")
                df = pd.read_csv(filepath)
                return df
        else:
            print("Reading from bigquery")
            df = self.list_rows(dataset, table)
            df.to_csv(filepath, index=False)
            return df
