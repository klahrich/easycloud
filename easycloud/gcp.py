from google.cloud.exceptions import NotFound
from google.cloud import bigquery
from google.cloud import storage
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
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import yaml
import importlib
import argparse
import logging
import tempfile


class Client:
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
        self.storage_client = storage.Client(credentials=credentials,
                                             project=credentials.project_id)


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


    def query_to_df(self, sql, use_bqstorage=True, **inputs) -> pd.DataFrame:
        '''
        Args:
            sql (str): the SQL query you want to run
            use_bqstorage (bool): set to True to download big data, will be faster

        Returns:
            A pandas dataframe
        '''
        p = Path(sql)
        if p.is_file():
            with open(sql, 'r') as f:
                sql = f.read()

        if inputs is not None:
            sql = sql.format(**inputs)
            
        res = self.client.query(sql)
        if not use_bqstorage:
            return res.to_dataframe()
        else:
            return res.to_dataframe(bqstorage_client=self.bqstorage_client)


    def query_to_table(self, sql, dataset, table, overwrite=False, append=True, **inputs) -> None:
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


    def table_to_df(self,
                    dataset: str,
                    table: str,
                    fields: Dict[str, str] = None,
                    start_index: int = None,
                    nrows: int = None,
                    use_bqstorage=True,
                    filepath=None,
                    force=False) -> pd.DataFrame:
        """
        Retrieves data from a bigquery table and save it to local CSV.
        Before downloading the data, we check that the table is more recent than the CSV. If not, we skip.
        Use force=True to disable this and download the table anyway.

        Args:
            dataset (str): name of the dataset on BigQuery
            table (str): name of the table on BigQuery
            fields (dict): dict of {"field_name": "field_type"}. If None, all columns are returned
            start_index (int): index of first row to retrieve
            nrows (int): number of rows to retrieve
            use_bqstorage (bool): set to True to download big data, will be faster
            filepath (str): full path to a local CSV file to use as cache. If local CSV file is more recent than bigquery table, we read directly from it (skip download).
            force (bool): just download the table, whether it is more recent than the CSV or not

        Returns:
            The data as a pandas dataframe.
        """
        def _table_to_df():
            selected_fields = [bigquery.SchemaField(k, v) for k,v in fields.items()] if fields is not None else None
            table_path = '.'.join([self.project, dataset, table])
            rows = self.client.list_rows(table=table_path, selected_fields=selected_fields, max_results=nrows, start_index=start_index)

            if not use_bqstorage or (nrows is not None):
                return rows.to_dataframe()
            else:
                return rows.to_dataframe(bqstorage_client=self.bqstorage_client)
                
        if (filepath is None) or force:
            print("Downloading from bigquery.")
            df = _table_to_df(dataset, table, fields, start_index, nrows, use_bqstorage)
            if filepath is not None:
                df.to_csv(filepath, index=False)
            return df

        elif os.path.isfile(filepath):
            info = self.table_info(dataset, table)
            date_bq = info.modified.astimezone(pytz.timezone('UTC'))
            date_csv = os.path.getmtime(filepath)
            date_csv = datetime.fromtimestamp(date_csv)
            date_csv = pytz.timezone(self.timezone).localize(date_csv).astimezone(pytz.timezone('UTC'))
            if date_bq > date_csv:
                print("Bigquery table is more recent. Downloading from bigquery and overwriting CSV.")
                df = _table_to_df(dataset, table, fields, start_index, nrows, use_bqstorage)
                df.to_csv(filepath, index=False)
                return df
            else:
                print("CSV is up to date. Reading from csv.")
                df = pd.read_csv(filepath)
                return df
        else:
            print("CSV not found. Downloading from bigquery.")
            df = _table_to_df(dataset, table, fields, start_index, nrows, use_bqstorage)
            df.to_csv(filepath, index=False)
            return df   


    def csv_to_table(self, filepath: str, dataset: str, table: str, overwrite: bool = False, append:bool = True) -> None:
        '''
        Upload a local CSV file to a BigQuery table

        Args:
            filepath (str): full path to the CSV file
            dataset (str): name of the dataset on BigQuery
            table (str): name of the table on BigQuery
            overwrite (bool): True = overwrite
            append (bool): only considered if overwrite=False.
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


    def df_to_table(self, df: pd.DataFrame, dataset: str, table: str, overwrite: bool = False, append: bool = True) -> None:
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
        
        
    def file_to_blob(self, filepath: str):
        p = Path(filepath)
        bucket = self.storage_client.get_bucket(self.name)
        bucket.blob(p.name).upload_from_filename(filepath)
        
        
    def df_to_blob(df, bucket:str, blobname: str):
        bucket = self.storage_client.get_bucket(bucket)
        with tempfile.NamedTemporaryFile as temp:
            df.to_csv(temp.name, index=False)
            bucket.blob(p.name).upload_from_filename(filepath)



class Dataflow:

    def __init__(self, job_name, project, temp_location, input_table, output_table, output_schema, setup_file):
        self.options = {}
        self.options['job_name'] = job_name
        self.options['project'] = project
        self.options['temp_location'] = temp_location
        self.options['staging_location'] = temp_location
        self.options['runner'] = 'DataflowRunner'
        self.options['setup_file'] = setup_file
        self.input_table = project + ':' + input_table
        self.output_table = project + ':' + output_table
        self.output_schema = output_schema

    def run(self, runf_func):
        logging.getLogger().setLevel(logging.INFO)

        pipeline_options = PipelineOptions.from_dictionary(self.options)
        pipeline_options.view_as(SetupOptions).save_main_session = True

        with beam.Pipeline(options=pipeline_options) as p:
            p = (p | 'read_bq_table' >> beam.io.Read(beam.io.BigQuerySource(self.input_table)))
            p = runf_func(p)
            (p | 'write_bq_table' >> beam.io.gcp.bigquery.WriteToBigQuery(
                                        self.output_table,
                                        schema = self.output_schema,
                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))


# TODO: add a dataflow command to make it more obvious what we're doing
if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('runner')
    parser.add_argument('--config_file')

    args = parser.parse_args()

    with open(args.config_file) as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    runner_module = importlib.import_module(args.runner)

    flow = Dataflow(**config)
    flow.run(runner_module.run)



