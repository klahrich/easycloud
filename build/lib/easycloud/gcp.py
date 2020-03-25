from google.cloud.exceptions import NotFound
from google.cloud import bigquery as bgq
from google.cloud import storage
from google.oauth2 import service_account
from google.cloud import bigquery_storage_v1beta1
import os
from typing import Dict, Union
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
import click
from pprint import pprint


class Bigquery:
    '''
    A simple wrapper over google bigquery api.
    You need to set a GOOGLE_APPLICATION_CREDENTIALS environment variable to point to your secret file.
    '''
    def __init__(self, timezone='US/Eastern', service_account_path=os.environ['GOOGLE_APPLICATION_CREDENTIALS']):
        self.timezone = timezone
        credentials = service_account.Credentials.from_service_account_file(service_account_path)
        self.project = credentials.project_id
        self.client = bgq.Client(credentials=credentials,
                                 project=credentials.project_id)
        self.bqstorage_client = bigquery_storage_v1beta1.BigQueryStorageClient(credentials=credentials)
        self.storage_client = storage.Client(credentials=credentials,
                                             project=credentials.project_id)

@click.group()
@click.option('--timezone', default='US/Eastern')
@click.option('--service-account-path', envvar='GOOGLE_APPLICATION_CREDENTIALS')
@click.pass_context
def bigquery(bq, timezone, service_account_path):
    bq.obj = Bigquery(timezone=timezone, service_account_path=service_account_path)


@bigquery.command()
@click.argument('table-path', type=str)
@click.option('--verbose/--silent', default=True)
@click.pass_obj
def table_exists(bq: Bigquery, table_path: str, verbose:bool = True) -> bool:
    '''
    Check if a table exists.

    Args:
        table_path (str): path of the table in BigQuery, format: 'dataset.tablename'

    Returns: True or False
    '''
    dataset, table = table_path.split('.')
    dataset_ref = bq.client.dataset(dataset)
    table_ref = dataset_ref.table(table)
    try:
        bq.client.get_table(table_ref)
        if verbose:
            print(f'Table {table_path} exists.')
        return True
    except NotFound:
        if verbose:
            print(f'Table {table_path} not found.')
        return False


@bigquery.command()
@click.argument('table-path', type=str)
@click.option('--verbose/--silent', default=True)
@click.pass_obj
def table_info(bq: Bigquery, table_path: str, verbose:bool = True) -> Table:
    '''
    Retrive information about a bigquery table.

    Args:
        table_path (str): path of the table in BigQuery, format: 'dataset.tablename'

    See https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.table.Table.html
    '''
    dataset, table = table_path.split('.')
    dataset_ref = bq.client.dataset(dataset)
    table_ref = dataset_ref.table(table)
    info = bq.client.get_table(table_ref)
    if verbose:
        pprint({'created': info.created,
               'description': info.description,
               'modified': info.modified,
               'num_bytes': f'{info.num_bytes:,}',
               'num_rows': f'{info.num_rows:,}',
               'schema': info.schema})
    return info


@bigquery.command()
@click.argument('sql', type=str)
@click.argument('--filepath', required=False, default=None)
@click.option('--use-bqstorage/no-bqstorage', is_flag=True, default=True)
@click.pass_obj
def query_to_local(bq: Bigquery, sql, filepath=None, use_bqstorage=True, **inputs) -> pd.DataFrame:
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

    res = bq.client.query(sql)
    if not use_bqstorage:
        df = res.to_dataframe()
    else:
        df = res.to_dataframe(bqstorage_client=bq.bqstorage_client)

    if filepath is not None:
        df.to_csv(filepath, index=False)

    return df


@bigquery.command()
@click.argument('sql', type=str)
@click.argument('table-path', type=str)
@click.option('--overwrite', is_flag=True, default=False)
@click.option('--append', is_flag=True, default=True)
@click.pass_obj
def query_to_table(bq: Bigquery, sql, table_path, overwrite=False, append=True, **inputs) -> None:
    '''
    Create a BigQuery table from sql query.

    Args:
        sql (str): either the sql query to run (e.g. "SELECT * FROM some_dataset.some_table") or the path to a file containing it.
        table_path (str): path of destination table in bigquery. Format: 'dataset.table'
        overwrite (bool): True = overwrite
        append (bool): only considered if overwrite=False, True = append
        inputs (dict): your sql string can have placeholder variables such as {table1}, {table2}, etc;
                       you can set the values of those placeholders here, i.e. {'table1': 'name_of_table1', {'table2': 'name_of_table2'}. Note that the table names (name_of_table1, name_of_table2) must be complete table names (project.dataset.table).
    '''
    dataset, table = table_path.split('.')
    dataset_ref = bq.client.dataset(dataset)
    table_ref = dataset_ref.table(table)
    job_config = bgq.QueryJobConfig()
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

    query_job = bq.client.query(sql, job_config=job_config)

    query_job.result()


@bigquery.command()
@click.argument('table-path', type=str)
@click.option('--start-index', default=None)
@click.option('--nrows', default=None)
@click.option('--use-bqstorage/--no-bqstorage', is_flag=True, default=True)
@click.option('--file-path', type=str, default=None)
@click.option('--force/--no-force', default=False)
@click.option('--verbose/--silent', default=True)
@click.pass_obj
def table_to_local(bq: Bigquery,
                table_path: str,
                fields: Dict[str, str] = None,
                start_index: int = None,
                nrows: int = None,
                use_bqstorage=True,
                file_path=None,
                force=False,
                verbose=True) -> pd.DataFrame:
    """
    Retrieves data from a bigquery table and optionally saves it to local CSV.
    Before downloading the data, we check that the table is more recent than the CSV.
    If not, we skip and just read from the CSV.
    Use force=True to disable this and always download the table.

    Args:
        table_path (str): path of the table in bigquery. Format: 'dataset.table'.
        fields (dict): dict of {"field_name": "field_type"}. If None, all columns are returned
        start_index (int): index of first row to retrieve
        nrows (int): number of rows to retrieve
        use_bqstorage (bool): set to True to download big data, will be faster
        file_path (str): full path to a local CSV file to use as cache. 
        force (bool): download the table, whether it is more recent than the CSV or not

    Returns:
        The data as a pandas dataframe.
    """
    def _table_to_df():
        selected_fields = [bgq.SchemaField(k, v) for k,v in fields.items()] if fields is not None else None
        table_path = '.'.join([bq.project, dataset, table])
        rows = bq.client.list_rows(table=table_path, selected_fields=selected_fields, max_results=nrows, start_index=start_index)

        if not use_bqstorage or (nrows is not None):
            return rows.to_dataframe()
        else:
            return rows.to_dataframe(bqstorage_client=bq.bqstorage_client)

    dataset, table = table_path.split('.')

    if (file_path is None) or force:
        if verbose:
            print("Downloading from bigquery.")
        df = _table_to_df(dataset, table, fields, start_index, nrows, use_bqstorage)
        if file_path is not None:
            df.to_csv(file_path, index=False)
        return df

    elif os.path.isfile(file_path):
        info = table_info(dataset, table)
        date_bq = info.modified.astimezone(pytz.timezone('UTC'))
        date_csv = os.path.getmtime(file_path)
        date_csv = datetime.fromtimestamp(date_csv)
        date_csv = pytz.timezone(bq.timezone).localize(date_csv).astimezone(pytz.timezone('UTC'))
        if date_bq > date_csv:
            if verbose:
                print("Bigquery table is more recent. Downloading from bigquery and overwriting CSV.")
            df = _table_to_df()
            df.to_csv(file_path, index=False)
            return df
        else:
            if verbose:
                print("CSV is up to date. Reading from csv.")
            df = pd.read_csv(file_path)
            return df
    else:
        if verbose:
            print("CSV not found. Downloading from bigquery.")
        df = _table_to_df()
        df.to_csv(file_path, index=False)
        return df


@bigquery.command()
@click.argument('local', type=str)
@click.argument('table-path', type=str)
@click.option('--overwrite', is_flag=True, default=False)
@click.option('--append', is_flag=True, default=True)
@click.pass_obj
def local_to_table(bq: Bigquery, local: Union[str, pd.DataFrame], table_path: str, overwrite: bool = False, append:bool = True) -> None:
    '''
    Upload a local CSV file to a BigQuery table

    Args:
        local (str or DataFrmae): if str, full path to the CSV file. Otherwise a pandas dataframe.
        table_path (str): path of the table in bigquery. Format: 'dataset.table'.
        overwrite (bool): True = overwrite
        append (bool): only considered if overwrite=False.
    '''
    dataset, table = table_path.split('.')
    dataset_ref = bq.client.dataset(dataset)
    table_ref = dataset_ref.table(table)
    job_config = bgq.LoadJobConfig()

    if type(local) == str:
        job_config.source_format = bgq.SourceFormat.CSV
        job_config.skip_leading_rows = 1
        job_config.autodetect = True
    elif type(local) == pd.DataFrame:
        job_config.source_format = bgq.SourceFormat.PARQUET
        job_config.autodetect = True
    else:
        raise TypeError(f'local parameter must be a string or a pandas dataframe, not {type(local)}')

    if overwrite:
        job_config.write_disposition = "WRITE_TRUNCATE"
    elif append:
        job_config.write_disposition = "WRITE_APPEND"
    else:
        job_config.write_disposition = "WRITE_EMPTY"

    if type(local) == str:
        with open(local, "rb") as source_file:
            job = bq.client.load_table_from_file(source_file, table_ref, job_config=job_config)
    else:
        job = bq.client.load_table_from_dataframe(local, table_ref, job_config=job_config)

    job.result()
    print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset, table))


@bigquery.command()
@click.argument('file-path', type=str, default=None)
@click.argument('bucket', type=str)
@click.pass_obj
def local_to_blob(bq: Bigquery, local: Union[str, pd.DataFrame], bucket: str):
    bucket = bq.storage_client.get_bucket(bucket)
    if type(local) == str:
        p = Path(file_path)
        bucket.blob(p.name).upload_from_filename(file_path)
    elif type(local) == pd.DataFrame:
        with tempfile.NamedTemporaryFile(delete=False) as temp:
            df.to_csv(temp.name, index=False)
            bucket.blob(blobname).upload_from_filename(temp.name)
        os.remove(temp.name)
    else:
        raise TypeError(f'local must be a string or a dataframe, not {type(local)}')

def df_to_blob(bq: Bigquery, df, bucket:str, blobname: str):
    bucket = bq.storage_client.get_bucket(bucket)



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

    def run(self, run_func, *args):
        logging.getLogger().setLevel(logging.INFO)

        pipeline_options = PipelineOptions.from_dictionary(self.options)
        pipeline_options.view_as(SetupOptions).save_main_session = True

        with beam.Pipeline(options=pipeline_options) as p:
            p = (p | 'read_bq_table' >> beam.io.Read(beam.io.BigQuerySource(self.input_table)))
            p = run_func(p, *args)
            (p | 'write_bq_table' >> beam.io.gcp.bigquery.WriteToBigQuery(
                                        self.output_table,
                                        schema = self.output_schema,
                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

@click.command()
@click.argument('runner')
@click.option('--config-file')
def dataflow(runner, config_file):
    with open(config_file) as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    runner_module = importlib.import_module(runner)

    args = None

    if hasattr(runner_module, 'init'):
        init = runner_module.init
        if callable(init):
            args = init()

    flow = Dataflow(**config)

    if args is not None:
        flow.run(runner_module.run, *args)
    else:
        flow.run(runner_module.run)

