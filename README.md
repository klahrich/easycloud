# easycloud



### Pre-requisites:

In order to use the functions this script, you will need: 
- a GCP account, with a project and at least a dataset 
- a service account key (https://cloud.google.com/iam/docs/creating-managing-service-account-keys)
- an environment variable called _GOOGLE_APPLICATION_CREDENTIALS_ that points to your service account key file

### Install:

- `pip install --upgrade git+https://github.com/klahrich/easycloud.git`

### Example usage:

- Bigquery:

```python
from easycloud.gcp.bigquery import Bigquery

bq = Bigquery()

df = bq.query("SELECT AVG(some_numeric_var) AS my_avg FROM some_dataset.some_table")

# df is a regular pandas dataframe
df.head()
```

- Dataflow

Right now the dataflow module only supports reading from and writing to a Bigquery table.

1. Code structure

To use dataflow, you should have some code you want to run at scale. 
Your code should itself be structured like a package, as such:

```
root_folder
|
|_setup.py
|
|_my_package
  |_ __init__.py
  |_module1.py
  |_my_runner.py
```

2. Dataflow runner

The code you want to run on Dataflow should be in one of your modules (for this example, we assume it is the `my_runner` module), and it has to be inside a `run` function, taking one parameter (the dataflow pipeline). Here's an example of what such a module could contain, where `some_func` is a function you want to apply to every row of a Bigquery table:

```python

import apache_beam as beam
from my_package.module1 import some_func

def run(p):
    return (p | 'processing' >> 
            beam.Map(lambda d: {'var1': d['var2'],
                                'var2': d['var2'],                                
                                'output': some_func(d['var1'], d['var2'])})
```

3. Config file

You need a `config.yaml` file under your root folder. Here's an example of what this file might contain (adapt to your own use-case):

```
job_name: some_name
project: id_of_bigquery_project
temp_location: gs://bucket_path
input_table: dataset.input_table_name
output_table: dataset.output_table_name
output_schema: var1:STRING, var2:FLOAT, Credit:FLOAT, output:BOOLEAN
setup_file: ./setup.py
```

4. Running the job

From within your root folder, run the following command (here we assume that your runner is inside module1):
`python -m easycloud.gcp.dataflow my_package.my_runner --config_file config.yaml`

