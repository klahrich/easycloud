# easycloud

### What it is

A set of python functions and command-line utilities to interact with:
- Bigquery
- Dataflow

### Pre-requisites:

In order to use the functions this script, you will need: 
- a GCP account, with a project and at least a dataset 
- a service account (https://cloud.google.com/iam/docs/creating-managing-service-account-keys)
- an environment variable called _GOOGLE_APPLICATION_CREDENTIALS_ that points to your service account key file

### Install:

- `pip install --upgrade git+https://github.com/klahrich/easycloud.git`

### Example usage:

- Bigquery python api:

```python
from easycloud.gcp import *

bq = Bigquery()

df = query_to_local(bq, "SELECT AVG(some_numeric_var) AS my_avg FROM some_dataset.some_table", "my_local_df.csv")

# df is a regular pandas dataframe
df.head()
```

- Bigquery command-line:

```
bigquery query-to-local 'SELECT AVG(some_numeric_var) AS my_avg FROM some_dataset.some_table' my_local_df.csv
```

### Available functions

- `table_exists`
- `table_info`
- `query_to_local`
- `query_to_table`
- `table_to_local`
- `local_to_table`

### Dataflow

The dataflow portion only supports reading from and writing to a Bigquery table.

1. Code structure

To use dataflow, you should have some code structured like a package, as such:

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

The `setup.py` file can be as simple as:
```
from setuptools import setup, find_packages

setup(
    name='my_package',
    packages=find_packages(),
    install_requires=[] 
)
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

You need to pass a `config.yaml` file as a command-line option. Here's an example of what this file might contain (adapt to your own use-case):

```
job_name: some_name
project: id_of_bigquery_project
temp_location: gs://bucket_path
input_table: dataset.input_table_name
output_table: dataset.output_table_name
output_schema: var1:STRING, var2:FLOAT, output:BOOLEAN
setup_file: ./setup.py
```

4. Running the job

From within your root folder, run the following command to launch the dataflow job (assuming that your runner is inside module1):
```
dataflow my_package.my_runner --config_file path/to/config.yaml
```

5. Initializing the job

It might happen that you need to do some initialization, as a one time thing before the dataflow job starts.

To do this, you can implement a `init` function in your runner module. __If implemented, this function must return a tuple__. Then, your `run` function must take as many additional positional parameters as there are elements in that tuple, so that the tuple you return from `init` will be unpacked and passed to `run`.

So it would look something like this:

```{python}
def init():
    # do something here
    return (value1, value2)

def run(p, param1, parame):
    # dataflow stuff, that will be passed param1=value1, param2=value2
```
