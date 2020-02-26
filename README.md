# easycloud



### Pre-requisites:

In order to use the functions this script, you will need: 
- a GCP account, with a project and at least a dataset 
- a service account key (https://cloud.google.com/iam/docs/creating-managing-service-account-keys)
- an environment variable called _GOOGLE_APPLICATION_CREDENTIALS_ that points to your service account key file

### Install:

- `pip install git+https://github.com/klahrich/easycloud.git`

### Example usage:

```python
from import easycloud.gcp import Bigquery

bq = Bigquery()

df = bq.query("SELECT AVG(some_numeric_var) AS my_avg FROM some_dataset.some_table")

# df is a regular pandas dataframe
df.head()
```
