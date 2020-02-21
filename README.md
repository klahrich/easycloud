# easycloud



### Pre-requisites:

In order to use the functions this script, you will need: 
- a GCP account, with a project and at least a dataset 
- a service account key (https://cloud.google.com/iam/docs/creating-managing-service-account-keys)
- an environment variable called _GOOGLE_APPLICATION_CREDENTIALS_ that points to your service account key file
- install gcp libraries for bigquery: `pip install --upgrade google-cloud-bigquery[bqstorage]`
