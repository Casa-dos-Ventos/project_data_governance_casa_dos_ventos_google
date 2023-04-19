# General imports
from googleapiclient import discovery
import json
import time
import pandas as pd
from datetime import datetime, timedelta


def get_credentials_from_service_account(service_account, scopes=['https://www.googleapis.com/auth/cloud-platform'], key_lifetime_seconds=3600):
    """Return Google Oauth2 Credentials Object

    Required Permissions:
        Service Account Token Creator

    Parameters:
        service_account (str): Service account 
        scopes (list): Scopes list
        key_lifetime_seconds (int): Credentials Lifetime in Seconds

    Returns:    
        credentials (google.oauth2.credentials.Credentials): Google Oauth2 Credentials Object

    Prerequisites to run on-premisses:
        Google SDK installed and run the follow command:
            - gcloud auth application-default login
    """

    from google.cloud import iam_credentials
    from google.oauth2 import credentials
    from google.protobuf import duration_pb2

    client = iam_credentials.IAMCredentialsClient()
    lifetime = duration_pb2.Duration()
    lifetime.FromSeconds(seconds=key_lifetime_seconds)
    request = iam_credentials.GenerateAccessTokenRequest(name=service_account,
                                                         scope=scopes,
                                                         lifetime=lifetime)
    access_token = client.generate_access_token(request)
    return credentials.Credentials(token=access_token.access_token)

def list_all_projects_gcp(credentials):
    """
    List all projects in gcp.

    Args:
        credentials (google.oauth2.credentials.Credentials): the credentials of account to use to access api.

    Returns:
        list_all_projects (str): list of projects

    """
    
    service = discovery.build('cloudresourcemanager', 'v1', credentials=credentials)
    list_all_projects = []

    pageToken=""
    while pageToken is not None:
        request = service.projects().list(filter="lifecycleState:ACTIVE", pageToken=pageToken)
        resp_list_projects = request.execute()
        #listando os projetos da gcp
        for i in range(0, len(resp_list_projects["projects"])):
            list_all_projects.append(resp_list_projects["projects"][i]["projectId"])
        
        pageToken = resp_list_projects.get('nextPageToken')
    
    return list_all_projects

def list_datasets(project, credentials):

    """
    List all datasets in a project in gcp.

    Args:
        project (str): project name on gcp to search for datasets
        credentials (google.oauth2.credentials.Credentials): the credentials of account to use to access api.

    Returns:
        list_datasets (list): list of datasets

    """

    service = discovery.build('bigquery', 'v2', credentials=credentials, cache_discovery=False)
    pageToken=""
    list_datasets = []
    while pageToken is not None:

        rqst = service.datasets().list(projectId=project, pageToken=pageToken)
        resp = rqst.execute()
        if not 'datasets' in resp:
            list_datasets=[]
        else:
            for i in range(0,len(resp['datasets'])):
                list_datasets.append(resp['datasets'][i]['datasetReference']['datasetId'])
        pageToken = resp.get('nextPageToken')
    
    return list_datasets

def list_views(project, dataset, credentials):
    """
    List all views for a project and dataset in gcp.

    Args:
        project (str): project name on gcp to search for tables
        dataset (str): dataset name on gcp to search for tables
        credentials (google.oauth2.credentials.Credentials): the credentials of account to use to access api.

    Returns:
        df_info_views (DataFrame): dataframe with views infos

    """
    service = discovery.build('bigquery', 'v2', credentials=credentials, cache_discovery=False)
    pageToken_list_views=""
    list_views = []

    df_info_views = pd.DataFrame()
    df_info_view = pd.DataFrame()
    while pageToken_list_views is not None:

        rqst = service.tables().list(projectId=project, datasetId=dataset, pageToken=pageToken_list_views)
        resp = rqst.execute()
        if 'tables' in resp.keys():
            for i in range(0,len(resp['tables'])):
                if resp['tables'][i]['type']=='VIEW':
                    list_views.append(resp['tables'][i]['tableReference']['tableId'])
                else: None
            pageToken_list_views = resp.get('nextPageToken')
        else:
            print("There are no views in this dataset.")
            list_views = []
            pageToken_list_views = None
    
    for view in list_views:
        print(f"view: {view}")

        rqst = service.tables().get(projectId=project, datasetId=dataset, tableId=view)
        resp = rqst.execute()
        df_info_view = pd.json_normalize(resp)

        df_info_views = pd.concat([df_info_views,df_info_view], ignore_index=True)
    
    return df_info_views


################ Main code #######################



# Getting google credentials...
SCOPES = ['https://www.googleapis.com/auth/cloud-platform']
#put yout service account here
svc_account = 'test@developer.gserviceaccount.com'
credentials = get_credentials_from_service_account(svc_account,SCOPES)
# credentials = GoogleCredentials.get_application_default() #you can use this to default authentication

list_all_projects = []
list_projects_with_bigquery_api_enabled = []

#Date extraction to store all day extractions
date_extraction = datetime.today().date()
log_time = datetime.today()

#List all project of gcp
list_all_projects = list_all_projects_gcp(credentials=credentials)

#Remove project from appscript,appsheets and etc that starts with "sys-"
list_all_projects = [x for x in list_all_projects if not x.startswith('sys-')]


#Filtering only project that has big query API enabled
service = discovery.build('serviceusage', 'v1', credentials=credentials)
print('Filtering only project that has big query API enabled')
for project in list_all_projects:
    pageToken=""
    while pageToken is not None:
        request = service.services().list(parent="projects/{}".format(project), filter = "state:ENABLED", fields='services/config/name,nextPageToken', pageToken=pageToken, pageSize=200)
        resp = request.execute()
        pageToken = resp.get('nextPageToken')
        time.sleep(0.3)
        if resp == {}: #jump if response is empty
            continue
        for i in range(0,len(resp['services'])):
            if "bigquery.googleapis.com" in resp['services'][i]['config']['name']: #check if exists big query api enabled, if yes, so append it
                list_projects_with_bigquery_api_enabled.append(project)
            else:
                None

#remove duplicates if has
list_projects_with_bigquery_api_enabled = list(set(list_projects_with_bigquery_api_enabled))

#filtering only important columns, you can add more if you want
cols_table_filter = ['tableReference.projectId','tableReference.datasetId', 'tableReference.tableId', 'location',
                    'creationTime', 'lastModifiedTime', 'description','schema.fields','view.query']
info_views_bigquery = pd.DataFrame(columns=cols_table_filter)

#For loop to go inside each project and dataset and list tables
for project in list_projects_with_bigquery_api_enabled:
    print("Analyzing the project {}".format(project))
    datasets = list_datasets(project, credentials)

    if datasets == []:
        continue #jump iteration
    else: None
    
    for dataset in datasets:
        print(f"------------Dataset: {dataset}------------")
        info_views = list_views(project, dataset, credentials)
        info_views = info_views.reindex(columns = cols_table_filter) 
        info_views_bigquery = pd.concat([info_views_bigquery,info_views],ignore_index=True).reset_index(drop = True)

info_views_bigquery = info_views_bigquery[cols_table_filter]

#renaming columns
info_views_bigquery.rename(columns = {'tableReference.projectId':'project_id', 
                                        'tableReference.datasetId':'dataset_id', 
                                        'tableReference.tableId':'view_id',
                                        'creationTime':'creation_time', 
                                        'lastModifiedTime':'last_modified_time',
                                        'description':'description',
                                        'schema.fields':'schema_fields',
                                        'view.query':'query'
                                        }, inplace=True)

info_views_bigquery['date_extraction'] = date_extraction  #day of extraction
info_views_bigquery['log_time'] = log_time #datetime of extraction

columns_order = ['date_extraction','project_id','dataset_id', 'view_id', 'creation_time', 
            'last_modified_time', 'location', 'description','schema_fields','query','log_time']
                    
info_views_bigquery = info_views_bigquery[columns_order]

#Table Schema
table_schema_file = "table_schema.json"
table_schema_json = open(table_schema_file)
table_schema = json.load(table_schema_json)

#bigquery informations
dataset_name = 'dataset'
gbq_table= 'bigquery_views_analysis'
project_gcp = 'project'

#Treating dataframe columns
info_views_bigquery.date_extraction = pd.to_datetime(info_views_bigquery['date_extraction'])
info_views_bigquery.creation_time = pd.to_datetime(info_views_bigquery['creation_time'], unit='ms')
info_views_bigquery.last_modified_time = pd.to_datetime(info_views_bigquery['last_modified_time'], unit='ms')

#Remove all \r or \n from dataframe
info_views_bigquery = info_views_bigquery.replace(r'\n','', regex=True)
info_views_bigquery = info_views_bigquery.replace(r'\r','', regex=True)
info_views_bigquery['schema_fields'] = '"' + info_views_bigquery.schema_fields.astype(str) + '"'

print('Inserting data to bigquery')
info_views_bigquery.to_gbq(destination_table = dataset_name+"."+gbq_table,
                            project_id = project_gcp,
                            if_exists='append',
                            chunksize=1000000,
                            table_schema=table_schema)