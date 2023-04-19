# General imports
from googleapiclient import discovery
import pandas as pd
from datetime import datetime, timedelta
import json

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


def list_all_lakes(project, credentials):
    """
    List all lakes in dataplex.

    Args:
        credentials (google.oauth2.credentials.Credentials): the credentials of account to use to access api.

    Returns:
        list_lakes (list): list of lakes

    """
    service = discovery.build('dataplex', 'v1', credentials=credentials)

    list_lakes = []
    parent = "projects/{project}/locations/{locations}".format(
                project=project,
                locations = "-" #pegando todas as locations
            )
    request = service.projects().locations().lakes().list(parent=parent)
    resp = request.execute()

    for record in resp['lakes']:
        lake_id = record['name']
        list_lakes.append(lake_id)
    
    return list_lakes



def list_all_zones(lake_id, credentials):
    """
    List all zones inside lakes in dataplex.

    Args:
        lake_id (str): name id of lake in dataplex
        credentials (google.oauth2.credentials.Credentials): the credentials of account to use to access api.

    Returns:
        list_zones (list): list of zones

    """
    service = discovery.build('dataplex', 'v1', credentials=credentials)
    list_zones = []
    parent = lake_id
    request = service.projects().locations().lakes().zones().list(parent=parent)
    resp = request.execute()
    if resp == {}:
        list_zones = []
    else:    
        for record in resp['zones']:
            zone_id = record['name']
            list_zones.append(zone_id)

    return list_zones

def list_all_assets(zone_id, credentials):

    """
    List all assets in zone in dataplex.

    Args:
        zone_id (str): name id of zone in dataplex
        credentials (google.oauth2.credentials.Credentials): the credentials of account to use to access api.

    Returns:
        df_assets (DataFrame): dataframe with assets infos

    """
    service = discovery.build('dataplex', 'v1', credentials=credentials)
    parent = zone_id
    df_assets = pd.DataFrame()

    pageToken=""
    while pageToken is not None:
        request = service.projects().locations().lakes().zones().assets().list(parent=parent, pageToken=pageToken)
        resp = request.execute()   

        if resp == {}:
            name_value = [parent]
            df_assets_tmp = pd.DataFrame(name_value, columns=['name'])
        else: 
            df_assets_tmp = pd.json_normalize(resp,record_path ='assets')
        
        df_assets = pd.concat([df_assets_tmp,df_assets],ignore_index=True).reset_index(drop = True)

        pageToken = resp.get('nextPageToken')

    return df_assets


################ Main code #######################
# Getting google credentials...
SCOPES = ['https://www.googleapis.com/auth/cloud-platform']
#put yout service account here
svc_account = 'test@developer.gserviceaccount.com'
credentials = get_credentials_from_service_account(svc_account,SCOPES)
# credentials = GoogleCredentials.get_application_default() #you can use this to default authentication

#project with your dataplex
project_id = 'project'


list_lakes = list_all_lakes(project=project_id, credentials=credentials)
df_assets = pd.DataFrame()

for lake in list_lakes:
    print(f'lake: {lake}')
    list_zones = list_all_zones(lake_id=lake, credentials=credentials)

    for zone in list_zones:
        print(f'zone: {zone}')
        df_assets_by_zone = list_all_assets(zone_id=zone, credentials=credentials)
        df_assets = pd.concat([df_assets_by_zone,df_assets],ignore_index=True).reset_index(drop = True)

#filtering only important columns, you can add more if you want
df_assets = df_assets[['name', 'createTime', 'updateTime', 'state',
    'resourceSpec.name', 'resourceSpec.type', 'resourceStatus.state',
    'resourceStatus.updateTime', 'securityStatus.state',
    'securityStatus.updateTime', 'discoverySpec.enabled',
    'discoverySpec.csvOptions.delimiter',
    'discoverySpec.csvOptions.encoding',
    'discoverySpec.jsonOptions.encoding', 'discoverySpec.schedule',
    'discoveryStatus.state', 'discoveryStatus.updateTime',
    'discoveryStatus.lastRunTime', 'discoveryStatus.stats.dataItems',
    'discoveryStatus.stats.dataSize', 'discoveryStatus.stats.tables',
    'discoveryStatus.lastRunDuration']]

#renaming columns
df_assets.rename(columns = {'name':'asset_id',
'createTime': 'create_time_asset', 
'updateTime': 'update_time_asset', 
'state': 'state_asset',
'resourceSpec.name': 'full_name_resource_spec', 
'resourceSpec.type': 'type_resource_spec', 
'resourceStatus.state': 'state_resource_status',
'resourceStatus.updateTime': 'update_time_resource_status', 
'securityStatus.state': 'state_security_status',
'securityStatus.updateTime': 'update_time_security_status', 
'discoverySpec.enabled': 'enabled_discovery_spec',
'discoverySpec.csvOptions.delimiter': 'csv_options_delimiter_discovery_spec',
'discoverySpec.csvOptions.encoding': 'csv_options_encoding_discovery_spec',
'discoverySpec.jsonOptions.encoding': 'json_options_encoding_discovery_spec', 
'discoverySpec.schedule': 'schedule_discovery_spec',
'discoveryStatus.state': 'state_discovery_status', 
'discoveryStatus.updateTime': 'update_time_discovery_status',
'discoveryStatus.lastRunTime': 'lastrun_time_discovery_status', 
'discoveryStatus.stats.dataItems': 'data_items_discovery_status',
'discoveryStatus.stats.dataSize': 'data_size_discovery_status', 
'discoveryStatus.stats.tables': 'stats_tables_discovery_status',
'discoveryStatus.lastRunDuration': 'lastrun_duration_discovery_status'}, inplace=True)


df_assets['date_extraction'] = datetime.today().date()
#df_assets['date_extraction'] = datetime(2022,9,12)
df_assets['log_time'] = datetime.today()


df_assets['project_asset'] = df_assets.asset_id.str.split('/').str[1]
df_assets['location_asset'] = df_assets.asset_id.str.split('/').str[3]
df_assets['lake_asset'] = df_assets.asset_id.str.split('/').str[5]
df_assets['zone_asset'] = df_assets.asset_id.str.split('/').str[7]
df_assets['name_asset'] = df_assets.asset_id.str.split('/').str[9]

#SPliting project, from the resource
df_assets['project_resource_spec'] = df_assets.full_name_resource_spec.str.split('/').str[1]

df_assets['name_resource_spec'] = df_assets.full_name_resource_spec.str.split('/').str[-1]

df_assets.drop(columns=['asset_id','full_name_resource_spec'], inplace=True)

#Treating dataframe columns
df_assets['create_time_asset'] = pd.to_datetime(df_assets['create_time_asset'])
df_assets['create_time_asset'] = df_assets['create_time_asset'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
df_assets['create_time_asset'] = df_assets['create_time_asset'].astype('datetime64[us]')

df_assets['update_time_asset'] = pd.to_datetime(df_assets['update_time_asset'])
df_assets['update_time_asset'] = df_assets['update_time_asset'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
df_assets['update_time_asset'] = df_assets['update_time_asset'].astype('datetime64[us]')

df_assets['update_time_resource_status'] = pd.to_datetime(df_assets['update_time_resource_status'])
df_assets['update_time_resource_status'] = df_assets['update_time_resource_status'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
df_assets['update_time_resource_status'] = df_assets['update_time_resource_status'].astype('datetime64[us]')

df_assets['update_time_security_status'] = pd.to_datetime(df_assets['update_time_security_status'])
df_assets['update_time_security_status'] = df_assets['update_time_security_status'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
df_assets['update_time_security_status'] = df_assets['update_time_security_status'].astype('datetime64[us]')

df_assets['update_time_discovery_status'] = pd.to_datetime(df_assets['update_time_discovery_status'])
df_assets['update_time_discovery_status'] = df_assets['update_time_discovery_status'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
df_assets['update_time_discovery_status'] = df_assets['update_time_discovery_status'].astype('datetime64[us]')

df_assets['lastrun_time_discovery_status'] = pd.to_datetime(df_assets['lastrun_time_discovery_status'])
df_assets['lastrun_time_discovery_status'] = df_assets['lastrun_time_discovery_status'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
df_assets['lastrun_time_discovery_status'] = df_assets['lastrun_time_discovery_status'].astype('datetime64[us]')

df_assets.date_extraction = pd.to_datetime(df_assets['date_extraction'])

df_assets.data_items_discovery_status = df_assets.data_items_discovery_status.fillna(0)
df_assets.data_items_discovery_status = df_assets.data_items_discovery_status.astype('int64')

df_assets.data_size_discovery_status = df_assets.data_size_discovery_status.fillna(0)
df_assets.data_size_discovery_status = df_assets.data_size_discovery_status.astype('int64')


df_assets.stats_tables_discovery_status = df_assets.stats_tables_discovery_status.fillna(0)
df_assets.stats_tables_discovery_status = df_assets.stats_tables_discovery_status.astype('int64')

df_assets.lastrun_duration_discovery_status = df_assets.lastrun_duration_discovery_status.str.replace('s','').astype('float')


#Table Schema
table_schema_file = "table_schema.json"
table_schema_json = open(table_schema_file)
table_schema = json.load(table_schema_json)

#bigquery informations
dataset_name = 'dataset'
gbq_table= 'dataplex_assets_analysis'
project_gcp = 'project'


print('Inserting data to bigquery')
df_assets.to_gbq(destination_table = dataset_name+"."+gbq_table,
                            project_id = project_gcp,
                            if_exists='append',
                            chunksize=1000000,
                            table_schema=table_schema)
