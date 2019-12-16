import pandas as pd
import pandas_gbq
from functools import reduce

from google.oauth2 import service_account
from datetime import datetime

import numpy as np
import json
import os
import sys
import pprint


# ref 1: https://pandas-gbq.readthedocs.io/en/latest/intro.html#authenticating-to-bigquery
# ref 2: https://stackoverflow.com/questions/26255671/pandas-column-values-to-columns
# ref 3: https://stackoverflow.com/questions/50741330/difference-between-df-reindex-and-df-set-index-methods-in-pandas


# load data from JSON
# project_base = "/home/christnp/Development/e6893/homework/e6893-project/src/usheatmap/.tmp/static/"
########################### UNCOMMENT BELOW #####################################
# project_base = "/home/christnp/Development/e6893/homework/e6893-project/src/usheatmap/.tmp/"
# files = [f for f in os.listdir(project_base) if os.path.isfile(os.path.join(project_base, f)) and f.endswith(".json")]
# files.sort()

# temp = {}
# for f in files:
#     json_file = os.path.join(project_base,f)

#     f_split = f.split('_')
#     state = f_split[0]
#     product = f_split[1]

#     if state not in temp:
#         temp[state] = {
#             'pr':  [],
#             'tasmin':  [],
#             'tasmax':  [],
#             'vci': [],
#             'tci': [],
#             'vhi': []
#         }
#     if (product == 'pr'):
#         temp[state]['pr'].append(json_file)
#     elif (product == 'tasmax'):
#         temp[state]['tasmax'].append(json_file)
#     elif (product == 'tasmin'):
#         temp[state]['tasmin'].append(json_file)
#     elif (product == 'VCI'):
#         temp[state]['vci'].append(json_file)
#     elif (product == 'TCI'):
#         temp[state]['tci'].append(json_file)
#     elif (product == 'VHI'):
#         temp[state]['vhi'].append(json_file)

# pr_data = []
# tasmin_data = []
# tasmax_data = []
# vci_data = []
# tci_data = []
# vhi_data = []

# print("Building dataframe...")
# for state in temp:
#     # print(temp[state])
#     # skip JSON with all for now
#     if state == 'all':
#         continue
#     pr_data = [json.load(open(x)) for x in temp[state]['pr']]
#     tasmin_data = [json.load(open(x)) for x in temp[state]['tasmin']]
#     tasmax_data = [json.load(open(x)) for x in temp[state]['tasmax']]
#     vci_data = [json.load(open(x)) for x in temp[state]['vci']]
#     tci_data = [json.load(open(x)) for x in temp[state]['tci']]
#     vhi_data = [json.load(open(x)) for x in temp[state]['vhi']]

#     common = ['date','centroid_lon','centroid_lat','county','state']
#     dfs = []
#     for dict in [tasmin_data,tasmax_data,pr_data,vci_data,tci_data,vhi_data]: #[pr_data,vci_data]:
#     # for dict in [vhi_data]: #[pr_data,vci_data]:
#         for tmp in dict:
#             df = pd.DataFrame(tmp)  
#             # Need to clean-up some of the data; rename mean column as type and remove type column
#             # TODO: future revision will properly name columns
#             # ref: https://stackoverflow.com/questions/19758364/rename-specific-columns-in-pandas
#             try:
#                 # convert date string to datetime
#                 df['date']= pd.to_datetime(df['date']) 
#                 # rename mean column to the product type
#                 df.rename(columns={'mean':tmp[0]['type'].lower()}, inplace=True)
#                 # remove the 'type' column (not needed)
#                 df.drop(columns=['type'],inplace=True) 
#                 dfs.append(df)
#             except Exception as e:
#                 print("Error: {} \ndata: {}".format(e,tmp))

#     # ref: https://stackoverflow.com/questions/23668427/pandas-three-way-joining-multiple-dataframes-on-columns
#     df_final = reduce(lambda left, right: pd.merge(left,right, on=common), dfs)
########################### UNCOMMENT ABOVE #####################################

# start BQ
project = "eecs-e6893-edu"
# bucket = "eecs-e6893-edu"  
# tmp_dir = 'gs://{}/hadoop/tmp/bigquery/pyspark_output/usheatmap'.format(bucket)
dataset = 'usheatmap' #the name of output dataset in BigQuery
# table_name = 'initial'
# table_name = 'debug'
table_name = 'final'
table_id = '{0}.{1}'.format(dataset,table_name)

credentials = service_account.Credentials \
                .from_service_account_file('/home/christnp/Development/e6893/gcp/EECS-E6893-edu-f676d83967fd.json')

# use pandas_gbq to read BigQuery table into dataframe
pandas_gbq.context.credentials = credentials
pandas_gbq.context.project = project

# instantiate the GCP interface
# gcpif = GCPInterface(project,bucket,tmp_dir,dataset,vh_table,vh_cols)
# gcpif.run(vh_json,vh_schema)
# gcpif.saveToBigQuery(sc, dataset, vh_table, tmp_dir)    
# print("Node results stored in BigQuery table \'{0}.{1}\'...".format(dataset, vh_table)) 
#DATA_PATH = "gs://eecs-e6893-edu/input/hw2/q1.txt"


data = {}
data['data'] = []

# query the table, return as pandas df.
# SQL = "SELECT * FROM `{}` ORDER BY date ASC LIMIT 8".format(table_id)
SQL = "SELECT * FROM `{}` ORDER BY date ASC".format(table_id)
df = pandas_gbq.read_gbq(SQL)
df.fillna(-1,inplace=True) # fill NaN/Na with -1
# iterate each row of the dataframe

def getGeoid(row):
    statefp = row['state'].split("_")[0]
    countyfp = row['county'].split("_")[0]
    return "{}{}".format(statefp,countyfp) 

# add the geoid (FIXME: this should have been included in original processing)
df['geoid'] = df.apply (lambda row: getGeoid(row), axis=1)
# j = (df.groupby(['date','geoid'], as_index=False)
#              .apply(lambda x: x[['vci','tci','vhi','tasmin','tasmax','pr']].to_dict('r'))
#              .reset_index()
#              .rename(columns={0:'Test'})
#              .to_json(orient='records'))

# pprint.pprint(j)
# sys.exit()
tmp = {}
for index, row in df.iterrows():    
    dt_date = row['date'].to_pydatetime().strftime('%Y-%m-%d')
    if (dt_date != "2018-01-08"):
        continue;
    # statefp = row['state'].split("_")[0]
    # countyfp = row['county'].split("_")[0]
    # geoid = "{}{}".format(statefp,countyfp) 
    # geoid = SSCCC, SS = State FIPS, CCC = County FIPS
    # BQ has state = SS_Name, county = CCC_Name

    tmp = { 'date' : dt_date, \
            'geoid': row['geoid'], \
            'value' : { 'vci' : row['vci'], \
                        'tci':row['tci'], \
                        'vhi':row['vhi'], \
                        'tasmin':row['tasmin'], \
                        'tasmax':row['tasmax'], \
                        'pr':row['pr'] \
                    } \
        }


    # 'date1': [
    #   {
    #       'geoid': geoid1,
    #       'values':   {
    #                       'vci': vci,
    #                       'tci': tci,
    #                       ...    
    #                   }
    #   },
    #   {
    #       'geoid': geoid2,
    #       'values':   {
    #                       'vci': vci,
    #                       'tci': tci,
    #                       ...    
    #                   }
    #   }, ...
    # ]
    #
    #  # 'date2': [
    #   {
    #       'geoid': geoid1,
    #       'values':   {
    #                       'vci': vci,
    #                       'tci': tci,
    #                       ...    
    #                   }
    #   },
    #   {
    #       'geoid': geoid2,
    #       'values':   {
    #                       'vci': vci,
    #                       'tci': tci,
    #                       ...    
    #                   }
    #   }, ...
    # ]

    data['data'].append(tmp)



pprint.pprint(data)
# print("Uploading \'df_final\' dataframe to \'{}\' table.".format(table_id))
# print(df_final.head())
# pandas_gbq.to_gbq(df_final, table_id, project_id=project,if_exists='replace')