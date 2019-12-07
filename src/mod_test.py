from usheatmap.UshmDataScraper import UshmDataScraper
from usheatmap.UshmDataParser import UshmDataParser
from usheatmap.UshmDataPreprocessor import UshmDataPreprocessor
from usheatmap.GCPInterface import GCPInterface
from usheatmap.UshmUtils import UshmUtils

from datetime import datetime
from urllib.parse import urlparse
import pprint
import os
import sys
import json
import numpy as np

import pandas as pd
import pandas_gbq
from functools import reduce
from google.oauth2 import service_account
from datetime import datetime

VERBOSITY = 3    # sets debug level: 0=not verbose, 4=max verbosity
QUICK = False     # set TRUE to process only the first week (c5)

GET_VH = False      # set TRUE to retrieve raw VH data from FTP server
PARSE_VH = False     # set TRUE to parse VH data
SAVE_VH = False     # set TRUE to save parsed output as local JSON file
# Set PARSE_VH to FALSE and SAVE_VH to TRUE to load already saved JSON file without
# reparsing/resaving data as JSON

GET_C5 = False      # set TRUE to retrieve raw CMIP-5 data from FTP server
PARSE_C5 = False    # set TRUE to parse CMIP-5 data
SAVE_C5 = True     # set TRUE to save parsed output as local JSON file
# Set PARSE_C5 to FALSE and SAVE_C5 to TRUE to load already saved JSON file without
# reparsing/resaving data as JSON

SAVE_BQ = True


def saveUshmData(path,data,product,dateform='%Y-%m-%d'):
    """
    Saves JSON data in standard output file format.
    """
    dates = data['attr'][0]['date']
    date_start = datetime.strptime(dates[0], "%Y-%m-%d %H:%M:%S").date().strftime(dateform)
    date_end = datetime.strptime(dates[-1], "%Y-%m-%d %H:%M:%S").date().strftime(dateform)
    json_file = "{}_json_{}_{}.json".format(product.lower(),date_start,date_end)
    json_path = os.path.join(path,json_file)
    print("[{}] Saving {} JSON file as \'{}\'".format(__file__,product,json_path))
    with open(json_path, 'w') as outfile:
        try:
            json.dump(data, outfile)
        except Exception as e:
            print("[{}] Failed to save \'{}\'. Error: {}".format(__file__,json_path,e))
    
def loadUshmData(path,prod):
    """
    JSON data generator. Yields JSON data from JSON files in path if they
    are of type 'prod'
    """
    json_files = [f for f in os.listdir(temp_dir) if os.path.isfile(os.path.join(temp_dir, f)) and prod in f]
    for json_file in json_files:
        json_path = os.path.join(path,json_file)
        try:
            print("[{}] Loading {} JSON file \'{}\'".format(__file__,prod,json_path))
            yield json.load(open(json_path))
        except Exception as e:
            print("[{}] Failed to open \'{}\'. Error: {}".format(__file__,json_path,e))
            yield None


################## TEST DIRECTORIES; USER DEPENDENT ############################
project_base = "/home/christnp/Development/e6893/homework/e6893-project/"
# vh_dir = 'scratch/temp/ftp.star.nesdis.noaa.gov-static/'
# c5_dir = 'scratch/temp/gdo-dcp.ucllnl.org/'

# for local debug/development
temp_dir = ".tmp/"


if not os.path.exists(temp_dir):
    os.makedirs(temp_dir)

################################################################################

vh_src_url = "ftp://ftp.star.nesdis.noaa.gov/pub/corp/scsb/wguo/data/Blended_VH_4km/VH/"
c5_src_url = "ftp://gdo-dcp.ucllnl.org/pub/dcp/archive/cmip5/loca/LOCA_2016-04-02/"

scraper = UshmDataScraper()
parser = UshmDataParser()
preproc = UshmDataPreprocessor()

vh_src = urlparse(vh_src_url)
# vh_storage = "temp/temp_{}".format(vh_src.netloc)
vh_dir = os.path.join(temp_dir,'temp_'+vh_src.netloc)#'scratch/temp/ftp.star.nesdis.noaa.gov-static/'
c5_src = urlparse(c5_src_url)
# c5_storage = "temp/temp_{}".format(c5_src.netloc)
c5_dir = os.path.join(temp_dir,'temp_'+c5_src.netloc)

if GET_VH:
    # scrape
    years = [str(x) for x in list(range(2018,2020))]
    for year in years:
        scraper.getVegHealthData(vh_src.geturl(),vh_dir,date=year)

if GET_C5:
    # scrape
    scraper.getCmipModelData(c5_src.geturl(),c5_dir,model='inmcm4',
                        experiment='rcp45',spatial="16th",debug=3) 


if PARSE_VH:
    files = [f for f in os.listdir(vh_dir) if os.path.isfile(os.path.join(vh_dir, f))]
    for f in files:
        prod_path = os.path.join(vh_dir,f)    
        product = f.split(".")[-2]
        vh_json = parser.parseVH(prod_path)#,product=product)
        saveUshmData(temp_dir,vh_json,product)

if PARSE_C5:
    # parse    # c5_path = c5_dir #os.path.join(project_base,c5_dir)
    files = [f for f in os.listdir(c5_dir) if os.path.isfile(os.path.join(c5_dir, f))]
    for f in files:
        prod_path = os.path.join(c5_dir,f)        
        product = f.split("_")[0]
        c5_json = parser.parseCmip(prod_path,product=product)
        saveUshmData(temp_dir,c5_json,product)
    
   

# for preprocessor
# state_fips = {"Washington"             :  "53"}
# state_fips = {"Louisiana"              :  "22"}
# state_fips = {"Maine"              :  "23"}
state_fips = {
   "Connecticut"            :  "09",
   "Maine"                  :  "23",
   "Massachusetts"          :  "25",
   "New Hampshire"          :  "33",
   "Rhode Island"           :  "44",
   "Vermont"                :  "50"
}

# state_fips = []

if SAVE_VH:
    vh_list = loadUshmData(temp_dir,prod="vh")
    # preprocessor
    for vh_json in vh_list:
        if QUICK:
            preproc.run(vh_json,fips=state_fips,target=['TCI'], plot=True,debug=VERBOSITY) # plot =>savefig, target=['TCI','VHI'],
        else:
            preproc.run(vh_json,fips=state_fips,plot=True,debug=VERBOSITY) # plot =>savefig, target=['TCI','VHI'],


if SAVE_C5:
    for prod in ['pr','tasmin','tasmax']:
        c5_list = loadUshmData(temp_dir,prod=prod)
        # preprocessor
        for c5_json in c5_list:
            if QUICK:
                preproc.run(c5_json,fips=state_fips,limit=1,plot=True,debug=VERBOSITY) # plot =>savefig, target=['tasmax'], limit=3,
            else:
                preproc.run(c5_json,fips=state_fips,plot=True,debug=VERBOSITY) # plot =>savefig, target=['tasmax'], limit=3,

################################################################################

############################## Save to BQ Table ################################
if SAVE_BQ:
    # ref 1: https://pandas-gbq.readthedocs.io/en/latest/intro.html#authenticating-to-bigquery
    # ref 2: https://stackoverflow.com/questions/26255671/pandas-column-values-to-columns
    # ref 3: https://stackoverflow.com/questions/50741330/difference-between-df-reindex-and-df-set-index-methods-in-pandas

    # load data from JSON
    # preproc_base = "/home/christnp/Development/e6893/homework/e6893-project/src/usheatmap/.tmp/static/"
    preproc_base = "/home/christnp/Development/e6893/homework/e6893-project/src/usheatmap/.tmp/"
    # vh_dir = os.path.join(temp_dir,'temp_'+vh_src.netloc)#'scratch/temp/ftp.star.nesdis.noaa.gov-static/'

    files = [f for f in os.listdir(preproc_base) if os.path.isfile(os.path.join(preproc_base, f)) and f.endswith(".json")]
    files.sort()
    temp = {}
    for f in files:
        json_file = os.path.join(preproc_base,f)

        f_split = f.split('_')
        state = f_split[0]
        product = f_split[1]

        if state not in temp:
            temp[state] = {
                'pr':  [],
                'tasmin':  [],
                'tasmax':  [],
                'vci': [],
                'tci': [],
                'vhi': []
            }
        if (product == 'pr'):
            temp[state]['pr'].append(json_file)
        elif (product == 'tasmax'):
            temp[state]['tasmax'].append(json_file)
        elif (product == 'tasmin'):
            temp[state]['tasmin'].append(json_file)
        elif (product == 'VCI'):
            temp[state]['vci'].append(json_file)
        elif (product == 'TCI'):
            temp[state]['tci'].append(json_file)
        elif (product == 'VHI'):
            temp[state]['vhi'].append(json_file)

    pr_data = []
    tasmin_data = []
    tasmax_data = []
    vci_data = []
    tci_data = []
    vhi_data = []
    print("[{}] Building dataframe...".format(__file__))
    for state in temp:
        # print(temp[state])
        # skip JSON with all for now
        if state == 'all':
            continue
        pr_data = [json.load(open(x)) for x in temp[state]['pr']]
        tasmin_data = [json.load(open(x)) for x in temp[state]['tasmin']]
        tasmax_data = [json.load(open(x)) for x in temp[state]['tasmax']]
        vci_data = [json.load(open(x)) for x in temp[state]['vci']]
        tci_data = [json.load(open(x)) for x in temp[state]['tci']]
        vhi_data = [json.load(open(x)) for x in temp[state]['vhi']]

        common = ['date','centroid_lon','centroid_lat','state','county']
        dfs_final = []
        for dict in [tasmin_data,tasmax_data,pr_data,vci_data,tci_data,vhi_data]: #[pr_data,vci_data]:
        # for dict in [vhi_data]: #[pr_data,vci_data]:
            dfs_prod = []
            for tmp in dict:
                df = pd.DataFrame(tmp) 
                # Need to clean-up some of the data; rename mean column as type and remove type column
                # TODO: future revision will properly name columns
                # ref: https://stackoverflow.com/questions/19758364/rename-specific-columns-in-pandas
                try:
                    # convert date string to datetime
                    df['date']= pd.to_datetime(df['date']) 
                    if(tmp[0]['type'] in ['VCI','TCI','VHI']):
                        df['date'] = df["date"] +  pd.Timedelta(days=1)
                    # rename mean column to the product type
                    df.rename(columns={'mean':tmp[0]['type'].lower()}, inplace=True)
                    # remove the 'type' column (not needed)
                    df.drop(columns=['type'],inplace=True) 
                    dfs_prod.append(df)
                except Exception as e:
                    print("[{}] Error: {} \ndata: {}".format(__file__,e,tmp))
            # need to combine like products with different dates
            if dfs_prod:
                df_prod = reduce(lambda left, right: pd.merge(left,right,how="outer"), dfs_prod)
                dfs_final.append(df_prod)
            else:
                print("[{}] \'dfs_prod\' length is {}. Did no reduce.".format(__file__,len(dfs_prod)))

        # ref: https://stackoverflow.com/questions/23668427/pandas-three-way-joining-multiple-dataframes-on-columns
        if dfs_final:
            df_final = reduce(lambda left, right: pd.merge(left,right, on=common), dfs_final)
        else:
            print("[{}] \'dfs_final\' length is {}. Did no reduce.".format(__file__,len(dfs_prod)))

    # start BQ
    project = "eecs-e6893-edu"
    # bucket = "eecs-e6893-edu"  
    # tmp_dir = 'gs://{}/hadoop/tmp/bigquery/pyspark_output/usheatmap'.format(bucket)
    dataset = 'usheatmap' #the name of output dataset in BigQuery
    # table_name = 'initial'
    table_name = 'training_data'
    # table_name = 'test'
    # table_name = 'sandbox'
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

    print("[{}] Uploading \'df_final\' dataframe to \'{}\' table.".format(__file__,table_id))
    print(df_final.head())
    print("\n")
    pandas_gbq.to_gbq(df_final, table_id, project_id=project,if_exists='replace')
    # pandas_gbq.to_gbq(df_final, table_id, project_id=project,if_exists='append')

################################################################################


