from usheatmap.UshmDataScraper import UshmDataScraper
from usheatmap.UshmDataParser import UshmDataParser
from usheatmap.GCPInterface import GCPInterface

import pandas_gbq

from urllib.parse import urlparse
import pprint
import os
import sys


GET_VH = False
PARSE_VH = True
GET_C5 = False
PARSE_C5 = False
vh_src_url = "ftp://ftp.star.nesdis.noaa.gov/pub/corp/scsb/wguo/data/Blended_VH_4km/VH/"
c5_src_url = "ftp://gdo-dcp.ucllnl.org/pub/dcp/archive/cmip5/loca/LOCA_2016-04-02/"

project_base = "/home/christnp/Development/e6893/homework/e6893-project/"
vh_dir = 'scratch/temp/ftp.star.nesdis.noaa.gov-static/'
c5_dir = 'scratch/temp/gdo-dcp.ucllnl.org/'

scraper = UshmDataScraper()
parser = UshmDataParser()

if GET_VH:
    # scrape
    vh_src = urlparse(vh_src_url)
    vh_storage = "temp/temp_{}".format(vh_src.netloc)
    scraper.getVegHealthData(vh_src.geturl(),vh_storage,debug=3)
if PARSE_VH:
    # parse
    vh_path = os.path.join(project_base,vh_dir)
    files = [f for f in os.listdir(vh_path) if os.path.isfile(os.path.join(vh_path, f))]
    for f in files:
        product = f.split(".")[-2]
        print("parsing: {},{}".format(f,product))

        vh_path = os.path.join(project_base,vh_dir,f)
        vh_json = parser.parseVH(vh_path)#,product=product)
        # pprint.pprint(vh_json)

if GET_C5:
    # scrape
    c5_src = urlparse(c5_src_url)
    c5_storage = "temp/temp_{}".format(c5_src.netloc)
    scraper.getCmipModelData(c5_src.geturl(),c5_storage,model='inmcm4',
                        experiment='rcp45',spatial="16th",debug=3) 
if PARSE_C5:
    # parse
    c5_path = os.path.join(project_base,c5_dir)
    files = [f for f in os.listdir(c5_path) if os.path.isfile(os.path.join(c5_path, f))]
    for f in files:
        product = f.split("_")[0]
        print("parsing: {},{}".format(f,product))

        c5_path = os.path.join(project_base,c5_dir,f)
        c5_json = parser.parseCmip(c5_path,product=product)
        pprint.pprint(c5_json)

# start BQ
project = "eecs-e6893-edu"
bucket = "eecs-e6893-edu"  
tmp_dir = 'gs://{}/hadoop/tmp/bigquery/pyspark_output/usheatmap'.format(bucket)
dataset = 'usheatmap' #the name of output dataset in BigQuery
vh_table_name = 'vh'
vh_table = '{0}.{1}'.format(dataset,vh_table_name)
vh_cols = ['date','vci','tci','vhi']
vh_schema = []

# instantiate the GCP interface
# gcpif = GCPInterface(project,bucket,tmp_dir,dataset,vh_table,vh_cols)
# gcpif.run(vh_json,vh_schema)
# gcpif.saveToBigQuery(sc, dataset, vh_table, tmp_dir)    
# print("Node results stored in BigQuery table \'{0}.{1}\'...".format(dataset, vh_table)) 
#DATA_PATH = "gs://eecs-e6893-edu/input/hw2/q1.txt"
import pandas
import pandas_gbq
from google.oauth2 import service_account

credentials = service_account.Credentials \
                .from_service_account_file('/home/christnp/Development/e6893/gcp/EECS-E6893-edu-f676d83967fd.json')
# use pandas_gbq to write dataframe into BigQuery table
pandas_gbq.context.credentials = credentials
pandas_gbq.context.project = project

print("Building dataframe...")
# df = pandas.DataFrame(vh_json)
df = pandas.io.json.json_normalize(vh_json) #https://stackoverflow.com/questions/34341974/nested-json-to-pandas-dataframe-with-specific-format
df.head()
#pandas_gbq.to_gbq(df, vh_table, project_id=project)



