import salem
import xarray as xr
import numpy as np
import json
import os
import sys
import pprint

import pandas as pd
from functools import reduce

SALEM = False

if SALEM:
    ncfile = "temp/ftp.star.nesdis.noaa.gov-static/VHP.G04.C07.NN.P2006001.VH.nc"
    da = xr.DataArray(np.arange(20).reshape(4, 5), dims=['lat', 'lon'],
                        coords={'lat':np.linspace(0, 30, 4),
                                'lon':np.linspace(-20, 20, 5)})
    # dse = salem.open_xr_dataset(salem.get_demo_file('era_interim_tibet.nc'))
    # dse = salem.open_xr_dataset(ncfile)
    print(da.salem)

    da.salem.quick_map()

# load state json
# project_base = "/home/christnp/Development/e6893/homework/e6893-project/src/usheatmap/.tmp/static/"
project_base = "/home/christnp/Development/e6893/homework/e6893-project/src/usheatmap/.tmp/"
files = [f for f in os.listdir(project_base) if os.path.isfile(os.path.join(project_base, f)) and f.endswith(".json")]
    # if file.endswith(".txt"):
files.sort()

temp = {}
for f in files:
    json_file = os.path.join(project_base,f)

    f_split = f.split('_')
    state = f_split[0]
    product = f_split[1]
    # date = f_split[2].split('-')[1]

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

    common = ['name','date']
    dfs = []
    for dict in [tasmin_data,tasmax_data,pr_data,vci_data,tci_data,vhi_data]: #[pr_data,vci_data]:
    # for dict in [vhi_data]: #[pr_data,vci_data]:
        for tmp in dict:
            df = pd.DataFrame(tmp)  
            # Need to clean-up some of the data; rename mean column as type and remove type column
            # TODO: future revision will properly name columns
            # ref: https://stackoverflow.com/questions/19758364/rename-specific-columns-in-pandas
            try:
                df.rename(columns={'mean':tmp[0]['type'].lower()}, inplace=True)
                df.drop(columns=['type','fips'],inplace=True) # the fips column is redundant
                dfs.append(df)
            except Exception as e:
                print("Error: {} \ndata: {}".format(e,tmp))

    # ref: https://stackoverflow.com/questions/23668427/pandas-three-way-joining-multiple-dataframes-on-columns
    df_final = reduce(lambda left, right: pd.merge(left,right, on=common), dfs)
    print(df_final)
   
    sys.exit()

        # db_dict['county'].append()
        # db_dict['date'] = []
        # db_dict['tasmin'] = []
        # db_dict['tasmax'] = []
        # db_dict['pr'] = []
        # db_dict['vci'] = []
        # db_dict['tci'] = []
        # db_dict['vhi'] = []

        # break

# df = pandas.DataFrame(
#     {
#         "my_string": ["a", "b", "c"],
#         "my_int64": [1, 2, 3],
#         "my_float64": [4.0, 5.0, 6.0],
#         "my_bool1": [True, False, True],
#         "my_bool2": [False, True, False],
#         "my_dates": pandas.date_range("now", periods=3),
#     }
# )

# pandas_gbq.to_gbq(df, table_id, project_id=project_id)

sys.exit()
db_dict = {}
db_dict['data'] = []
for f in files:
    json_file = os.path.join(project_base,f)
    print("loading: {}".format(f))
    try: 
        data = json.load(open(json_file))
    except Exception as e:
        print(e)
        continue

    # (county,date,tmin,tmax,precip,vci,tci,vhi)
    for element in data:
        
        db_dict['data'].append({
            # 'year':         vh_date[0].year,
            'county':         element['name'],
            'data':         element['date'],
            'lon':          vh_x_lon,
            'lon_start':    vh_x_lon_range[0]
        })
        print(element)
    sys.exit()
    pprint.pprint(data)

