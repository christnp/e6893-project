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

from usheatmap.UshmDataParser import UshmDataParser
from usheatmap.UshmDataPreprocessor import UshmDataPreprocessor

prod_path = "/home/christnp/Development/e6893/homework/e6893-project/src/.tmp/temp_ftp.star.nesdis.noaa.gov/VHP.G04.C07.npp.P2018001.VH.nc"

parser = UshmDataParser()
preproc = UshmDataPreprocessor()
product = prod_path.split(".")[-2]
vh_json = parser.parseVH(prod_path)#,product=product)
# saveUshmData(temp_dir,vh_json,product)
pprint.pprint(vh_json['attr'])