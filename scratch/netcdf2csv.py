# converst netCDF to Python
# https://joehamman.com/2013/10/12/plotting-netCDF-data-with-Python/
from netCDF4 import Dataset
import numpy as np
import os
import matplotlib.pyplot as plt
# from mpl_toolkits.basemap import Basemap

nc_dir = '/home/christnp/Downloads/INM-CM4_rcp45_2025_Monthly_netCDF/'
nc_file = 'INM-CM4_rcp45_2025_Tmax01.nc'
nc_path = os.path.join(nc_dir,nc_file)
fh = Dataset(nc_path, mode='r')
easting = fh.variables['easting'][:]
northing = fh.variables['northing'][:]
tmax = fh.variables['Tmax01'][:]

lon_0 = easting.mean()
lat_0 = northing.mean()
