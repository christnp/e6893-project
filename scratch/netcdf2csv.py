# converst netCDF to Python
# https://joehamman.com/2013/10/12/plotting-netCDF-data-with-Python/
from netCDF4 import Dataset
import numpy as np
import os
import matplotlib.pyplot as plt
# from mpl_toolkits.basemap import Basemap

nc_dir = '/home/christnp/Development/e6893/homework/e6893-project/scratch/data/ftp.star.nesdis.noaa.gov-static/'
nc_file = 'VHP.G04.C07.NN.P2006001.VH.nc'
nc_path = os.path.join(nc_dir,nc_file)
fh = Dataset(nc_path, mode='r')
# easting = fh.variables['easting'][:]
# northing = fh.variables['northing'][:]
# tmax = fh.variables['Tmax01'][:]

print(fh.variables['VHI'][3615][9999])
# lon_0 = easting.mean()
# lat_0 = northing.mean()
