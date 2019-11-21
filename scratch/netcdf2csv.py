# converst netCDF to Python
# https://joehamman.com/2013/10/12/plotting-netCDF-data-with-Python/
from netCDF4 import Dataset
import numpy as np
import os
import matplotlib.pyplot as plt
import xarray as xr
import sys
# from mpl_toolkits.basemap import Basemap

DEBUG = 3           # Debug levels. 0=no output, 1=results, 2=(1)+variables, 3=(2)+debug
console = ""        # Console output string

########################### Vegetation Health Parser ###########################
nc_dir = '/home/christnp/Development/e6893/homework/e6893-project/scratch/data/ftp.star.nesdis.noaa.gov-static/'
nc_file = 'VHP.G04.C07.NN.P2006001.VH.nc'
nc_path = os.path.join(nc_dir,nc_file)
fh = Dataset(nc_path, mode='r')

# for var in fh.variables.values():
#     print(var[590][377])
#     print("+++++++++++++++++++++++++++++++++++++++++")

# tci_array = np.asarray(tci[:][:],dtype=np.float32)
# print(tci_array[590][377])
# print(fh.dimensions)


# variables
# for name in fh.ncattrs():
    # print("{0}: {1}".format(name, getattr(fh, name)))

product = getattr(fh,'PRODUCT_NAME')

year = getattr(fh,'YEAR')
week = getattr(fh,'PERIOD_OF_YEAR')


y_lat_range = [getattr(fh,'START_LATITUDE_RANGE'),
            getattr(fh,'END_LATITUDE_RANGE')]
x_lon_range = [getattr(fh,'START_LONGITUDE_RANGE'),
                getattr(fh,'END_LONGITUDE_RANGE')]
# dimensions
y_height = fh.dimensions['HEIGHT'].size
x_width = fh.dimensions['WIDTH'].size

# https://carpentrieslab.github.io/python-aos-lesson/09-provenance/index.html
# dset = xr.open_dataset(nc_path)
# print(dset)
# print(dset.dims['WIDTH'])
# print(dset.values)
# print(dset.var)
# print(dset.coords)
# get the data
# product = dset.attrs['PRODUCT_NAME']
# year = dset.attrs['YEAR']
# week = dset.attrs['PERIOD_OF_YEAR']
# y_lat_range = [dset.attrs['START_LATITUDE_RANGE'],
#             dset.attrs['END_LATITUDE_RANGE']]
# # start_lat = dset.attrs['START_LATITUDE_RANGE']
# # end_lat = dset.attrs['END_LATITUDE_RANGE']
# x_lon_range = [dset.attrs['START_LONGITUDE_RANGE'],
#                 dset.attrs['END_LONGITUDE_RANGE']]
# start_lon = dset.attrs['START_LONGITUDE_RANGE']
# end_lon = dset.attrs['END_LONGITUDE_RANGE']
# x_width = dset.dims['WIDTH']
# y_height = dset.dims['HEIGHT']

# vci_values = dset['VCI'].values
# vci_attrs = dset['VCI'].attrs
# tci_values = dset['TCI'].values
# tci_attrs = dset['TCI'].attrs
# vhi_values = dset['VHI'].values
# vhi_attrs = dset['VHI'].attrs

# vci_sf = vci_attrs['scale_factor']
# vci_ao = vci_attrs['add_offset']
# tci_sf = tci_attrs['scale_factor']
# tci_ao = tci_attrs['add_offset']
# vhi_sf = vhi_attrs['scale_factor']
# vhi_ao = vhi_attrs['add_offset']

x_step = (abs(x_lon_range[0])+abs(x_lon_range[1]))/x_width
y_step = (abs(y_lat_range[0])+abs(y_lat_range[1]))/y_height

# make some local variables from the data
# use X.scale_factor and X.add_offset for those values
tci = fh['TCI']
vci = fh['VCI']
vhi = fh['VHI']


# tci_array = np.empty(tci.shape,dtype=tci.dtype)
# tci_array = np.asarray(tci[:][:],dtype=tci.dtype)





if DEBUG > 1:
    console += "product: {}\n".format(product)
    console += "year: {}\n".format(year)
    console += "week: {}\n".format(week)
    console += "x_lon_range: {}\n".format(x_lon_range)
    console += "y_lat_range: {}\n".format(y_lat_range)
    console += "x_width: {}\n".format(x_width)
    console += "y_height: {}\n".format(y_height)
    console += "x_step: {}\n".format(x_step)
    console += "y_step: {}\n".format(y_step)
    console += "vci.scale_factor: {}\n".format(vci.scale_factor)
    console += "vci.add_offset: {}\n".format(vci.add_offset)
    console += "tci.scale_factor: {}\n".format(tci.scale_factor)
    console += "tci.add_offset: {}\n".format(tci.add_offset)
    console += "vhi.scale_factor: {}\n".format(vhi.scale_factor)
    console += "vhi.add_offset: {}\n".format(vhi.add_offset)


print(console)
sys.exit()

# for now, just take a small chunk
x_lon_bound = [-110,-90]    # some lon in the middle of the US (from W to E)
y_lat_bound = [45,35]       # some lat in the middle of the US (from N to S)

x_start = x_lon_bound[0]/x_lon_range[0] - 1
x_stop = x_lon_bound[1]
y_start = 
y_end = 

# print(tci[590][377]) # fh[var][y_height][x_width]
# print(tci.range.dtype)
# set scale factors and offsets (Value= scale_factor * (ScaledInteger - add_offset))
for y in range(0,width-1):
    for x in range(0,height-1):
        val = vci_array[x][y]
        if not np.isnan(val):
            print(val)

# for y in vci_arr.values:
#     for x in y:
#         # print(np.isnan(x))
#         if not np.isnan(x):
#             print(x)
    
#http://xarray.pydata.org/en/stable/io.html


############################## CMIP-5 LOCA Parser ##############################

