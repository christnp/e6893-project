import matplotlib.pyplot as plt
import numpy as np
import json
import os
import sys

from netCDF4 import Dataset

DEBUG = 3           # Debug levels. 0=no output, 1=results, 2=(1)+variables, 3=(2)+debug
console = ""        # Console output string

project_base = "/home/christnp/Development/e6893/homework/e6893-project/"
#################################### CMIP/VH ###################################

vh_dir = 'data/ftp.star.nesdis.noaa.gov-static/'
vh_file = 'VHP.G04.C07.NN.P2006001.VH.nc'
vh_path = os.path.join(project_base,vh_dir,vh_file)

c5_dir = 'data/gdo-dcp.ucllnl.org-static/'
c5_file = 'pr_day_inmcm4_rcp45_r1i1p1_20060101-20061231.LOCA_2016-04-02.16th.nc'
c5_path = os.path.join(project_base,c5_dir,c5_file)

vh = Dataset(vh_path, mode='r')
c5 = Dataset(c5_path, mode='r')

## VH 
# attributes
vh_product     = getattr(vh,'PRODUCT_NAME')
vh_year        = getattr(vh,'YEAR')
vh_week        = getattr(vh,'PERIOD_OF_YEAR')
vh_y_lat_range = [getattr(vh,'START_LATITUDE_RANGE'), getattr(vh,'END_LATITUDE_RANGE')]
vh_x_lon_range = [getattr(vh,'START_LONGITUDE_RANGE'), getattr(vh,'END_LONGITUDE_RANGE')]
# dimensions
vh_y_height    = vh.dimensions['HEIGHT'].size
vh_x_width     = vh.dimensions['WIDTH'].size
# variables
tci = vh['TCI']
vci = vh['VCI']
vhi = vh['VHI']

vh_x_step = (abs(vh_x_lon_range[0])+abs(vh_x_lon_range[1]))/vh_x_width
vh_y_step = (abs(vh_y_lat_range[0])+abs(vh_y_lat_range[1]))/vh_y_height


## CMIP5
# attributes
c5_model_id = getattr(c5,'model_id')
# dimensions
c5_bands_dim    = c5.dimensions['bnds'].size
c5_y_lat_dim    = c5.dimensions['lat'].size
c5_x_lon_dim    = c5.dimensions['lon'].size
c5_time_dim     = c5.dimensions['time'].size
# variables
c5_lon          = c5['lon']
c5_lon_bands    = c5['lon_bnds']
c5_lat          = c5['lat']
c5_lat_bands    = c5['lat_bnds']
c5_time         = c5['time']
c5_time_bnds    = c5['time_bnds']
c5_pr           = c5['pr']


if DEBUG > 1:
    # CMIP5-LOCA
    console += "\nCMIP5-LOCA Specs \n"
    console += "c5_model_id: {}\n".format(c5_model_id)
    console += "c5_bands_dim: {}\n".format(c5_bands_dim)
    console += "c5_y_lat_dim: {}\n".format(c5_y_lat_dim)
    console += "c5_x_lon_dim: {}\n".format(c5_x_lon_dim)
    console += "c5_time_dim:  {}\n".format(c5_time_dim)
    # VH
    console += "\nVH Specs \n"
    console += "vh_product: {}\n".format(vh_product)
    console += "vh_year: {}\n".format(vh_year)
    console += "vh_week: {}\n".format(vh_week)
    console += "vh_x_lon_range: {}\n".format(vh_x_lon_range)
    console += "vh_y_lat_range: {}\n".format(vh_y_lat_range)
    console += "vh_x_width: {}\n".format(vh_x_width)
    console += "vh_y_height: {}\n".format(vh_y_height)
    console += "x_step: {}\n".format(vh_x_step)
    console += "y_step: {}\n".format(vh_y_step)
    console += "vci.scale_factor: {}\n".format(vci.scale_factor)
    console += "vci.add_offset: {}\n".format(vci.add_offset)
    console += "tci.scale_factor: {}\n".format(tci.scale_factor)
    console += "tci.add_offset: {}\n".format(tci.add_offset)
    console += "vhi.scale_factor: {}\n".format(vhi.scale_factor)
    console += "vhi.add_offset: {}\n".format(vhi.add_offset)

    print(console)

sys.exit()

# for i in range(0,c5_time-1):
#     print(c5_pr[i][400][200])

# C5 variables(dimensions): 
# float64 lon(lon), float64 lon_bnds(lon,bnds), 
# float64 lat(lat), float64 lat_bnds(lat,bnds), 
# float64 time(time), float64 time_bnds(time,bnds), 
# float32 pr(time,lat,lon)

geo_cart_map = np.empty(tci.shape, dtype=(tci.range.dtype,2))

# print(geo_cart_map.shape)
# for xi in range(1,geo_cart_map.shape[1]):
#     for yi in range(1,geo_cart_map.shape[0]):
#         geo_cart_map[yi][xi] =  [xi*x_step+x_lon_range[0], yi*y_step+y_lat_range[0]]

test = np.fromfunction(lambda x, y: (x*vh_x_step+vh_x_lon_range[0], y*vh_y_step+vh_y_lat_range[0]), tci.shape, dtype=tci.range.dtype)

print(test[0][0].size)
# print(geo_cart_map)
sys.exit()

#################################### SHAPELY ###################################
import shapely.geometry
import geopandas as gpd
import shapely.speedups

# Data sources
# For State/County/Region shapefiles:
#   - https://www.census.gov/geographies/mapping-files/time-series/geo/carto-boundary-file.html
#   - https://www2.census.gov/geo/tiger/TIGER2019/
#   ++ census data was read in as .shp and written out as .json (GeoJSON) using GeoPandas
############
def readGeoJSON(json_path):    
    try:
        gpd_out = gpd.read_file(json_path, driver='GeoJSON')
    except TypeError as e:
        print("Couldn't load JSON... requires \'UTF-8\' encoding...")
        print("TypeError: {}".format(e))
        print("Try to convert encoding from \'ISO-8859-1\'and try again")
        cur_json = json.load(open(json_path, encoding='ISO-8859-1'))
        path,ext = os.path.splitext(json_path)
        new_path =path+"_new"+ext
        with open(new_path,"w", encoding='utf-8') as jsonfile:
                json.dump(cur_json,jsonfile,ensure_ascii=False)
        print("Created a new GeoJSON with correct \'UTF-8\' encoding:")
        print("> {}".format(new_path))
        gpd_out = gpd.read_file(new_path, driver='GeoJSON')
    return gpd_out
############

shapely.speedups.enable()
# load state FIPS codes
us_fips_path = "../data/geojson/cont_stateToFips.json"
try: 
    us_fips = json.load(open(us_fips))
except:
    print("Failed to load fips codes...")
    us_fips = {"Kansas"                 :  "20"}

us_border_path = "../data/geojson/gz_2010_us_outline_500k.json"
us_states_path = "../data/geojson/gz_2010_us_040_00_500k.json"
us_county_path = "../data/geojson/gz_2010_us_050_00_500k.json"
# us_border = gpd.read_file(us_border_path, driver='GeoJSON')
# us_states = gpd.read_file(us_states_path, driver='GeoJSON')
# us_county = gpd.read_file(us_county_path, driver='GeoJSON')
us_border = readGeoJSON(us_border_path)
us_states = readGeoJSON(us_states_path)
us_county = readGeoJSON(us_county_path)

oregon = us_states.loc[us_states['NAME']=='Oregon'] # replaced .ix with .loc per warning
oregon.reset_index(drop=True, inplace=True) # what does this do?

# https://stackoverflow.com/questions/44399749/get-all-lattice-points-lying-inside-a-shapely-polygon
# points = MultiPoint(VH_data_coords) #np.transpose([np.tile(x, len(y)), np.repeat(y, len(x))]))
# bounded_ind = points.intersection(state.loc[0,'geometry'])
# print(bounded_ind)
# data_for_analysis = VH_data[bounded_ind]
# pip_data = oregon.loc[pip_mask]

# plotting 
# ref: http://geopandas.org/mapping.html