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

vh = Dataset(vh_path, mode='r') # using netCDF4 to load .nc file
c5 = Dataset(c5_path, mode='r') # using netCDF4 to load .nc file

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

vh_x_lon_step = (vh_x_lon_range[1]-vh_x_lon_range[0])/vh_x_width
vh_y_lat_step = (vh_y_lat_range[1]-vh_y_lat_range[0])/vh_y_height


## CMIP5
# attributes
c5_model_id = getattr(c5,'model_id')
# dimensions
c5_bands_dim    = c5.dimensions['bnds'].size
c5_y_height    = c5.dimensions['lat'].size
c5_x_width    = c5.dimensions['lon'].size
c5_time_dim     = c5.dimensions['time'].size
# variables
c5_lon          = c5['lon'][:]
c5_lon_bands    = c5['lon_bnds']
c5_lat          = c5['lat'][:]
c5_lat_bands    = c5['lat_bnds']
c5_time         = c5['time']
c5_time_bnds    = c5['time_bnds']
c5_pr           = c5['pr']


if DEBUG > 1:
    # CMIP5-LOCA
    console += "\nCMIP5-LOCA Specs \n"
    console += "c5_model_id: {}\n".format(c5_model_id)
    console += "c5_bands_dim: {}\n".format(c5_bands_dim)
    console += "c5_y_height: {}\n".format(c5_y_height)
    console += "c5_x_width: {}\n".format(c5_x_width)
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
    console += "x_lon_step: {}\n".format(vh_x_lon_step)
    console += "y_lat_step: {}\n".format(vh_y_lat_step)
    console += "vci.scale_factor: {}\n".format(vci.scale_factor)
    console += "vci.add_offset: {}\n".format(vci.add_offset)
    console += "tci.scale_factor: {}\n".format(tci.scale_factor)
    console += "tci.add_offset: {}\n".format(tci.add_offset)
    console += "vhi.scale_factor: {}\n".format(vhi.scale_factor)
    console += "vhi.add_offset: {}\n".format(vhi.add_offset)

    print(console)


# for i in range(0,c5_time-1):
#     print(c5_pr[i][400][200])

# convert to proper coordinate range (-180 to 180, -90 to 90)
# c5_x_lon_new = [(x-360.0) if x > 180.0 else x for x in c5_lon[:]]# TODO: do we need to flip the list?
# c5_y_lat_new = [(y-90.0) if y > 90.0 else y for y in c5_lat[:]]
# ref: https://stackoverflow.com/questions/46962288/change-longitude-from-180-to-180-to-0-to-360
c5_x_lon_new = ((np.asarray(c5_lon[:]) - 180) % 360) - 180
c5_x_lon_range = [c5_x_lon_new[0],c5_x_lon_new[-1]]

c5_y_lat_new = ((np.asarray(c5_lat[:]) - 90) % 180) - 90
c5_y_lat_range = [c5_y_lat_new[0],c5_y_lat_new[-1]]

c5_x_lon_step = (abs(c5_x_lon_range[0])+abs(c5_x_lon_range[1]))/c5_x_width
c5_y_lat_step = (abs(c5_y_lat_range[0])+abs(c5_y_lat_range[1]))/c5_y_height

# C5 variables(dimensions): 
# float64 lon(lon), float64 lon_bnds(lon,bnds), 
# float64 lat(lat), float64 lat_bnds(lat,bnds), 
# float64 time(time), float64 time_bnds(time,bnds), 
# float32 pr(time,lat,lon)

############################ bound the data ###################################
#   - CMIP5 data is restricted to contiguous US and some southern CAN/northern MEX
# https://boundingbox.klokantech.com/
#[[[-125.2489197254,25.0753572133],[-66.6447598244,25.0753572133],[-66.6447598244,49.2604253419],[-125.2489197254,49.2604253419],[-125.2489197254,25.0753572133]]]

lon_bounds = c5_x_lon_range
lat_bounds = c5_y_lat_range
# https://gist.github.com/graydon/11198540
# country boxes in format: [SW.lat, SW.lon, NE.lat, NE.lon]
# lat_lower = SW.lat, lat_upper = NE.lat
# lon_lower = SW.lon, lon_upper = NE.lon
tmp_boxes = json.load(open('country_boxes.json'))
us_bounds = tmp_boxes['USA']
lon_bounds = [us_bounds[1],us_bounds[3]]
lat_bounds = [us_bounds[0],us_bounds[2]]

def bounded (coord_span, coord_step, coord_bounds,res=5):
    coords = np.arange(coord_span[0],coord_span[1],coord_step)
    new_coords = []
    idx = []
    for i,coord in enumerate(coords):
        if coord_bounds[0] <= coord <= coord_bounds[1]:
            new_coords.append(np.around(coord,res))
            idx.append(i)
    new_span = [new_coords[0],new_coords[-1]]
    new_idx = [idx[0],idx[-1]]

    return new_idx,new_span


# new vh longitude
vh_x_lon_idx, vh_x_lon_range = bounded(vh_x_lon_range,vh_x_lon_step,lon_bounds)
c5_x_lon_idx,c5_x_lon_range = bounded(c5_x_lon_range,c5_x_lon_step,lon_bounds)
# print(vh_x_lon_idx)
# print(vh_x_lon_range)

# vh_lon = np.arange(vh_x_lon_range[0],vh_x_lon_range[1],vh_x_lon_step)
# vh_x_lon_new = []
# idx_lon = []
# for i,lon in enumerate(vh_lon):
#     if lon_bounds[0] <= lon <= lon_bounds[1]:
#         vh_x_lon_new.append(lon)
#         idx_lon.append(i)
# vh_x_lon_range = [vh_x_lon_new[0],vh_x_lon_new[-1]]
# vh_x_lon_idx = [idx_lon[0],idx_lon[-1]]

# print(vh_x_lon_idx)
# print(vh_x_lon_range)
# sys.exit()
# new vh latitude
vh_y_lat_idx, vh_y_lat_range = bounded(vh_y_lat_range,vh_y_lat_step,lat_bounds)
c5_y_lat_idx, c5_y_lat_range = bounded(c5_y_lat_range,c5_y_lat_step,lat_bounds)
# print(vh_y_lat_idx)
# print(vh_y_lat_range)

# vh_lat = np.arange(vh_y_lat_range[0],vh_y_lat_range[1],vh_y_lat_step)
# vh_y_lat_new = []
# idy_lat = []
# for i,lat in enumerate(vh_lat):
#     if lat_bounds[0] <= lat <= lat_bounds[1]:
#         vh_y_lat_new.append(lat)
#         idy_lat.append(i)
# vh_y_lat_range = [vh_y_lat_new[0],vh_y_lat_new[-1]]
# vh_y_lat_idx = [idy_lat[0],idy_lat[-1]]

# print(vh_y_lat_idx)
# print(vh_y_lat_range)
# sys.exit()
# vh_y_lat_new

# vh_lat[::-1].sort() # np 
# print(len(vh_lon))
# print(vh_lon)
# print(len(vh_lat))
# print(vh_lat)

print(c5_x_lon_range)
print(c5_y_lat_range)
print(vh_x_lon_range)
print(vh_y_lat_range)

sys.exit()
###############################################################################

vh_json = {}
vh_json['type'] = 'vci'
vh_json['attr'] = []
vh_json['data'] = []

vh_json['attr'].append({
    'year': vh_year,
    'week': vh_week,
    'lon_start': vh_x_lon_range[0],
    'lon_end': vh_x_lon_range[1],
    'lon_step': vh_x_lon_step,
    'lat_start': vh_y_lat_range[0],
    'lat_end': vh_y_lat_range[1],
    'lat_step': vh_y_lat_step
})

print(vh_json)

c5_json = {}
c5_json['type'] = 'pr'
c5_json['attr'] = []
c5_json['data'] = []

c5_json['attr'].append({
    'year': c5_time[0],
    'week': c5_time[0],
    'lon_start': c5_x_lon_range[0],
    'lon_end': c5_x_lon_range[1],
    'lon_step': c5_x_lon_step,
    'lat_start': c5_y_lat_range[0],
    'lat_end': c5_y_lat_range[1],
    'lat_step': c5_y_lat_step
})

# precipitation, kg m-2 s-1 (converted to mm/day in 'Subset Request' interface)
# minimum surface air temperature, °K (converted to °C in 'Subset Request' interface)
# maximum surface air temperature, °K (converted to °C in 'Subset Request' interface)

print(c5_json)

# print(c5_time_bnds[:][:])
sys.exit()

with open('c5_data.json', 'w') as outfile:
    json.dump(c5_json, outfile)

geo_cart_map = np.empty(tci.shape, dtype=(tci.range.dtype,2))

# print(geo_cart_map.shape)
# for xi in range(1,geo_cart_map.shape[1]):
#     for yi in range(1,geo_cart_map.shape[0]):
#         geo_cart_map[yi][xi] =  [xi*x_lon_step+x_lon_range[0], yi*y_lat_step+y_lat_range[0]]

test = np.fromfunction(lambda x, y: (x*vh_x_lon_step+vh_x_lon_range[0], y*vh_y_lat_step+vh_y_lat_range[0]), tci.shape, dtype=tci.range.dtype)

print(test[0][0].size)
# print(geo_cart_map)
sys.exit()

# #################################### SHAPELY ###################################
# import shapely.geometry
# import geopandas as gpd
# import shapely.speedups

# # Data sources
# # For State/County/Region shapefiles:
# #   - https://www.census.gov/geographies/mapping-files/time-series/geo/carto-boundary-file.html
# #   - https://www2.census.gov/geo/tiger/TIGER2019/
# #   ++ census data was read in as .shp and written out as .json (GeoJSON) using GeoPandas
# ############
# def readGeoJSON(json_path):    
#     try:
#         gpd_out = gpd.read_file(json_path, driver='GeoJSON')
#     except TypeError as e:
#         print("Couldn't load JSON... requires \'UTF-8\' encoding...")
#         print("TypeError: {}".format(e))
#         print("Try to convert encoding from \'ISO-8859-1\'and try again")
#         cur_json = json.load(open(json_path, encoding='ISO-8859-1'))
#         path,ext = os.path.splitext(json_path)
#         new_path =path+"_new"+ext
#         with open(new_path,"w", encoding='utf-8') as jsonfile:
#                 json.dump(cur_json,jsonfile,ensure_ascii=False)
#         print("Created a new GeoJSON with correct \'UTF-8\' encoding:")
#         print("> {}".format(new_path))
#         gpd_out = gpd.read_file(new_path, driver='GeoJSON')
#     return gpd_out
# ############

# shapely.speedups.enable()
# # load state FIPS codes
# us_fips_path = "../data/geojson/cont_stateToFips.json"
# try: 
#     us_fips = json.load(open(us_fips))
# except:
#     print("Failed to load fips codes...")
#     us_fips = {"Kansas"                 :  "20"}

# us_border_path = "../data/geojson/gz_2010_us_outline_500k.json"
# us_states_path = "../data/geojson/gz_2010_us_040_00_500k.json"
# us_county_path = "../data/geojson/gz_2010_us_050_00_500k.json"
# # us_border = gpd.read_file(us_border_path, driver='GeoJSON')
# # us_states = gpd.read_file(us_states_path, driver='GeoJSON')
# # us_county = gpd.read_file(us_county_path, driver='GeoJSON')
# us_border = readGeoJSON(us_border_path)
# us_states = readGeoJSON(us_states_path)
# us_county = readGeoJSON(us_county_path)

# oregon = us_states.loc[us_states['NAME']=='Oregon'] # replaced .ix with .loc per warning
# oregon.reset_index(drop=True, inplace=True) # what does this do?

# # https://stackoverflow.com/questions/44399749/get-all-lattice-points-lying-inside-a-shapely-polygon
# # points = MultiPoint(VH_data_coords) #np.transpose([np.tile(x, len(y)), np.repeat(y, len(x))]))
# # bounded_ind = points.intersection(state.loc[0,'geometry'])
# # print(bounded_ind)
# # data_for_analysis = VH_data[bounded_ind]
# # pip_data = oregon.loc[pip_mask]

# # plotting 
# # ref: http://geopandas.org/mapping.html

if __name__ == "__main__":
    pass
    