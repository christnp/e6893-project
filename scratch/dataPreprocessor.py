import shapely.geometry
import shapely.speedups
import geopandas as gpd

import json
import os
import sys

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



#######################################################################
# Use Masked arrays an np.ndarray.nanmean for generating mean of county?
# 1. https://docs.scipy.org/doc/numpy/reference/maskedarray.generic.html
# 2. https://docs.scipy.org/doc/numpy/reference/generated/numpy.nanmean.html

#numpy.putmask(a, mask, values)¶

# tmp = np.empty((c5_y_height,c5_x_width))
# tmp.fill(1.0e30)
# print(tmp.shape)
# print(tmp)
# print(c5_pr[:].filled())

# print("c5_y_lat_idx = {}".format(c5_y_lat_idx))
# print("c5_x_lon_idx = {}".format(c5_x_lon_idx))
# print(c5_y_lat_idx[0])
# print(c5_y_lat_idx[1])
# for y in range(c5_y_lat_idx[0],c5_y_lat_idx[1]):
#     for x in range(c5_x_lon_idx[0],c5_x_lon_idx[1]):
#         tmp[y][x] = 1

# # tmp[c5_y_lat_idx[0]:c5_y_lat_idx[1]][c5_x_lon_idx[0]:c5_x_lon_idx[1]] = 1

# print(tmp.shape)
# print(tmp[0][c5_y_lat_idx[0]][:])
# sys.exit()
# for i in tmp:
#     for x,j in enumerate(i):
#         if x in range(c5_y_lat_idx[0],c5_y_lat_idx[1]):
#             print(x)
#             print(j[:])
#             if j[0] != 0:
#                 break
if __name__ == "__main__":
    pass
