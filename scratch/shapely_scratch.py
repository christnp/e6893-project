import shapely.geometry
import matplotlib.pyplot as plt
import geopandas as gpd
import shapely.speedups
import sys
import os

import json

# poly = shapely.geometry.Polygon([(2,2), (1,-1), (-1,-1), (-1, 1)])
# point = shapely.geometry.Point(1, 1)


# print(poly)
# print(point.intersects(poly))
# print(point.within(poly))
CONVERT_STATE_SHP = False
STATE_DETAILED = False
CONVERT_COUNTY_SHP = False
COUNTY_DETAILED = False
PLOT = False

gj_base = "../data/geojson/"
sh_base = "../data/shp/"
state_gjpath = "cb_2018_us_state_500k.json"
state_gjpath_tl = "tl_2019_us_state.json"
county_gjpath = "cb_2018_us_county_500k.json" # Cartographic Boundary GeoJSON for boundary mapping only
county_gjpath_tl = "tl_2019_us_county.json"  # Tiger/Line GeoJSON provides extra metadata

state_path = os.path.join(gj_base,state_gjpath_tl) if STATE_DETAILED else os.path.join(gj_base,state_gjpath)
county_path = os.path.join(gj_base,county_gjpath_tl) if COUNTY_DETAILED else os.path.join(gj_base,county_gjpath)

# ref: https://www.census.gov/geographies/mapping-files/time-series/geo/carto-boundary-file.html
if CONVERT_STATE_SHP:
    # read in the shapefile
    state_shpath ="cb_2018_us_state_500k/cb_2018_us_state_500k.shp"
    state_shpath_tl = "tl_2019_us_state/tl_2019_us_state.shp"
    state_path = os.path.join(sh_base,state_shpath_tl) if STATE_DETAILED else os.path.join(sh_base,state_shpath)
    us_state = gpd.read_file(state_path)
    # write out the GeoJSON file
    state_gjpath = "cb_2018_us_state_500k.json"
    state_gjpath_tl = "tl_2019_us_state.json"
    state_path = os.path.join(gj_base,state_gjpath_tl) if STATE_DETAILED else os.path.join(gj_base,state_gjpath)
    us_state.to_file(state_path, driver='GeoJSON')
else:
    us_state = gpd.read_file(state_path, driver='GeoJSON')

if CONVERT_COUNTY_SHP:
    # read in the shapefile
    county_shpath = "cb_2018_us_county_500k/cb_2018_us_county_500k.shp" 
    county_shpath_tl = "shp/tl_2019_us_county/tl_2019_us_county.shp" 
    county_path = os.path.join(sh_base,county_shpath_tl) if COUNTY_DETAILED else os.path.join(sh_base,county_shpath)    
    us_county = gpd.read_file(county_path)
    # write out the GeoJSON file
    county_gjpath = "cb_2018_us_county_500k.json"
    county_gjpath_tl = "tl_2019_us_county.json"
    county_path = os.path.join(gj_base,county_gjpath_tl) if COUNTY_DETAILED else os.path.join(gj_base,county_gjpath)
    us_county.to_file(county_path, driver='GeoJSON')
else:
    try:
        us_county = gpd.read_file(county_path, driver='GeoJSON')
    except TypeError as e:
        print("Couldn't load JSON... requires \'UTF-8\' encoding...")
        print("TypeError: {}".format(e))
        print("Try to convert encoding from \'ISO-8859-1\'and try again")
        cur_json = json.load(open(county_path, encoding='ISO-8859-1'))
        path,ext = os.path.splitext(county_path)
        new_path =path+"_new"+ext
        with open(new_path,"w", encoding='utf-8') as jsonfile:
                json.dump(cur_json,jsonfile,ensure_ascii=False)
        us_county = gpd.read_file(new_path, driver='GeoJSON')

# print(us_county.columns)
# Example for getting specific state data 
# Columns: STATEFP, COUNTYFP, COUNTYNS, AFFGEOID, GEOID, NAME, LSAD, ALAND, AWATER, geometry
state = us_county.loc[us_county['STATEFP']!='02']
state = state.loc[state['STATEFP']!='15']
# county = state.loc[state['NAME']=='Morrow'] # for Oregon
county = state.loc[state['COUNTYFP']=='049']

###########################################
# TO PLOT CHOROPLETH CHART
###########################################
if PLOT:
    # ref: http://geopandas.org/mapping.html
    state['test'] = state.AWATER + state.ALAND # example of adding column for plot (see below)
    fig = plt.figure(1, figsize=(5,5), dpi=90)
    ax = fig.add_subplot(111)
    state.plot(ax=ax,column='test', legend=True,
                legend_kwds={'label': "Total Area",
                            'orientation': "horizontal"})
    plt.show()
###########################################

sys.exit()

# Use shapley to get data for each county
# https://automating-gis-processes.github.io/CSC18/lessons/L4/point-in-polygon.html
shapely.speedups.enable()


# print(us_states)
print(us_county)
# check for point within poly
# dot = shapely.geometry.Point(-121.238251, 44.339013)
# poly = shapely.geometry.Polygon([(-121.238251, 44.339013),(-121.238251, 44.039013),(-121.038251, 44.039013),(-121.238251, 44.339013)])
# poly1 = shapely.geometry.Polygon([(-121.238251, 44.339013),(-121.238251, 44.039013),(-121.038251, 44.039013),(-121.238251, 44.339013)])
# poly2 = shapely.geometry.Polygon([(-120.238251, 43.339013),(-120.238251, 43.039013),(-120.038251, 43.039013),(-120.238251, 43.339013)])
# mpoly = shapely.geometry.MultiPolygon([poly1,poly2])
# print(mpoly)

sys.exit()
oregon_geo = oregon.loc[0, 'geometry']
# us_geo = us_border.loc[0, 'geometry']
# pip_mask = oregon_geo.within(us_geo)
pip_mask = mpoly.intersection(oregon.loc[0, 'geometry'])

# https://stackoverflow.com/questions/44399749/get-all-lattice-points-lying-inside-a-shapely-polygon
# points = MultiPoint(VH_data_coords) #np.transpose([np.tile(x, len(y)), np.repeat(y, len(x))]))
# bounded_ind = points.intersection(state.loc[0,'geometry'])
# print(bounded_ind)
# data_for_analysis = VH_data[bounded_ind]
# pip_data = oregon.loc[pip_mask]

# print(poly.exterior.coords)
# print(us_geo)
# print(pip_mask)
# print(pip_data)
# print(oregon.loc[0, 'geometry'])
# fig, ax = plt.subplots()
# us_gpd.plot(ax=ax, facecolor='gray')
# oregon.plot(ax=ax, facecolor='red')
# ax.scatter(poly, color='black',s=64)
# plt.tight_layout()
# plt.show()

# print(us_gpd)
# x,y = poly.exterior.xy
# fig = plt.figure(1, figsize=(5,5), dpi=90)
# ax = fig.add_subplot(111)
# ax.plot(x, y, color='#6699cc', alpha=0.7,
#     linewidth=3, solid_capstyle='round', zorder=2)
# ax.plot(point, "or")
# ax.set_title('Polygon')
# plt.show()