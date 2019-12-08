import salem
import xarray as xr
import numpy as np
import json
import os
import sys
import pprint
import shapely.geometry
import shapely.speedups
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import warnings
# used for masking geometries
from datetime import datetime,date,timedelta
from functools import reduce

SALEM = False
BOUND = False

if SALEM:
    ncfile = "temp/ftp.star.nesdis.noaa.gov-static/VHP.G04.C07.NN.P2006001.VH.nc"
    da = xr.DataArray(np.arange(20).reshape(4, 5), dims=['lat', 'lon'],
                        coords={'lat':np.linspace(0, 30, 4),
                                'lon':np.linspace(-20, 20, 5)})
    # dse = salem.open_xr_dataset(salem.get_demo_file('era_interim_tibet.nc'))
    # dse = salem.open_xr_dataset(ncfile)
    print(da.salem)

    da.salem.quick_map()

from usheatmap.CountryBoxes import CountryBoxes
from usheatmap.UshmDataParser import UshmDataParser
from usheatmap.UshmDataPreprocessor import UshmDataPreprocessor
from usheatmap.UshmUtils import UshmUtils
from netCDF4 import Dataset


def readGeoJSON(json_path):    
    try:
        gpd_out = gpd.read_file(json_path, driver='GeoJSON')
    except TypeError as e:
        print("Couldn't load JSON... requires \'UTF-8\' encoding...")
        print("TypeError: {}".format(e))
        print("Try to convert encoding from \'ISO-8859-1\'and try again")
        cur_json = json.load(open(json_path, encoding='ISO-8859-1'))
        path,ext = os.path.splitext(json_path)
        new_path =path+"_tmp"+ext
        with open(new_path,"w", encoding='utf-8') as jsonfile:
                json.dump(cur_json,jsonfile,ensure_ascii=False)
        print("Created a new GeoJSON with correct \'UTF-8\' encoding:")
        print("> {}".format(new_path))
        gpd_out = gpd.read_file(new_path, driver='GeoJSON')
    return gpd_out

def boundCoords (coord_span, coord_step, coord_bounds,res=5):
    '''
    Takes a range/span of float values (coords) separated at a specific float
    step/separation, returning a new range/span and start/stop indices for
    the values that are with the specified bounds.
    Input:
        coord_span:     list of floats representing the initial range of values 
                        (usually a two element list [min,max])
        coord_step:     float representing the step size between values
        coord_bounds:   two-element list of floats representing the bounds
    Output:
        new_idx:        two-element list representing the new start/stop indices
        new_span:       two-element list representing the new range/span after
                        coord_span has been bounded.
        new_coords:     coordinate list after bounds applied (len(arange(new_span,coord_step)))
    '''
    try:
        coords = np.arange(coord_span[0],coord_span[-1]-coord_step,coord_step)
    except Exception as e:
        print(e)
    
    # print("len(coords): {}".format(len(coords)))
    # print("coords: ({},{})".format(coord_span[0],coord_span[1]))
    # print("step: {}".format(coord_step))

    new_coords = []
    idx = []
    for i,coord in enumerate(coords):
        if coord_bounds[0] <= coord <= coord_bounds[1]:
            new_coords.append(np.around(coord,res))
            idx.append(i)
    new_span = [new_coords[0],new_coords[-1]]
    new_idx = [idx[0],idx[-1]]
    # need to remove the last element of new_coords
    return new_idx,new_span,new_coords[:-1 or None]



utils = UshmUtils()

prod_path = "/home/christnp/Development/e6893/homework/e6893-project/src/.tmp/temp_ftp.star.nesdis.noaa.gov/VHP.G04.C07.npp.P2018001.VH.nc"
# prod_path = "/home/christnp/Development/e6893/homework/e6893-project/src/.tmp/vh_json_2018-01-07_2018-01-07.json"


vh = Dataset(prod_path, mode='r') # using netCDF4 to load .nc file
# TODO: can this be loaded from FTP server directly?

# attributes
vh_product     = getattr(vh,'PRODUCT_NAME')
vh_year        = getattr(vh,'YEAR')
vh_week        = getattr(vh,'PERIOD_OF_YEAR')
try:
    vh_y_lat_range = [getattr(vh,'geospatial_lat_min'), 
                        getattr(vh,'geospatial_lat_max')]
    vh_x_lon_range = [getattr(vh,'geospatial_lon_min'), 
                        getattr(vh,'geospatial_lon_max')]
except Exception as e:
    print("\nLat/Lon attributes have changed. Error: {}\n".format(e))
    print(">>> Attempting the old format now...\n")
    vh_y_lat_range = [getattr(vh,'START_LATITUDE_RANGE'), 
                        getattr(vh,'END_LATITUDE_RANGE')]
    vh_x_lon_range = [getattr(vh,'START_LONGITUDE_RANGE'), 
                        getattr(vh,'END_LONGITUDE_RANGE')]
    print(">>> success!")
# dimensions
vh_y_height    = vh.dimensions['HEIGHT'].size
vh_x_width     = vh.dimensions['WIDTH'].size
# variables
vh_vci_attr = vh['VCI']
vh_vci      = vh['VCI'][:]
vh_tci_attr = vh['TCI']
vh_tci      = vh['TCI'][:]
vh_vhi_attr = vh['VHI']
vh_vhi      = vh['VHI'][:]
#variable attributes
vh_vci_prod_name = vh_vci_attr.long_name
vh_tci_prod_name = vh_tci_attr.long_name
vh_vhi_prod_name = vh_vhi_attr.long_name
vh_vci_units = vh_vci_attr.units
vh_tci_units = vh_tci_attr.units
vh_vhi_units = vh_vhi_attr.units
vh_vci_fill = np.float(vh_vci_attr._FillValue)    # int16 not JSON serializable
vh_tci_fill = np.float(vh_tci_attr._FillValue)    # int16 not JSON serializable
vh_vhi_fill = np.float(vh_vhi_attr._FillValue)    # int16 not JSON serializable

prod_type = 'vci'


# vh_x_lon_step = (vh_x_lon_range[1]-vh_x_lon_range[0])/vh_x_width
vh_x_lon_step = (abs(vh_x_lon_range[1])+abs(vh_x_lon_range[0]))/vh_x_width
# vh_y_lat_step = (vh_y_lat_range[1]-vh_y_lat_range[0])/vh_y_height
vh_y_lat_step = (abs(vh_y_lat_range[1])+abs(vh_y_lat_range[0]))/vh_y_height

console = "[{}] Parsing \'{}\' data...".format(utils.timestamp(),vh_product)     # Console output string


######################## convert the data ##############################
vh_date = []
# res = datetime.strptime("2018 W30 w1", "%Y %W w%w")
d = "{}-{}-0".format(vh_year,vh_week) # set for YYYY-MM-Sunday
vh_date.append(datetime.strptime(d, "%Y-%W-%w").date().strftime('%Y-%m-%d %H:%M:%S'))

# TODO: remove
prod_date = vh_date[0]
# uncomment to use YYYY-week#
# d = "{}-{}".format(vh_year,vh_week) # set for YYYY-MM-Sunday
# vh_date.append(d)
vh_y_lat = np.arange(vh_y_lat_range[0],vh_y_lat_range[-1],vh_y_lat_step)
vh_x_lon = np.arange(vh_x_lon_range[0],vh_x_lon_range[-1],vh_x_lon_step)

######################### TODO: MAKE THIS A GLOBAL #####################
# tmp_boxes = json.load(open('../../scratch/country_boxes.json'))
# us_bounds = tmp_boxes['USA']
if BOUND:
    us_bounds = CountryBoxes().getBox('USA')
    lon_bounds = [us_bounds[1],us_bounds[3]] # lat.lower = SW.lat, lat.upper = NE.lat
    lat_bounds = [us_bounds[0],us_bounds[2]] # lon.lower = SW.lon, lon.upper = NE.lon
    ############################################################################

    # new longitude and width indices
        # Longitude = (-180.0 + (i+0.5)* dLon) (i: counts from 0 to 9999) 
    # i = (lon + 180.0)/dLon - 0.5 
    # vh_x_lon_idx = [int((lon_bounds[0] + 180.0)/vh_x_lon_step - 0.5),
    #                 int((lon_bounds[1] + 180.0)/vh_x_lon_step - 0.5)]
    # vh_x_lon_range = lon_bounds
    # vh_x_lon = np.arange(lon_bounds[0],lon_bounds[1]-vh_x_lon_step,vh_x_lon_step).tolist()
    vh_x_lon_idx, vh_x_lon_range,vh_x_lon = boundCoords(vh_x_lon_range,vh_x_lon_step,lon_bounds)
    vh_x_width = abs(vh_x_lon_idx[1] - vh_x_lon_idx[0])

    # new latitude and height indices
    # Latitude = (75.024 - (j+0.5) *dLat) (j: counts from 0 to 3615)
    # j = (75.024-lat)/dLat - 0.5
    # vh_y_lat_idx = [int((75.024-lat_bounds[1] )/vh_y_lat_step - 0.5),
    #                 int((75.024-lat_bounds[0])/vh_y_lat_step - 0.5)]
    # vh_y_lat_range = lat_bounds
    # vh_y_lat = np.arange(lat_bounds[0],lat_bounds[1],vh_y_lat_step).tolist()   
    vh_y_lat_idx, vh_y_lat_range,vh_y_lat = boundCoords(vh_y_lat_range,vh_y_lat_step,lat_bounds)
    vh_y_height = abs(vh_y_lat_idx[1] - vh_y_lat_idx[0])

    ############## apply bounds and convert VH to weekly data ##############
    vh_vci_new = np.ma.empty((1,vh_y_height,vh_x_width),dtype=np.float32)
    # vh_tci_new = np.ma.empty((1,vh_y_height,vh_x_width),dtype=np.float32)
    # vh_vhi_new = np.ma.empty((1,vh_y_height,vh_x_width),dtype=np.float32)
    # indices for bounding
    y0,y1 = vh_y_lat_idx[0], vh_y_lat_idx[1]
    x0,x1 = vh_x_lon_idx[0], vh_x_lon_idx[1]
    # new matrices using slicing
    vh_vci_new[:] = vh_vci[y0:y1,x0:x1]#.filled() # can add any number to .filled(#)
    # vh_tci_new[:] = vh_tci[y0:y1,x0:x1]#.filled() # can add any number to .filled(#)
    # vh_vhi_new[:] = vh_vhi[y0:y1,x0:x1]#.filled() # can add any number to .filled(#)
########################## FORMAT OUTPUT ###############################
shapely.speedups.enable()

state_fips = os.path.join("usheatmap","geojson/cont_stateToFips.json")
# load state FIPS codes if not user provided
#
try: 
    us_fips = json.load(open(state_fips))
except:
    print("Failed to load fips codes...")
    us_fips = {"Kansas"                 :  "20"}

county_path  = os.path.join("usheatmap","geojson/cb_2018_us_county_500k.json")
# read GeoJSON boundary file
# 
us_county = readGeoJSON(county_path)
us_county['centroid'] = None
us_county[prod_type] = None
us_contiguous = gpd.GeoDataFrame(columns=us_county.columns)


date_start = datetime.now()
print("[{}] Processing {} data from {}...".format(utils.timestamp(),prod_type,prod_date))

# transform data into xarray format

ds = xr.open_dataset(prod_path)
print(ds.VCI.data)
sys.exit()

try:
    da = xr.DataArray(vh_vci, dims=['lat', 'lon'],
                        coords={'lat':vh_y_lat,'lon':vh_x_lon})
except Exception as e:
    print("[{}] Failed to load data into xarray.DataArray(). Error: {}".format(utils.timestamp(),e))
    # return -1

us_fips = {"Louisiana"              :  "22"}

county_mean = {}
states = []
choro_state = [] # choropleth plot
for state_name in us_fips:

    state_fips = us_fips[state_name]
    states.append(state_fips)

    # focus on the current state
    state = us_county.loc[us_county['STATEFP']==state_fips]

    print("[{}] {} - {} ".format(utils.timestamp(),state_name,prod_date))

    county_mean[state_fips] = []
    choro = [] # choropleth plot
    for idx,counties in state.iterrows():
        # for fast failure testing
        # choro.append(idx)
        # choro_state.append(idx)
        # continue

        # select region of interest using county geojson geometry
        county = state.loc[state['COUNTYFP']==counties['COUNTYFP']]
        try:
            #ref: https://salem.readthedocs.io/en/v0.2.3/xarray_acc.html
            das = da.salem.subset(shape=county, margin=10) #gives bounding box
        except Exception as e:
            choro.append(0.0)
            print(da)
            print("[{}] Failed to select SUBSET on xarray.DataArray() for {}. Error: {}".format(utils.timestamp(),counties['NAME'],e))

            continue
        try:
            dsr = das.salem.roi(shape=county) # da.salem.subset(shape=county, margin=10) # gives bounding box
        except Exception as e:
            choro.append(0.0)
            print(das)
            print("[{}] Failed to select ROI on xarray.DataArray() for {}. Error: {}".format(utils.timestamp(),counties['NAME'],e))
            continue
        
        mean = np.nanmean(dsr.values)
        if np.isnan(mean):
            mean = -1.0 # indicate nan
            print(" np.nanmean: {}".format(dsr))

        mean = float(dsr.mean(skipna=True).values)
        if np.isnan(mean):
            mean = -1.0 # indicate nan
            print(" dsr.mean(): {}".format(dsr))

        
        # get the county centroid
        county_poly = county.geometry#loc[county['geometry'] == 'POLYGON']
        county_center_x_lon = float(county_poly.centroid.x)
        county_center_y_lat = float(county_poly.centroid.y)

        # store county mean and centroid in state GeoDF for plotting
        state.loc[state['COUNTYFP']==counties['COUNTYFP'],prod_type] = mean
        state.loc[state['COUNTYFP']==counties['COUNTYFP'],'centroid'] = county_poly.centroid
        # reload county to update us_contiguous GeoDF
        # TODO: I should be able to use state.loc[idx,'column_name'] and not need county
        county = state.loc[state['COUNTYFP']==counties['COUNTYFP']]
        us_contiguous = us_contiguous.append(county,ignore_index=True) # TODO: this does not retain original indices

        county_mean[state_fips].append({
            'date': prod_date,
            'centroid_lon': float(county_poly.centroid.x),
            'centroid_lat': float(county_poly.centroid.y),
            'state': "{}_{}".format(counties['STATEFP'],state_name),
            'county': "{}_{}".format(counties['COUNTYFP'],counties['NAME']),
            'type': prod_type,
            'mean': mean,
            # prod_type: mean
        })
        
        choro.append(mean)
        choro_state.append(mean)
        print(">>> {}_{}:  {}".format(counties['COUNTYFP'],counties['NAME'], mean))

    # save JSON file to temp location
    file_date = datetime.strptime(prod_date, "%Y-%m-%d %H:%M:%S").date().strftime('%Y-%m-%d')
    file_name = "{}_{}_json_{}".format(state_name,prod_type,file_date)
    json_fname = os.path.join(project_path,file_name+'.json')
    with open(json_fname, 'w') as outfile:
        try:
            # json.dump(county_mean[state_fips], outfile)
            print("[{}] Saved {}-{} JSON \'{}\'".format(utils.timestamp(),state_name,prod_type,json_fname))
        except Exception as e:
            print("[{}] Failed`to save \'{}\'. Error: {}".format(utils.timestamp(),json_fname,e))
    # debug output
        # pprint.pprint(county_mean[state_fips])
    # create choropleth plot
    if(plot==True):
        # ref: http://geopandas.org/mapping.html
        try:
            state['mean'] = choro
            fig = plt.figure(1, figsize=(5,5), dpi=90)
            ax = fig.add_subplot(111)
            state.plot(ax=ax,column='mean', legend=True,
                        legend_kwds={'label': "Mean",
                                    'orientation': "horizontal"})
            # ref: https://gis.stackexchange.com/questions/330008/center-normalize-choropleth-colors-in-geopandas
            # f = plt.gcf()
            # cax = f.get_axes()[1]
            # # cax.set_ylabel('test')
            # cax.set_xlim(0.0,100.0)
            plt_fname = os.path.join( self.project_path,file_name+'.png')
            plt.savefig(plt_fname,dpi=600)
            print("[{}] Saved {}-{} figure \'{}\'".format(utils.timestamp(),state_name,prod_type,plt_fname))
        except Exception as e:
            print("[{}] Could not save state plot. Error: {}".format(utils.timestamp(),e))
        finally:
            choro = [] # reset choropleth plot data list
            plt.clf() # clear figure

date_total = datetime.now()-date_start
print("[{}] Finished processing {} data for {} in {:0.3f} minutes!".format(utils.timestamp(),prod_type,prod_date, date_total.total_seconds()/60))

