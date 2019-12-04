import shapely.geometry
import shapely.speedups
import geopandas as gpd
import matplotlib.pyplot as plt
import warnings
# used for masking geometries
import salem
import xarray as xr

from datetime import datetime,date,timedelta
import numpy as np
import json
import pprint
import os
import sys

from usheatmap.UshmUtils import UshmUtils


class UshmDataPreprocessor():
    """ docstring for sphinx
    """
    def __init__(self):
        """ docstring
        """
        # this allows warnings to be caught with try/catch
        # warnings.filterwarnings("error")
        # this ignores warnings from being deisplayed
        warnings.simplefilter("ignore")

        self.rpath = os.path.dirname(os.path.realpath(__file__))
        tmp = os.path.join(self.rpath,".tmp")
        if not os.path.exists(tmp):
            os.makedirs(tmp)
        self.project_path = tmp
        # static FIPs state codes
        self.state_fips = os.path.join(self.rpath,"geojson/cont_stateToFips.json")
        # static GeoJSON boundary files
        # tl_ -> Tiger/Line GeoJSON provides extra metadata
        # cb_ -> Cartographic Boundary GeoJSON for boundary mapping only
        self.state_path   = os.path.join(self.rpath,"geojson/cb_2018_us_state_500k.json")
        self.county_path  = os.path.join(self.rpath,"geojson/cb_2018_us_county_500k.json")
        self.utils = UshmUtils()
    # Data sources
    # For State/County/Region shapefiles:
    #   - https://www.census.gov/geographies/mapping-files/time-series/geo/carto-boundary-file.html
    #   - https://www2.census.gov/geo/tiger/TIGER2019/
    #   ++ census data was read in as .shp and written out as .json (GeoJSON) using GeoPandas
    ############
    def readGeoJSON(self,json_path):    
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
        
    ############

    def run(self,data,fips=[],target=[],plot=False,debug=0):
        print("[{}] Starting {}...".format(self.utils.timestamp(),__name__))
        shapely.speedups.enable()
        
        # load state FIPS codes if not user provided
        #
        if not fips:
            try: 
                us_fips = json.load(open(self.state_fips))
            except:
                print("Failed to load fips codes...")
                us_fips = {"Kansas"                 :  "20"}
        else:
            us_fips = fips
        

        # get data attributes
        #
        # 'date':       list of str
        # 'time_dim':   int
        # 'lon_start':  float
        # 'lon_end':    float
        # 'lon_step':   float
        # 'lon_dims':   int
        # 'lon_units':  str
        # 'lat_start':  float
        # 'lat_end':    float
        # 'lat_step':   float
        # 'lat_dims':   int 
        # 'lat_units':  str
        attr = data['attr'][0]
        date_lst = attr['date']
        lon = np.array(attr['lon'])
        lon_start = np.float(attr['lon_start'])
        lon_end = np.float(attr['lon_end'])
        lon_step = np.float(attr['lon_step'])
        lat = np.array(attr['lat'])
        lat_start = np.float(attr['lat_start'])
        lat_end = np.float(attr['lat_end'])
        lat_step = np.float(attr['lat_step'])

        # get data
        #
        # 'type':     str
        # 'units':    str,
        # 'fill':     int,
        # 'data':     list of float -> to get np.ndarray do np.array(prod['data'])
        for product in data['prod']:
            
            # reconstruct masked data
            prod_mask = np.array(product['mask'])
            prod_fill = product['fill']
            prod_data = np.ma.array(product['data'],mask=prod_mask,fill_value=prod_fill)
            prod_type = product['type']

            # allows for preprocessing of certain products only
            if target and prod_type not in target:
                print("Skipping {}...".format(prod_type))
                continue
           
            # ref: https://cmdlinetips.com/2018/12/how-to-loop-through-pandas-rows-or-how-to-iterate-over-pandas-rows/#:~:targetText=Pandas%20has%20iterrows()%20function,the%20content%20of%20the%20iterator.
            # some data has a 3rd (time) dimension
            
            first_week = True # if true, only pre-processes first week of data

            # https://automating-gis-processes.github.io/2016/Lesson2-geopandas-basics.html

            proc_start = datetime.now()
            for week,prod_date in enumerate(date_lst):
                date_start = datetime.now()
                print("[{}] Processing data from {}...".format(self.utils.timestamp(),prod_date))

                # transform data into xarray format
                try:
                    da = xr.DataArray(prod_data[week,:,:], dims=['lat', 'lon'],
                                        coords={'lat':lat,'lon':lon})
                except Exception as e:
                    print("[{}] Failed to load data into xarray.DataArray(). Error: {}".format(self.utils.timestamp(),e))
                    # return -1
                    continue
                
                # load state/county border GeoJSON
                # us_states = self.readGeoJSON(us_states_path)
                # Columns: STATEFP, COUNTYFP, COUNTYNS, AFFGEOID, GEOID, NAME, LSAD, ALAND, AWATER, geometry
                us_county = self.readGeoJSON(self.county_path)
                # create new GeoPandas dataframe and column for storing new data         
                # us_contiguous = gpd.GeoDataFrame(us_county)
                # us_contiguous[prod_type] = None
                us_county['centroid'] = None
                us_county[prod_type] = None
                us_contiguous = gpd.GeoDataFrame(columns=us_county.columns)

                # for state_name in us_fips:
                #     state_fips = us_fips[state_name]
                #     state = us_county[us_county['STATEFP'] == state_fips]
                #     for idx,counties in state.iterrows():  
                #         county = state.loc[state['COUNTYFP']==counties['COUNTYFP']]
                #         # county.loc[county['county']==counties['COUNTYFP'], prod_type] = 1
                #         county[prod_type] = idx                        
                #         us_contiguous = us_contiguous.append(county,ignore_index=True)
                #         # us_contiguous.loc[idx, county.columns] = county
                # print(us_contiguous)                
                # sys.exit()

                county_mean = {}
                states = []
                choro_state = [] # choropleth plot
                for state_name in us_fips:

                    state_fips = us_fips[state_name]
                    states.append(state_fips)

                    # focus on the current state
                    state = us_county.loc[us_county['STATEFP']==state_fips]

                    if(debug > 2):
                        print("[{}] {} - {} ".format(self.utils.timestamp(),state_name,prod_date))

                    county_mean[state_fips] = []
                    choro = [] # choropleth plot
                    for idx,counties in state.iterrows():
                        # for fast failure testing
                        # choro.append(idx)
                        # choro_state.append(idx)
                        # continue

                        # STATEFP                                                    22
                        # COUNTYFP                                                  001
                        # COUNTYNS                                             00558389
                        # AFFGEOID                                       0500000US22001
                        # GEOID                                                   22001
                        # NAME                                                   Acadia
                        # LSAD                                                       15
                        # ALAND                                              1696880710
                        # AWATER                                                5918557
                        # geometry    POLYGON ((-92.632291 30.317085, -92.629226 30....
                        # Name: 131, dtype: object
                        # from shapely.geometry import Polygon
                        # county = state.loc[state['COUNTYFP']==counties['COUNTYFP']]
                        # county_poly = county.geometry#loc[county['geometry'] == 'POLYGON']
                        # county_center = (float(county_poly.centroid.x),float(county_poly.centroid.y))
                        # poly.bounds
                        # console = "lon range: ({},{})\n".format(poly.bounds.minx.values,poly.bounds.maxx.values)
                        # console += "lon range (360): ({},{})\n".format(360+poly.bounds.minx.values,360+poly.bounds.maxx.values)
                        # console += "lat range: ({},{})\n".format(poly.bounds.miny.values,poly.bounds.maxy.values)

                        # print(console)
                        # sys.exit()

                        # select region of interest using county geojson geometry
                        county = state.loc[state['COUNTYFP']==counties['COUNTYFP']]
                        try:
                            #ref: https://salem.readthedocs.io/en/v0.2.3/xarray_acc.html
                            das = da.salem.subset(shape=county, margin=10) #gives bounding box
                        except Exception as e:
                            choro.append(0.0)
                            print("\n[{}] Failed to select SUBSET on xarray.DataArray(). Error: {}".format(self.utils.timestamp(),e))
                            # Failed to select ROI on xarray.DataArray(). Error: zero-size array to reduction operation minimum which has no identity
                            # e.g., Montour County Pennsylvania
                            # Failed to select ROI on xarray.DataArray(). Error: index 1 is out of bounds for axis 0 with size 0
                            # e.g., Washington County Oregon missing lon, Willacy County Texas missing lat
                            print("\n county: \n{}".format(county))
                            print("\n xarray da: \n{}".format(da))
                            continue

                        try:
                            dsr = das.salem.roi(shape=county) # da.salem.subset(shape=county, margin=10) # gives bounding box
                        except Exception as e:
                            choro.append(0.0)
                            print("[{}] Failed to select ROI on xarray.DataArray(). Error: {}".format(self.utils.timestamp(),e))
                            print("\n county: \n{}".format(county))
                            print("\n xarray da: \n{}".format(da))
                            print("\n subset da: \n{}".format(das))
                            continue

                        
                        # mean = np.nanmean(dsr.values)
                        mean = float(dsr.mean(skipna=True).values)
                        if np.isnan(mean):
                            # pprint.pprint(prod_data)
                            # print(da)
                            # print(das)                            
                            # print(dsr)
                            # sys.exit()
                            mean = -1.0 # indicate nan
                        
                        # get the county centroid
                        county_poly = county.geometry#loc[county['geometry'] == 'POLYGON']
                        county_center_x_lon = float(county_poly.centroid.x)
                        county_center_y_lat = float(county_poly.centroid.y)

                        # store mean in county GeoDF, then append to us_contiguous GeoDF
                        # NOTE: I am now just using the state GeoDF
                        # county['centroid'] = county_poly.centroid
                        # county[prod_type] = mean
                        # us_contiguous = us_contiguous.append(county,ignore_index=True) # TODO: this does not retain original indices

                        # store county mean and centroid in state GeoDF for plotting
                        state.loc[state['COUNTYFP']==counties['COUNTYFP'],prod_type] = mean
                        state.loc[state['COUNTYFP']==counties['COUNTYFP'],'centroid'] = county_poly.centroid
                        # reload county to update us_contiguous GeoDF
                        # TODO: I should be able to use state.loc[idx,'column_name'] and not need county
                        county = state.loc[state['COUNTYFP']==counties['COUNTYFP']]
                        us_contiguous = us_contiguous.append(county,ignore_index=True) # TODO: this does not retain original indices


                        # TODO: does one NaN cause a problem for the entire dataset?
                        # focus on Louisiana

                        # print("\n prod_data: \n{}".format(prod_data))
                        # print("\n xarray da: \n{}".format(da))
                        # print("\n subset da: \n{}".format(das))
                        # print("\n dsr: \n{}".format(dsr))
                        # sys.exit()
                        # add county mean to JSON (ignoring np.nan)
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
                        if(debug > 2):
                            print(">>> {}_{}:  {}".format(counties['COUNTYFP'],counties['NAME'], mean))
                
                    # save JSON file to temp location
                    json_fname = os.path.join( self.project_path,'{}_{}_mean-{}.json'.format(state_name,prod_type,prod_date))
                    with open(json_fname, 'w') as outfile:
                        try:
                            json.dump(county_mean[state_fips], outfile)
                            print("Saved {}-{} JSON \'{}\'".format(state_name,prod_type,json_fname))
                        except Exception as e:
                            print("Failed to save \'{}\'. Error: {}".format(json_fname,e))
                    # debug output
                    if(debug > 3):
                        pprint.pprint(county_mean[state_fips])
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
                            plt_fname = os.path.join( self.project_path,'{}_{}_mean-{}.png'.format(state_name,prod_type,prod_date))
                            plt.savefig(plt_fname,dpi=600)
                            print("Saved {}-{} figure \'{}\'".format(state_name,prod_type,plt_fname))
                        except Exception as e:
                            print("Could not save state plot. Error: {}".format(e))
                        finally:
                            choro = [] # reset choropleth plot data list
                            plt.clf() # clear figure

                date_total = datetime.now()-date_start
                print("Finished processing data for {} in {:0.3f} minutes!".format(prod_date, date_total.total_seconds()/60))


                ###########################################
                # TO PLOT CHOROPLETH CHART
                ###########################################
                if plot:
                    try:
                        # ref: http://geopandas.org/mapping.html
                        us_county['mean'] = choro_state # example of adding column for plot (see below)
                        fig = plt.figure(2, figsize=(5,5), dpi=90)
                        ax = fig.add_subplot(111)

                        us_contiguous.plot(ax=ax,column='mean', legend=True,
                                    legend_kwds={'label': "Mean",
                                                'orientation': "horizontal"})
                        plt_fname = os.path.join( self.project_path,'us_{}_mean-{}.png'.format(prod_type,prod_date))
                        plt.savefig(plt_fname,dpi=600)
                        print("Saved us-{} figure \'{}\'".format(prod_type,plt_fname))
                    except Exception as e:
                        print("Could not save country plot. Error: {}".format(e))
                    finally:
                        choro_state = [] # reset choropleth plot data list
                        plt.clf() # clear figure
                
                # uses first_week flag to force only the first week of data being saved
                if(first_week):
                    break
                
            # save all US county information
            json_fname = os.path.join( self.project_path,'all_{}_mean-{}.json'.format(prod_type,prod_date))
            with open(json_fname, 'w') as outfile:
                try:
                    json.dump(county_mean, outfile)
                    print("Saved all-{} JSON \'{}\'".format(prod_type,json_fname))
                except Exception as e:
                    print("Failed to save \'{}\'. Error: {}".format(json_fname,e))
            

            proc_time = (datetime.now() - proc_start)
            print("Preprocessing for {} completed in {:0.3f} minutes!".format(prod_type, proc_time.total_seconds()/60))

        

        
            

################# ATTEMPT USING GeoSeries/GeoDataFrame #########################
                # import itertools
                # out = []
                # lat_ind = 0
                # x_dim = len(x)
                # y_dim = len(y)
                # lon_x = 0
                # lat_y = 0
                # for lat_ind,(lon,lat) in enumerate(itertools.product(x,y)):
                #     # lat_y = lat_ind*x_dim+lon_ind
                #     # print("{},{}".format(lon_x,lat_y))
                #     d = data['prod'][0]['data'][0][lat_y][lon_x] #
                #     lat_y += 1
                #     out.append((d,(lon,lat)))
                #     if not lat_y % y_dim and lat_y != 0:
                #         lon_x += 1
                #         lat_y = 0
                #     if lat_y == y_dim-1:# and lon_x == 1:
                #         break

                # # print(out)
                # print(type(out))
                # print(len(out))
                # print(data['prod'][0]['type'])
                # # sys.exit()
                # # x = range(lon_start,lon_end,lon_step)
                # # y = range(lat_start,lat_end,lat_step)
                # # print(x)
                # # print(y)
                # # data | lon | lat | geometry
                # from shapely.geometry import Point
                # # geom = map(lambda x : Point([x]), out)
                # tmp = {}
                # tmp['data'] = []
                # tmp['lat'] = []
                # tmp['lon'] = []
                # geom = []
                # for d,g in out:
                #     geom.append(Point(g))
                #     tmp['data'].append(d)
                #     tmp['lon'].append(g[0])
                #     tmp['lat'].append(g[1])

                # # geom = [Point(x) for x in out]
                # s = gpd.GeoDataFrame(tmp, geometry=geom) #geom is a Series
                # # s.crs = {‘init’ :’epsg:4326'}
                # # s = gpd.GeoSeries(map(Point, out))#zip(x, y)))

                # # IDX: [1528, 3140] #lon
                # # IDX: [707, 1390]  #lat
                # print(s.head(100))
                # print(len(s.index))
                # #TODO: loop through all products
                # # for prod in data['prod']:
                # #     data = np.array(prod['data'])
                # #     pprint.pprint(prod['type'])
                # #     pprint.pprint(type(data))

                # sys.exit()
################################################################################
# other methods:
# ref: https://stackoverflow.com/questions/37563526/area-intersection-in-python
# ref: https://medium.com/@bobhaffner/spatial-joins-in-geopandas-c5e916a763f3
        


        # oregon = us_states.loc[us_states['NAME']=='Oregon'] # replaced .ix with .loc per warning
        # oregon.reset_index(drop=True, inplace=True) # what does this do?

        # print(us_county.columns)
        # Example for getting specific state data 
        # state = us_county.loc[us_county['STATEFP']!='02']
        # state = state.loc[state['STATEFP']!='15']
        # # county = state.loc[state['NAME']=='Morrow'] # for Oregon
        # county = state.loc[state['COUNTYFP']=='049']

        # ###########################################
        # # TO PLOT CHOROPLETH CHART
        # ###########################################
        # if plot:
        #     # ref: http://geopandas.org/mapping.html
        #     state['test'] = state.AWATER + state.ALAND # example of adding column for plot (see below)
        #     fig = plt.figure(1, figsize=(5,5), dpi=90)
        #     ax = fig.add_subplot(111)
        #     state.plot(ax=ax,column='test', legend=True,
        #                 legend_kwds={'label': "Total Area",
        #                             'orientation': "horizontal"})
        #     plt.show()
        ###########################################


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
    prep = UshmDataPreprocessor()
    prep.run()#(plot=True)
    pass
