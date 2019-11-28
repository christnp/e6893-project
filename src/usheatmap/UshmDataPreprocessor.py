import shapely.geometry
import shapely.speedups
import geopandas as gpd
import matplotlib.pyplot as plt

import json
import os
import sys


class UshmDataPreprocessor():
    """ docstring for sphinx
    """
    def __init__(self):
        """ docstring
        """
        # static FIPs state codes
        self.state_fips = "geojson/cont_stateToFips.json"
        # static GeoJSON boundary files
        # tl_ -> Tiger/Line GeoJSON provides extra metadata
        # cb_ -> Cartographic Boundary GeoJSON for boundary mapping only
        self.state_path   = "geojson/cb_2018_us_state_500k.json"
        self.county_path  = "geojson/cb_2018_us_county_500k.json" 

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

    def run(self,plot=False):
        shapely.speedups.enable()
        
        # load state FIPS codes
        try: 
            us_fips = json.load(open(self.state_fips))
        except:
            print("Failed to load fips codes...")
            us_fips = {"Kansas"                 :  "20"}
        
        # load state/county border GeoJSON
        # us_states = self.readGeoJSON(us_states_path)
        us_county = self.readGeoJSON(self.county_path)

        ####################### TODO: ###############################
        # 0) import data from UshmDataParser.py
        # 1) go through some counties, use shape to create mask
        #   a) develop np mask generator function
        #   b) apply mask to data from (0)
        # 2) average data values from (0) that are not masked 'np.nanmean(week_arr,axis=2)'
        #   a) pick a small county and convince myself it is right (raw data lookup?)
        # 3) use results to plot choropleth plot as below (to convince) ourselves
        #   a) plot use methods below
        #   b) use panoply to confirm? will need a few neighboring counties...






        # oregon = us_states.loc[us_states['NAME']=='Oregon'] # replaced .ix with .loc per warning
        # oregon.reset_index(drop=True, inplace=True) # what does this do?

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
        if plot:
            # ref: http://geopandas.org/mapping.html
            state['test'] = state.AWATER + state.ALAND # example of adding column for plot (see below)
            fig = plt.figure(1, figsize=(5,5), dpi=90)
            ax = fig.add_subplot(111)
            state.plot(ax=ax,column='test', legend=True,
                        legend_kwds={'label': "Total Area",
                                    'orientation': "horizontal"})
            plt.show()
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
