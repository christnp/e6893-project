import matplotlib.pyplot as plt
import numpy as np
import json
import pprint

from datetime import datetime,date,timedelta
import os
import sys

from netCDF4 import Dataset

# DEBUG = 3           # Debug levels. 0=no output, 1=results, 2=(1)+variables, 3=(2)+debug
# console = ""        # Console output string



class UshmDataParser():
    """
    docstring description
    """
    def __init__(self,project_base=""):
        if not project_base:
            self.project_base = "/home/christnp/Development/e6893/homework/e6893-project/"
        else:
            self.project_base = project_base
        


    def boundCoords (self,coord_span, coord_step, coord_bounds,res=5):
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
        '''
        try:
            coords = np.arange(coord_span[0],coord_span[-1],coord_step)
        except Exception as e:
            print(e)

        new_coords = []
        idx = []
        for i,coord in enumerate(coords):
            if coord_bounds[0] <= coord <= coord_bounds[1]:
                new_coords.append(np.around(coord,res))
                idx.append(i)
        new_span = [new_coords[0],new_coords[-1]]
        new_idx = [idx[0],idx[-1]]

        return new_idx,new_span

    def convertLonTo180(self):
        return True
    def convertLatTo90(self):
        return True
############################# PARSE VH DATA ####################################
    def parseVH(self,debug=0):
        ''' Documentation for parseVH()

        Function to parse vegetation health data (*.VH.nc) stored in netcdf file
        format. This has only been tested with data found on NOAA STAR ftp server 
        (ftp.star.nesdis.noaa.gov).
        
        Inputs:
        :param: 
        '''

        vh_dir = 'data/ftp.star.nesdis.noaa.gov-static/'
        vh_file = 'VHP.G04.C07.NN.P2006001.VH.nc'
        vh_path = os.path.join(self.project_base,vh_dir,vh_file)

        vh = Dataset(vh_path, mode='r') # using netCDF4 to load .nc file
        # TODO: can this be loaded from FTP server directly?

        # attributes
        vh_product     = getattr(vh,'PRODUCT_NAME')
        vh_year        = getattr(vh,'YEAR')
        vh_week        = getattr(vh,'PERIOD_OF_YEAR')
        vh_y_lat_range = [getattr(vh,'START_LATITUDE_RANGE'), 
                            getattr(vh,'END_LATITUDE_RANGE')]
        vh_x_lon_range = [getattr(vh,'START_LONGITUDE_RANGE'), 
                            getattr(vh,'END_LONGITUDE_RANGE')]
        # dimensions
        vh_y_height    = vh.dimensions['HEIGHT'].size
        vh_x_width     = vh.dimensions['WIDTH'].size
        # variables
        tci = vh['TCI']
        vci = vh['VCI']
        vhi = vh['VHI']

        vh_x_lon_step = (vh_x_lon_range[1]-vh_x_lon_range[0])/vh_x_width
        vh_y_lat_step = (vh_y_lat_range[1]-vh_y_lat_range[0])/vh_y_height

        console = "Parsing \'{}\' data...\n".format(vh_product)     # Console output string

        if debug > 1:
            # VH
            console += "\nVH Data Specs \n"
            console += "  vh_product: {}\n".format(vh_product)
            console += "  vh_year: {}\n".format(vh_year)
            console += "  vh_week: {}\n".format(vh_week)
            console += "  vh_x_lon_range: {}\n".format(vh_x_lon_range)
            console += "  vh_y_lat_range: {}\n".format(vh_y_lat_range)
            console += "  vh_x_width: {}\n".format(vh_x_width)
            console += "  vh_y_height: {}\n".format(vh_y_height)
            console += "  x_lon_step: {}\n".format(vh_x_lon_step)
            console += "  y_lat_step: {}\n".format(vh_y_lat_step)
            console += "  vci.scale_factor: {}\n".format(vci.scale_factor)
            console += "  vci.add_offset: {}\n".format(vci.add_offset)
            console += "  tci.scale_factor: {}\n".format(tci.scale_factor)
            console += "  tci.add_offset: {}\n".format(tci.add_offset)
            console += "  vhi.scale_factor: {}\n".format(vhi.scale_factor)
            console += "  vhi.add_offset: {}\n".format(vhi.add_offset)
        print(console)

        ######################## convert the data ##############################
        vh_date = []
        # res = datetime.strptime("2018 W30 w1", "%Y %W w%w")
        d = "{}-{}-0".format(vh_year,vh_week) # set for YYYY-MM-Sunday
        vh_date.append(datetime.strptime(d, "%Y-%W-%w").date())

        ######################### TODO: MAKE THIS A GLOBAL #####################
        tmp_boxes = json.load(open('country_boxes.json'))
        us_bounds = tmp_boxes['USA']
        lon_bounds = [us_bounds[1],us_bounds[3]] # lat.lower = SW.lat, lat.upper = NE.lat
        lat_bounds = [us_bounds[0],us_bounds[2]] # lon.lower = SW.lon, lon.upper = NE.lon
        ############################################################################

        # new longitude and width indices
        vh_x_lon_idx, vh_x_lon_range = self.boundCoords(vh_x_lon_range,vh_x_lon_step,lon_bounds)
        vh_x_width = abs(vh_x_lon_idx[1] - vh_x_lon_idx[0])

        # new latitude and height indices
        vh_y_lat_idx, vh_y_lat_range = self.boundCoords(vh_y_lat_range,vh_y_lat_step,lat_bounds)
        vh_y_height = abs(vh_y_lat_idx[1] - vh_y_lat_idx[0])
        
        ############## apply bounds and convert VH to weekly data ##############
        # TODO: do this

        ########################## FORMAT OUTPUT ###############################
        vh_json = {}

        vh_json['type'] = vh_product
        vh_json['attr'] = []
        vh_json['vci']  = []
        vh_json['tci']  = []
        vh_json['vhi']  = []

        vh_json['attr'].append({
            'year': vh_date[0].year,
            'week': vh_date,
            'lon_start': vh_x_lon_range[0],
            'lon_end': vh_x_lon_range[1],
            'lon_step': vh_x_lon_step,
            'lat_start': vh_y_lat_range[0],
            'lat_end': vh_y_lat_range[1],
            'lat_step': vh_y_lat_step
        })

        vh_json['vci'].append({
            'units':   [],
            'data':   []
        })
        vh_json['tci'].append({
            'units':   [],
            'data':   []
        })
        vh_json['vhi'].append({
            'units':   [],
            'data':   []
        })

        if debug > 1:
            pprint.pprint(vh_json)

        return vh_json
    

    def parseCmip(self,debug=0):
        """ docstring description

        """
        c5_dir = 'scratch/temp/gdo-dcp.ucllnl.org/'
        c5_file = 'pr_day_inmcm4_rcp45_r1i1p1_20060101-20061231.LOCA_2016-04-02.16th.nc'
        c5_path = os.path.join(self.project_base,c5_dir,c5_file)

        c5 = Dataset(c5_path, mode='r') # using netCDF4 to load .nc file

        # attributes
        c5_model_id = getattr(c5,'model_id')
        # dimensions
        c5_bands_dim    = c5.dimensions['bnds'].size
        c5_y_height     = c5.dimensions['lat'].size
        c5_x_width      = c5.dimensions['lon'].size
        c5_time_dim     = c5.dimensions['time'].size
        # variables
        c5_lon          = c5['lon'][:]      # units: degrees_east
        c5_lon_units    = c5['lon'].units
        # c5_lon_bands    = c5['lon_bnds']
        c5_lat          = c5['lat'][:]      # units: degrees_north
        c5_lat_units    = c5['lat'].units
        # c5_lat_bands    = c5['lat_bnds']
        c5_time         = c5['time'][:]        # units: days since 1900-01-01
        c5_time_units   = c5['time'].units
        # c5_time_bnds    = c5['time_bnds']
        c5_pr           = c5['pr']
        c5_pr_units     = c5['pr'].units
        c5_pr_fill      = c5['pr']._FillValue

        ######################## convert the data ##############################
        #   - to proper coordinate range (-180 to 180, -90 to 90)
        #   - time from days since 1900.01.01 to year/week
        # ref: https://stackoverflow.com/questions/46962288/change-longitude-from-180-to-180-to-0-to-360
        # convert coordinates
        # TODO: implement these hepers
        self.convertLonTo180()
        self.convertLatTo90()
        c5_x_lon_new = ((np.asarray(c5_lon[:]) - 180) % 360) - 180
        c5_x_lon_range = [c5_x_lon_new[0],c5_x_lon_new[-1]]
        c5_y_lat_new = ((np.asarray(c5_lat[:]) - 90) % 180) - 90
        c5_y_lat_range = [c5_y_lat_new[0],c5_y_lat_new[-1]]
        # update step
        c5_x_lon_step = (abs(c5_x_lon_range[0])+abs(c5_x_lon_range[1]))/c5_x_width
        c5_y_lat_step = (abs(c5_y_lat_range[0])+abs(c5_y_lat_range[1]))/c5_y_height

        # convert days since 1900.01.01 to datetime
        # ref: https://stackoverflow.com/questions/38691545/python-convert-days-since-1990-to-datetime-object
        c5_date = []
        for i in range(0,c5_time_dim):
            start = date(1900,1,1)      # This is the "days since" part
            delta = timedelta(c5_time[i])     # Create a time delta object from the number of days
            c5_date.append(start + delta)      # Add the specified number of days to 1990

        console = "Parsing CMIP-5 \'{}\' data...\n".format(c5_model_id)     # Console output string
        if debug > 1:
            # CMIP5-LOCA
            console += "\nCMIP5-LOCA Specs \n"
            console += "c5_model_id: {}\n".format(c5_model_id)
            console += "c5_bands_dim: {}\n".format(c5_bands_dim)
            console += "c5_y_height: {}\n".format(c5_y_height)
            console += "c5_x_width: {}\n".format(c5_x_width)
            console += "c5_time_dim:  {}\n".format(c5_time_dim)
        print(console)

        ######################### bound the data ###############################
        # https://boundingbox.klokantech.com/
        #   - USA:  [[[-125.2489197254,25.0753572133],[-66.6447598244,25.0753572133],
        #               [-66.6447598244,49.2604253419],[-125.2489197254,49.2604253419],
        #               [-125.2489197254,25.0753572133]]]
        # https://gist.github.com/graydon/11198540
        #   - country boxes in format: [SW.lat, SW.lon, NE.lat, NE.lon]
        tmp_boxes = json.load(open('country_boxes.json'))
        us_bounds = tmp_boxes['USA']
        lon_bounds = [us_bounds[1],us_bounds[3]] # lat.lower = SW.lat, lat.upper = NE.lat
        lat_bounds = [us_bounds[0],us_bounds[2]] # lon.lower = SW.lon, lon.upper = NE.lon

        # new longitude and width indices
        # vh_x_lon_idx, vh_x_lon_range = boundCoords(vh_x_lon_range,vh_x_lon_step,lon_bounds)
        c5_x_lon_idx,c5_x_lon_range = self.boundCoords(c5_x_lon_range,c5_x_lon_step,lon_bounds)
        # vh_x_width = abs(vh_x_lon_idx[1] - vh_x_lon_idx[0])
        c5_x_width = abs(c5_x_lon_idx[1] - c5_x_lon_idx[0])

        # new latitude and height indices
        # vh_y_lat_idx, vh_y_lat_range = boundCoords(vh_y_lat_range,vh_y_lat_step,lat_bounds)
        c5_y_lat_idx, c5_y_lat_range = self.boundCoords(c5_y_lat_range,c5_y_lat_step,lat_bounds)
        # vh_y_height = abs(vh_y_lat_idx[1] - vh_y_lat_idx[0])
        c5_y_height = abs(c5_y_lat_idx[1] - c5_y_lat_idx[0])

        ############## apply bounds and convert C5 to weekly data ##############
        week_arr = np.empty((7,c5_y_height,c5_x_width),dtype=np.float32)
        mean_arr = np.empty((1,c5_y_height,c5_x_width),dtype=np.float32)
        tmp = np.empty((c5_y_height,c5_x_width),dtype=np.float32)
        # indices for bounding
        y0,y1 = c5_y_lat_idx[0], c5_y_lat_idx[1]
        x0,x1 = c5_x_lon_idx[0], c5_x_lon_idx[1]

        week = 0
        day = 0.0
        week_list = []
        mean_list =[]
        for i,tslice in enumerate(c5_pr):
            # bounded dataset for time i (mask removed and masked values set to 0.0)
            tmp = tslice[y0:y1,x0:x1].filled(0.0) 

            # ref: https://stackoverflow.com/questions/34357617/append-2d-array-to-3d-array-extending-third-dimension
            week_list.append(tmp)
            day += 1
            if i%7 == 0 and i !=0: 
                week += 1
                # print("day: {0}, week: {1}".format(i,week))
                week_arr = np.dstack(week_list)
                mean_arr = np.nanmean(week_arr,axis=2) # np.dstack() makes 3rd axis the stack axis, not the 1st axis
                mean_list.append(mean_arr)
                # reset local vars
                week_arr = np.empty((7,c5_y_height,c5_x_width),dtype=np.float32)
                mean_arr = np.empty((1,c5_y_height,c5_x_width),dtype=np.float32)
                day = 0.0
                week_list = []
            else:
                # mark non-week dates for removal
                c5_date[i] = "remove"
        # update the date object
        c5_date = [value for value in c5_date if value != "remove"]
        if debug > 2:
            print("Dates for CMIP-5: \n{}".format(c5_date))

        # store list of weekly averages into a numpy array, and reshape (time,lat,lon)
        c5_pr_new = np.empty((c5_y_height,c5_x_width,52))
        c5_pr_new = np.dstack(mean_list)
        c5_pr_new = np.moveaxis(c5_pr_new,2,0) # dstack makes 3rd axis the time axis, so swap

        # getting memory error when allocating numpy arrays:
        # https://stackoverflow.com/questions/57507832/unable-to-allocate-array-with-shape-and-data-type
        ########################################################################

        c5_json = {}

        c5_json['type']     = c5_model_id # TODO: make this cmip-5 + model
        c5_json['attr']     = []    # global attributes
        c5_json['pr']       = []    # pr specific
        c5_json['tasmin']   = []    # tasmin specific
        c5_json['tasmax']   = []    # tasmax specific
        c5_json['dtr']      = []    # dtr specific

        c5_json['attr'].append({
            'year':         c5_date[0].year,
            'week':         c5_date,
            'lon_start':    c5_x_lon_range[0],
            'lon_end':      c5_x_lon_range[1],
            'lon_step':     c5_x_lon_step,
            'lon_units':    c5_lon_units,
            'lat_start':    c5_y_lat_range[0],
            'lat_end':      c5_y_lat_range[1],
            'lat_step':     c5_y_lat_step,
            'lat_units':    c5_lat_units
        })

        c5_json['pr'].append({
            'units':    c5_pr_units,
            'data':     c5_pr_new
        })
        c5_json['tasmin'].append({
            'units':    [],
            'data':     []
        })
        c5_json['tasmax'].append({
            'units':    [],
            'data':     []
        })
        c5_json['dtr'].append({
            'units':    [],
            'data':     []
        })

        # precipitation, kg m-2 s-1 (converted to mm/day in 'Subset Request' interface)
        # minimum surface air temperature, °K (converted to °C in 'Subset Request' interface)
        # maximum surface air temperature, °K (converted to °C in 'Subset Request' interface)
        if debug > 1:
            # pprint.pprint(vh_json)
            pprint.pprint(c5_json)
        
        return c5_json


    def deprecated(self):
        #################################### CMIP/VH ###################################

        # vh_dir = 'data/ftp.star.nesdis.noaa.gov-static/'
        # vh_file = 'VHP.G04.C07.NN.P2006001.VH.nc'
        # vh_path = os.path.join(project_base,vh_dir,vh_file)

        # c5_dir = 'data/gdo-dcp.ucllnl.org-static/'
        # c5_file = 'pr_day_inmcm4_rcp45_r1i1p1_20060101-20061231.LOCA_2016-04-02.16th.nc'
        # c5_path = os.path.join(project_base,c5_dir,c5_file)

        # vh = Dataset(vh_path, mode='r') # using netCDF4 to load .nc file
        # c5 = Dataset(c5_path, mode='r') # using netCDF4 to load .nc file

        ## VH 
        # # attributes
        # vh_product     = getattr(vh,'PRODUCT_NAME')
        # vh_year        = getattr(vh,'YEAR')
        # vh_week        = getattr(vh,'PERIOD_OF_YEAR')
        # vh_y_lat_range = [getattr(vh,'START_LATITUDE_RANGE'), getattr(vh,'END_LATITUDE_RANGE')]
        # vh_x_lon_range = [getattr(vh,'START_LONGITUDE_RANGE'), getattr(vh,'END_LONGITUDE_RANGE')]
        # # dimensions
        # vh_y_height    = vh.dimensions['HEIGHT'].size
        # vh_x_width     = vh.dimensions['WIDTH'].size
        # # variables
        # tci = vh['TCI']
        # vci = vh['VCI']
        # vhi = vh['VHI']

        # vh_x_lon_step = (vh_x_lon_range[1]-vh_x_lon_range[0])/vh_x_width
        # vh_y_lat_step = (vh_y_lat_range[1]-vh_y_lat_range[0])/vh_y_height

        ## CMIP5
        # attributes
        # c5_model_id = getattr(c5,'model_id')
        # # dimensions
        # c5_bands_dim    = c5.dimensions['bnds'].size
        # c5_y_height     = c5.dimensions['lat'].size
        # c5_x_width      = c5.dimensions['lon'].size
        # c5_time_dim     = c5.dimensions['time'].size
        # # variables
        # c5_lon          = c5['lon'][:]      # units: degrees_east
        # c5_lon_units    = c5['lon'].units
        # # c5_lon_bands    = c5['lon_bnds']
        # c5_lat          = c5['lat'][:]      # units: degrees_north
        # c5_lat_units    = c5['lat'].units
        # # c5_lat_bands    = c5['lat_bnds']
        # c5_time         = c5['time']        # units: days since 1900-01-01
        # c5_time_units   = c5['time'].units
        # # c5_time_bnds    = c5['time_bnds']
        # c5_pr           = c5['pr']
        # c5_pr_units     = c5['pr'].units
        # c5_pr_fill      = c5['pr']._FillValue

        # print(c5_pr_units)
        # sys.exit()
        # # convert to proper coordinate range (-180 to 180, -90 to 90)
        # # ref: https://stackoverflow.com/questions/46962288/change-longitude-from-180-to-180-to-0-to-360
        # c5_x_lon_new = ((np.asarray(c5_lon[:]) - 180) % 360) - 180
        # c5_x_lon_range = [c5_x_lon_new[0],c5_x_lon_new[-1]]

        # c5_y_lat_new = ((np.asarray(c5_lat[:]) - 90) % 180) - 90
        # c5_y_lat_range = [c5_y_lat_new[0],c5_y_lat_new[-1]]

        # c5_x_lon_step = (abs(c5_x_lon_range[0])+abs(c5_x_lon_range[1]))/c5_x_width
        # c5_y_lat_step = (abs(c5_y_lat_range[0])+abs(c5_y_lat_range[1]))/c5_y_height

        # C5 variables(dimensions): 
        # float64 lon(lon), float64 lon_bnds(lon,bnds), 
        # float64 lat(lat), float64 lat_bnds(lat,bnds), 
        # float64 time(time), float64 time_bnds(time,bnds), 
        # float32 pr(time,lat,lon)

        # if DEBUG > 1:
        #     # CMIP5-LOCA
        #     console += "\nCMIP5-LOCA Specs \n"
        #     console += "c5_model_id: {}\n".format(c5_model_id)
        #     console += "c5_bands_dim: {}\n".format(c5_bands_dim)
        #     console += "c5_y_height: {}\n".format(c5_y_height)
        #     console += "c5_x_width: {}\n".format(c5_x_width)
        #     console += "c5_time_dim:  {}\n".format(c5_time_dim)
            # VH
            # console += "\nVH Specs \n"
            # console += "vh_product: {}\n".format(vh_product)
            # console += "vh_year: {}\n".format(vh_year)
            # console += "vh_week: {}\n".format(vh_week)
            # console += "vh_x_lon_range: {}\n".format(vh_x_lon_range)
            # console += "vh_y_lat_range: {}\n".format(vh_y_lat_range)
            # console += "vh_x_width: {}\n".format(vh_x_width)
            # console += "vh_y_height: {}\n".format(vh_y_height)
            # console += "x_lon_step: {}\n".format(vh_x_lon_step)
            # console += "y_lat_step: {}\n".format(vh_y_lat_step)
            # console += "vci.scale_factor: {}\n".format(vci.scale_factor)
            # console += "vci.add_offset: {}\n".format(vci.add_offset)
            # console += "tci.scale_factor: {}\n".format(tci.scale_factor)
            # console += "tci.add_offset: {}\n".format(tci.add_offset)
            # console += "vhi.scale_factor: {}\n".format(vhi.scale_factor)
            # console += "vhi.add_offset: {}\n".format(vhi.add_offset)

            # print(console)

        # ############################ bound the data ###################################
        # # https://boundingbox.klokantech.com/
        # #   - USA:  [[[-125.2489197254,25.0753572133],[-66.6447598244,25.0753572133],
        # #               [-66.6447598244,49.2604253419],[-125.2489197254,49.2604253419],
        # #               [-125.2489197254,25.0753572133]]]
        # # https://gist.github.com/graydon/11198540
        # #   - country boxes in format: [SW.lat, SW.lon, NE.lat, NE.lon]
        # tmp_boxes = json.load(open('country_boxes.json'))
        # us_bounds = tmp_boxes['USA']
        # lon_bounds = [us_bounds[1],us_bounds[3]] # lat.lower = SW.lat, lat.upper = NE.lat
        # lat_bounds = [us_bounds[0],us_bounds[2]] # lon.lower = SW.lon, lon.upper = NE.lon

        # # new longitude and width indices
        # # vh_x_lon_idx, vh_x_lon_range = boundCoords(vh_x_lon_range,vh_x_lon_step,lon_bounds)
        # c5_x_lon_idx,c5_x_lon_range = boundCoords(c5_x_lon_range,c5_x_lon_step,lon_bounds)
        # # vh_x_width = abs(vh_x_lon_idx[1] - vh_x_lon_idx[0])
        # c5_x_width = abs(c5_x_lon_idx[1] - c5_x_lon_idx[0])

        # # new latitude and height indices
        # # vh_y_lat_idx, vh_y_lat_range = boundCoords(vh_y_lat_range,vh_y_lat_step,lat_bounds)
        # c5_y_lat_idx, c5_y_lat_range = boundCoords(c5_y_lat_range,c5_y_lat_step,lat_bounds)
        # # vh_y_height = abs(vh_y_lat_idx[1] - vh_y_lat_idx[0])
        # c5_y_height = abs(c5_y_lat_idx[1] - c5_y_lat_idx[0])

        # ################## apply bounds and convert C5 to weekly data ##################
        # week_arr = np.empty((7,c5_y_height,c5_x_width),dtype=np.float32)
        # mean_arr = np.empty((1,c5_y_height,c5_x_width),dtype=np.float32)
        # tmp = np.empty((c5_y_height,c5_x_width),dtype=np.float32)
        # # indices for bounding
        # y0,y1 = c5_y_lat_idx[0], c5_y_lat_idx[1]
        # x0,x1 = c5_x_lon_idx[0], c5_x_lon_idx[1]

        # week = 0
        # day = 0.0
        # week_list = []
        # mean_list =[]
        # for i,tslice in enumerate(c5_pr):
        #     # bounded dataset for time i (mask removed and masked values set to 0.0)
        #     tmp = tslice[y0:y1,x0:x1].filled(0.0) 

        #     # ref: https://stackoverflow.com/questions/34357617/append-2d-array-to-3d-array-extending-third-dimension
        #     week_list.append(tmp)
        #     day += 1
        #     if i%7 == 0 and i !=0: 
        #         week += 1
        #         # print("day: {0}, week: {1}".format(i,week))
        #         week_arr = np.dstack(week_list)
        #         mean_arr = np.nanmean(week_arr,axis=2) # np.dstack() makes 3rd axis the stack axis, not the 1st axis
        #         mean_list.append(mean_arr)
        #         # reset local vars
        #         week_arr = np.empty((7,c5_y_height,c5_x_width),dtype=np.float32)
        #         mean_arr = np.empty((1,c5_y_height,c5_x_width),dtype=np.float32)
        #         day = 0.0
        #         week_list = []
        # # store list of weekly averages into a numpy array, and reshape (time,lat,lon)
        # c5_pr_new = np.empty((c5_y_height,c5_x_width,52))
        # c5_pr_new = np.dstack(mean_list)
        # c5_pr_new = np.moveaxis(c5_pr_new,2,0) # dstack makes 3rd axis the time axis, so swap

        # # getting memory error when allocating numpy arrays:
        # # https://stackoverflow.com/questions/57507832/unable-to-allocate-array-with-shape-and-data-type
        # ###############################################################################

        # vh_json = {}
        # vh_json['type'] = vh_product
        # vh_json['attr'] = []
        # vh_json['data'] = [] #vh_vci_new

        # vh_json['attr'].append({
        #     'year': vh_year,
        #     'week': vh_week,
        #     'lon_start': vh_x_lon_range[0],
        #     'lon_end': vh_x_lon_range[1],
        #     'lon_step': vh_x_lon_step,
        #     'lat_start': vh_y_lat_range[0],
        #     'lat_end': vh_y_lat_range[1],
        #     'lat_step': vh_y_lat_step
        # })

        # vh_json['data'].append({
        #     'vci':   [],
        #     'tci':   [],
        #     'vhi':   []
        # })

        # c5_json = {}
        # c5_json['type'] = c5_model_id # TODO: make this cmip-5 + model
        # c5_json['attr'] = []
        # c5_json['data'] = [] #c5_pr_new # TODO: make this a list of all types (pr, tasmin, tasmax)

        # c5_json['attr'].append({
        #     'year':         c5_time[0],
        #     'week':         c5_time[0],
        #     'lon_start':    c5_x_lon_range[0],
        #     'lon_end':      c5_x_lon_range[1],
        #     'lon_step':     c5_x_lon_step,
        #     'lat_start':    c5_y_lat_range[0],
        #     'lat_end':      c5_y_lat_range[1],
        #     'lat_step':     c5_y_lat_step
        # })

        # c5_json['data'].append({
        #     'pr':       c5_pr_new,
        #     'tasmin':   [],
        #     'tasmax':   [],
        #     'dtr':      []
        # })


        # # precipitation, kg m-2 s-1 (converted to mm/day in 'Subset Request' interface)
        # # minimum surface air temperature, °K (converted to °C in 'Subset Request' interface)
        # # maximum surface air temperature, °K (converted to °C in 'Subset Request' interface)
        # if DEBUG > 1:
        #     # pprint.pprint(vh_json)
        #     pprint.pprint(c5_json)

        # # print(c5_time_bnds[:][:])
        # sys.exit()





        # with open('c5_data.json', 'w') as outfile:
        #     json.dump(c5_json, outfile)

        # # geo_cart_map = np.empty(tci.shape, dtype=(tci.range.dtype,2))

        # # print(geo_cart_map.shape)
        # # for xi in range(1,geo_cart_map.shape[1]):
        # #     for yi in range(1,geo_cart_map.shape[0]):
        # #         geo_cart_map[yi][xi] =  [xi*x_lon_step+x_lon_range[0], yi*y_lat_step+y_lat_range[0]]

        # # test = np.fromfunction(lambda x, y: (x*vh_x_lon_step+vh_x_lon_range[0], y*vh_y_lat_step+vh_y_lat_range[0]), tci.shape, dtype=tci.range.dtype)

        # # print(test[0][0].size)
        # # print(geo_cart_map)
        # sys.exit()

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
        return True


if __name__ == "__main__":

    print("Running {} locally".format(__file__))

    ushm = UshmDataParser()
    # test_c5 = ushm.parseCmip(debug=0)
    test_vh = ushm.parseVH(debug=3)
    pprint.pprint(test_vh)

    # print(parseVH())

    pass
    