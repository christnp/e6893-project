
# import libraries for scraping
import urllib.request
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import requests
import time
import ftplib
import os
import sys


################################# VEG HEALTH #################################
# vegitation health data source top level; weekly NetCDF *.VH.nc files for VHI, 
# TCI, and VCI features
# https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/vh_ftp.php
vh_src_url = "ftp://ftp.star.nesdis.noaa.gov/pub/corp/scsb/wguo/data/Blended_VH_4km/VH/"

# Per https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH_doc/VHP_uguide_v2.0_2018_0727.pdf
# Data arrays are in geographic projection (grid with equal latitude
# and longitude interval). The size of data array can be found by
# calling NETCDF function or using interactive tools such as HDFview.
# The array is in row major order. The first point of array is at the
# north-west corner of the grid. Then it goes eastward and then
# southward.
# For 4km VHP product, the arrays are with size 10000x3616, Covers
# latitude [-55.152 to 75.024], longitude [-180.0, 180.0] (outside
# border of the spatial area of VHP product).
# The size of grid pixel:
# dLon= dLat = (360.0/10000)
# For any pixel [i,j] in the array, the position of pixel’s center
# is calculated as:
# Latitude = (75.024 - (j+0.5) *dLat) (j: counts from 0 to 3615)
# Longitude = (-180.0 + (i+0.5)* dLon) (i: counts from 0 to 9999)
# In data of 2018, the latitude and longitude fields are added into
# the dataset.
# The spatial coverage of data array are described by the file
# attributes of NETCDF file, example:
#  START_LATITUDE_RANGE = 75.024
#  START_LONGITUDE_RANGE = -180.0
#  END_LATITUDE_RANGE = -55.152
#  END_LONGITUDE_RANGE = 180.0 
# In the new version (data of 2014), these attributes' names were
# changed as below (example):
#  geospatial_lat_min = -55.152
#  geospatial_lon_min = -180.0
#  geospatial_lat_max = 75.024
#  geospatial_lon_max = 180.0
##############################################################################

################################ CMIP-5 LOCA #################################
# CMIP-5 LOCA datasource top level; sub directories for each model and 1x1 or 
# 16th scale, daily NetCDF .nc files. The variables in the VHP file are saved as 
# scaled 16-bits integers. 
# https://gdo-dcp.ucllnl.org/downscaled_cmip_projections/dcpInterface.html#Projections:%20Complete%20Archives
c5_src_url = "ftp://gdo-dcp.ucllnl.org/pub/dcp/archive/cmip5/loca/LOCA_2016-04-02/"
# 
#|-- LOCA_2016-04-02/	                        Level 0 (l0)
#       |-- {model name1}                        Level 1 (l1)
#       |        |-- 16th (km)                    Level 2 (l2)
#       |        |   |-- historical                Level 3 (l3)
#       |        |   |   |-- r1i1p1                 Level 4 (l4)
#       |        |   |-- rcp45                     Level 3 (l3)
#       |        |   |   |-- r8i1p1                 Level 4 (l4)
#       |        |   |   |   |-- DTR                 Level 5 (l5)
#       |        |   |   |   |-- pr (precipitation)  Level 5 (l5)
#       |        |   |   |   |-- tasmax              Level 5 (l5)
#       |        |   |   |   |-- tasmin              Level 5 (l5)
#       |        |   |-- rcp85
#       |        |   |   |-- r2i1p1
#       |        |-- 1x1 (km)
#       |            |-- D.N.C.
#       |-- {model name 2}
# c5_l0 = "LOCA_2016-04-02"
# c5_11 = "ACCESS1-3"
# c5_12 = "16th"
# c5_13 = "rcp85"
# c5_14 = "r1i1p1"
# c5_15 = "DTR"
# c5_l6 = {data}

# Each LOCA climate projection has the following attributes:
# Variables:
#    precipitation, kg m-2 s-1 (TODO: convert to mm/day)
#    minimum surface air temperature, °K (TODO: convert to °C)
#    maximum surface air temperature, °K (TODO: convert to °C)
#    missing value flag: 1E+30
# Time:
#    coverage: 1950-2099
#    resolution: daily
# Space:
#    coverage: North American Land-Data Assimilation System domain (i.e. contiguous 
#              U.S. plus portions of southern Canada and northern Mexico, spanning 
#              25.125° N to 52.875° N and - 124.625° E to -67.000° E)
#    resolution: 1/16° latitude-longitude (~ 6 km by 6 km)
###############################################################################
# scrape the website
# https://towardsdatascience.com/how-to-web-scrape-with-python-in-4-minutes-bc49186a8460    
# https://docs.python-guide.org/scenarios/scrape/

def correct_model(model):
    ''' Correct name of models that have two, to make search work '''
    # list model as dict{dir name : search name}
    models={"ACCESS1-0" : "ACCESS1.0", "ACCESS1-3" : "ACCESS1.3",
            "CESM1-BGC" : "CESM1(BGC)", "CESM1-CAM5" : "CESM1(CAM5)",
            "CESM1-CAM5-1-FV2" : "CESM1(CAM5.1,FV2)", "CESM1-WACCM" : "CESM1(WACCM)",
            "CESM1-FASTCHEM" : "CESM1(FASTCHEM)", "bcc-csm1-1" : "BCC-CSM1.1",
            "bcc-csm1-1-m" : "BCC-CSM1.1(m)", "inmcm4" : "INM-CM4"}  
    # if the current model is one of the dict keys, change name
    if model in models.keys():
       return models[model]
    return model

def traverse(ftp, depth=0, start=None, blacklist=[]):
    """
    return a recursive listing of an ftp server contents (starting
    from the current directory)

    listing is returned as a recursive dictionary, where each key
    contains a contents of the subdirectory or None if it corresponds
    to a file.

    @param ftp: ftplib.FTP object
    """
    if depth > 10:
        return ['depth > 10']

    if start:
        ftp.cwd(start)
    # add '.' and '..' to blacklist
    blacklist = blacklist + ['.', '..']

    level = {}
    for entry in (path for path in ftp.nlst() if path not in blacklist):
        try:
            print(entry)
            ftp.cwd(entry)
            level[entry] = traverse(ftp, depth=depth+1)
            # print(level[entry])
            ftp.cwd('..')
        except ftplib.error_perm:
            level[entry] = None
    return level

def ftpConnect(netloc):
    # https://medium.com/@rrfd/ftp-access-with-python-1d096b061ef3
    try:
        ftp_handle = ftplib.FTP(netloc) 
        ftp_handle.login()
        print('Connection to \'{}\' successful'.format(netloc))
    except ftplib.all_errors as e:
        errorcode_string = str(e).split(None, 1)[0]
        print('Connection to \'{}\' failed'.format(netloc))
        print('  Error: \'{}\''.format(errorcode_string))

    return ftp_handle

def ftpDownload(ftp_handle,ftp_path,blacklist=[],output_path="."):
    # data_dir = "data"
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    # Move into the folder
    dir_path = os.path.join(os.getcwd(), output_path)
    os.chdir(dir_path)

    # change to data directory
    ftp_handle.cwd(ftp_path)

    # TODO: for-all files, download
    tempFileNames = [fnames for fnames in ftp_handle.nlst() if fnames not in blacklist]
    for i in tempFileNames:
        if "SM" not in i:
            tempFileName = i
            break

    # tempDirectory = os.path.join(vh_source_top,all_files[0])
    file = open(tempFileName, 'wb')

    # ftp_handle.cwd(tempDirectory)

    print('Downloading {}'.format(tempFileName))
    try:
        ftp_handle.retrbinary('RETR %s' % tempFileName, file.write)
        print('Successfully downloaded {}'.format(tempFileName))
    except ftplib.all_errors as e:
        errorcode_string = e#str(e).split(None, 1)[0]
        print('Error downloading {0} \n  {1}'.format(tempFileName, errorcode_string))


# TODO: multithread file downloading?
# The VH source is flat, so it is a simple download 
vh_src = urlparse(vh_src_url)
ftp_vh = ftpConnect(vh_src.netloc)
# build a blacklist; for now, only download 2006 files
ftp_vh.cwd(vh_src.path)
blacklist = [tgt for tgt in ftp_vh.nlst() if "P2006" not in tgt]
ftp_vh.cwd('..') # reset directory loc
# download
ftpDownload(ftp_vh,vh_src.path,blacklist=blacklist,output_path="data/{}".format(vh_src.netloc))


# The C5 source has embedded directories for various models, scenarios, etc.
# Top > Model > Area ("16th") > Scenario ("historical" & "rcp45") > ? > data
# levels = 5
c5_src_url = c5_src_url + "/inmcm4/16th/rcp45/r1i1p1/" # TODO: traverse!
c5_src = urlparse(c5_src_url)
ftp_c5 = ftpConnect(c5_src.netloc)
storage = "data/{}".format(c5_src.netloc)

# build blacklist and traverse target models/scenarios/areas/features
no_models = ['ACCESS1-0', 'ACCESS1-3', 'CCSM4', 'CESM1-BGC', 'CESM1-CAM5', 'CMCC-CM', 'CMCC-CMS', 'CNRM-CM5', 'CSIRO-Mk3-6-0', 'CanESM2', 'EC-EARTH', 'FGOALS-g2', 'GFDL-CM3', 'GFDL-ESM2G', 'GFDL-ESM2M', 'GISS-E2-H', 'GISS-E2-R', 'HadGEM2-AO', 'HadGEM2-CC', 'HadGEM2-ES', 'IPSL-CM5A-LR', 'IPSL-CM5A-MR', 'MIROC-ESM', 'MIROC-ESM-CHEM', 'MIROC5', 'MPI-ESM-LR', 'MPI-ESM-MR', 'MRI-CGCM3', 'NorESM1-M', 'bcc-csm1-1', 'bcc-csm1-1-m']#, 'inmcm4']
no_scens = ['rcp85'] #,'historical','rcp45']
no_areas = ['1x1'] #,'16th']
no_feats = [] #,'DTR','tasmin','tasmax','pr']
blacklist = no_models + no_scens + no_areas + no_feats
# temp = traverse(ftp_c5,start=c5_src.path,blacklist=blacklist)
# print(temp)

# get into next to last level
ftp_c5.cwd(c5_src.path)
for feature in ftp_c5.nlst():
    ftpDownload(ftp_c5,feature,storage)
    ftp_c5.cwd('..')
sys.exit()

# Close the connection ... it's just good practice
ftp_vh.close()
