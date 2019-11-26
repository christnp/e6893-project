
# import libraries for scraping
import urllib.request
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import requests
import time
import ftplib
import os
import sys
import re


# CMIP5 Model Lookup table
cmip5_model = ['ACCESS1-0', 'ACCESS1-3', 'CCSM4', 'CESM1-BGC',
            'CESM1-CAM5', 'CMCC-CM', 'CMCC-CMS', 'CNRM-CM5',
            'CSIRO-Mk3-6-0', 'CanESM2', 'EC-EARTH', 'FGOALS-g2',
            'GFDL-CM3', 'GFDL-ESM2G', 'GFDL-ESM2M', 'GISS-E2-H',
            'GISS-E2-R', 'HadGEM2-AO', 'HadGEM2-CC', 'HadGEM2-ES',
            'IPSL-CM5A-LR', 'IPSL-CM5A-MR', 'MIROC-ESM', 'MIROC-ESM-CHEM',
            'MIROC5', 'MPI-ESM-LR', 'MPI-ESM-MR', 'MRI-CGCM3', 'NorESM1-M',
            'bcc-csm1-1', 'bcc-csm1-1-m', 'inmcm4']

cmip5_spatial = ['1x1','16th']

cmip5_experiment = ['rcp85','rcp45','historical']

cmip5_feature = ['DTR','pr','tasmax','tasmin']

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
# <activity>/<product>/<institute>/<model>/<experiment>/<frequency>/
#   <modeling realm>/<variable name>/<ensemble member>/
# <variable name>_<MIP table>_<model>_<experiment>_<ensemblemember>[_<temporal subset>].nc 
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
#              25.125° N to 52.875° N and -124.625° E to -67.000° E)
#    resolution: 1/16° latitude-longitude (~ 6 km by 6 km)
###############################################################################
# scrape the website
# https://towardsdatascience.com/how-to-web-scrape-with-python-in-4-minutes-bc49186a8460    
# https://docs.python-guide.org/scenarios/scrape/

def cmip5Model(model):
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

def ftpTraverse(ftp, depth=0, start=None, blacklist=[]):
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
            level[entry] = ftpTraverse(ftp, depth=depth+1)
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
    ####### LOCAL
    #  local directory for downloading the file
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    # Move into the storage path
    loc_start_path = os.getcwd()
    dir_path = os.path.join(os.getcwd(), output_path)
    os.chdir(dir_path)

    ####### FTP Server
    # ftp_handle.cwd(ftp_path)
    # TODO: for-all VH files, download (remove break)
    tempFilePaths = [fnames for fnames in ftp_handle.nlst(ftp_path) if fnames not in blacklist]
    ftp_start_path = ftp_handle.pwd()
    ftp_handle.cwd(ftp_path)
    for tempFilePath in tempFilePaths:
        tempFileName = os.path.split(tempFilePath)[-1]
        # open file for downloading (do we need to download?)
        file = open(tempFileName, 'wb')
        print('Downloading {}'.format(tempFileName))
        try:
            ftp_handle.retrbinary('RETR %s' % tempFileName, file.write)
            print('Successfully downloaded {}'.format(tempFileName))
        except ftplib.all_errors as e:
            errorcode_string = e#str(e).split(None, 1)[0]
            print('Error downloading {0} \n  {1}'.format(tempFileName, errorcode_string))
        # TODO: remove break to download all files in path
        file.close()
        break

    # reset both local and ftp paths
    ftp_handle.cwd(ftp_start_path)
    os.chdir(loc_start_path)

def ftpGetTotalSize(ftp, target_dir, blacklist=[]):  
    '''
    Get the total size of files in 'target_dir' (does not ignore dirs)
    '''
    # need to remember starting dir, then change to target dir
    start_dir = ftp.pwd()
    ftp.cwd(target_dir)
    ls = []
    ftp.retrlines('LIST', ls.append) # list files
    size = 0
    for entry in ls:
        tmp = entry.split()
        f_name = tmp[-1] # file name is last element
        f_size = tmp[4] # file size is 5th element (is this always true?)
        ignore = [i for i in blacklist if f_name in i] # find non-blacklist items
        if(f_size.isnumeric() and not ignore):
            # print(f_name)
            size += float(f_size) 
        # print(entry)
    ftp.cwd(start_dir) # change back to original directory
    
    return size

def format_bytes(B):
   '''
   Return the given bytes as a human friendly KB, MB, GB, or TB string
   '''
   B = float(B)
   KB = float(1024)
   MB = float(KB ** 2) # 1,048,576
   GB = float(KB ** 3) # 1,073,741,824
   TB = float(KB ** 4) # 1,099,511,627,776

   if B < KB:
      return '{0} {1}'.format(B,'Bytes' if 0 == B > 1 else 'Byte')
   elif KB <= B < MB:
      return '{0:.2f} KB'.format(B/KB)
   elif MB <= B < GB:
      return '{0:.2f} MB'.format(B/MB)
   elif GB <= B < TB:
      return '{0:.2f} GB'.format(B/GB)
   elif TB <= B:
      return '{0:.2f} TB'.format(B/TB)

def getClimModelData(src_url,storage,blacklist=[],model=['inmcm4','16th','rcp45','']):
    print("Getting CMIP-5 data from \'{}\'".format(src_url))
    # The C5 source has embedded directories for various models, scenarios, etc.
    # Top > Model > Area ("16th") > Scenario ("historical" & "rcp45") > ? > data
    # levels = 5
    # TODO: list 'Top > Model' and iterate through each model '16th/rcp45/r1i1p1/'
    # src_url = src_url + "inmcm4/16th/rcp45/r1i1p1/"
    c5_src = urlparse(src_url)
    ftp_c5 = ftpConnect(c5_src.netloc)

    if not blacklist:
        # build blacklist and traverse target models/scenarios/areas/features
        dni_model = [n for n in cmip5_model if (n != model[0] and model[0] != '')]
        dni_spati = [n for n in cmip5_spatial if (n != model[1] and model[1] != '')]
        dni_exper = [n for n in cmip5_experiment if (n != model[2] and model[2] != '')]
        dni_featu = [n for n in cmip5_feature if (n != model[3] and model[3] != '')]
        # no_scens = ['rcp85'] #,'historical','rcp45']
        # no_areas = ['1x1'] #,'16th']
        # no_feats = [] #,'DTR','tasmin','tasmax','pr']
        blacklist = dni_model + dni_exper + dni_spati + dni_featu
        # print("dni_model = {}".format(dni_model))
        # print("dni_exper = {}".format(dni_exper))
        # print("dni_spati = {}".format(dni_spati))
        # print("dni_featu = {}".format(dni_featu))
        # temp = ftpTraverse(ftp_c5,start=c5_src.path,blacklist=blacklist)

    # CMIP data on LLNL FTP is stored in dtr, pr, tasmax, and tasmin 'feature' subdirs
    for feature in ftp_c5.nlst(c5_src.path):
        # get size of files in 'feature' subdir (excluding blacklisted items)
        download_size = ftpGetTotalSize(ftp_c5,feature,blacklist=blacklist)
        print("Expected download size: {}".format(format_bytes(download_size)))
        # download files (excluding blacklisted items)
        ftpDownload(ftp_c5,feature,blacklist=blacklist,output_path=storage)
    
    # Close the connection ... it's just good practice    
    ftp_c5.close() 


def getVegHealthData(src_url,storage,blacklist=[]):
    print("Getting VH data from \'{}\'".format(vh_src_url))
    # TODO: multithread file downloading?
    # The VH source is flat, so it is a simple download 
    vh_src = urlparse(vh_src_url)

    ftp_vh = ftpConnect(vh_src.netloc)

    # build a blacklist;
    if not blacklist:  
        vh_prods = [tgt for tgt in ftp_vh.nlst(vh_src.path) if ".VH." in tgt]
        keep = [k for k in vh_prods if "P2006" in k] # for bring up, only download 2006 files 
        
        # TODO: uncomment to get all VH
        # blacklist = [s for s in ftp_vh.nlst(vh_src.path) if not any(sub in s for sub in vh_prods)] 
        blacklist = [s for s in ftp_vh.nlst(vh_src.path) if not any(sub in s for sub in keep)] 

    # get size of files
    download_size = ftpGetTotalSize(ftp_vh,vh_src.path,blacklist=blacklist)
    print("Expected download size: {}".format(format_bytes(download_size)))

    # download
    ftpDownload(ftp_vh,vh_src.path,blacklist=blacklist,output_path=storage)

    # Close the connection ... it's just good practice
    ftp_vh.close()

##################################### MAIN #####################################
if __name__ == "__main__":
    # Case switches for bring-up/debug
    GET_VH = True
    GET_C5 = False

    if GET_VH:
        vh_src = urlparse(vh_src_url)
        getVegHealthData(vh_src.geturl(),"temp/temp_{}".format(vh_src.netloc))

    if GET_C5:
        c5_src_url = c5_src_url + "inmcm4/16th/rcp45/r1i1p1/"
        c5_src = urlparse(c5_src_url)
      
        getClimModelData(c5_src.geturl(),"temp/temp_{}".format(c5_src.netloc))    