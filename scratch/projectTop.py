from urllib.parse import urlparse


# import dataParser
import dataScraper
# import dataPreprocessor


#############################
# TODO:
# 1) Put data into Google BQ dataset:
#       * download from FTP server, as temp_[fname].nc on Google bucket
#       * open temp_[fname].nc from Google bucket
#       * parse files, storing data as local variables
#       * transform/convert any data (i.e., convert x/y to lat/lon)
#       * bound data as necessary (i.e., get contiguous US states, weekly data)
#       * store local vars in JSON file, save to bitbucket as temp_[fname].json
#       * load temp JSON from bitbucket into Google BQ dataset/table
#           - define schema
#           - table for C5, table for VH
#       * cleanup temp directory on Google Bucket.
#############################



################################ DOWNLOAD DATA ################################
# Case switches for bring-up/debug
GET_VH = True
GET_C5 = False

# data source
vh_src_url = "ftp://ftp.star.nesdis.noaa.gov/pub/corp/scsb/wguo/data/Blended_VH_4km/VH/"
c5_src_url = "ftp://gdo-dcp.ucllnl.org/pub/dcp/archive/cmip5/loca/LOCA_2016-04-02/"

if GET_VH:
    vh_src = urlparse(vh_src_url)
    dataScraper.getVegHealthData(vh_src.geturl(),"temp/temp_{}".format(vh_src.netloc))

if GET_C5:
    c5_src_url = c5_src_url + "inmcm4/16th/rcp45/r1i1p1/"
    c5_src = urlparse(c5_src_url)    
    dataScraper.getClimModelData(c5_src.geturl(),"temp/temp_{}".format(c5_src.netloc))   

# TODO: save in google bucket

################################## PARSE DATA ##################################
