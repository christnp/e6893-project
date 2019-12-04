# import csv, json
# import sys

# csv_loc = "/home/christnp/Development/e6893/homework/e6893-project/data/geojson/all-geocodes-v2018.csv"
# json_loc = "all-geocodes-v2018.json"


# f = open( csv_loc, 'rU' )

# columns = ("Summary Level", "State Code (FIPS)", "County Code (FIPS)"," County Subdivision Code (FIPS)", "Place Code (FIPS)", \
# "Consolidtated City Code (FIPS)", "Area Name (including legal/statistical area description)")

# reader = csv.DictReader( f, fieldnames = columns)

# county_fips = {}
# county_fips["state"] = []

# # Store frame names in a list
# for row in reader:
#     if row["County Code (FIPS)"] !=  '000':
#         if row["State Code (FIPS)"] not in county_fips:
#             county_fips["state"] = (row["State Code (FIPS)"]

#         county = {"name":row["Area Name (including legal/statistical area description)"],
#                 "fips": row["County Code (FIPS)"]
#                 }
#         county_fips["state"] = county
#         sys.exit()

#         print("{}\n".format(county))
# #    frame = {"FrameName":row["Frame name"],
# #    "FrameID": row["Frame ID"],
# #    "protocol": row["Protocol"],
# #    "segments":[]}

