# -*- coding: utf-8 -*-
'''
Format of a document stored in my MongoDB:
a_document = {
"_id" : ObjectId("5b3cc75f63569d2a8ad9443c"),
"properties" : {
         "CSDUID" : 3525005, 
         "CSDNAME" : "Hamilton",
         ...
},
"geometry" : {
         "type" : "MultiPolygon",
         "coordinates" :  [[[
                  [-79.86045371315016, 43.34735059806326],
                  [-79.8603510847835,43.34732740773817],
                  [-79.86028395897642,43.34734094412329],
                  [-79.86025988330843,43.34734580623075],
                  ...
         ]]]
},
...
}
My Goal is to flatten this nested geojson into a table dataframe of long lat
'''

from pymongo import MongoClient
import pymongo
from pandas.io.json import json_normalize
import pandas as pd
try:
    client = MongoClient('localhost', 27017)
except pymongo.errors.ConnectionFailure as e:
    print("Could not connect to MongoDB: %s" % e)
db = client.YourDatabase
collection = db.YourCollection

# Get one city for test
locations = collection.find_one()
geo = locations['geometry']
coords = geo['coordinates']
# Flatten json to df
# If you have a simple json, this step should be sufficient to get the data frame df
# But in case of nested json, further manipulation is needed
df = json_normalize(geo)
# Because MultiPolygon type is stored is [[[ [x,y],... ]]], so we need to visit the inner array 
# df['coordinates'][1] is null
for i in df['coordinates'][0]:
    # Currenly the whole multiplygon coordinate array is [[  [x,y], ...]]
    # First split 'coordinates', which is an 2 element array list, into 2 columns
    # by splitting first and second index
    # .apply(pd.Series) breaks the list of tuples into columns
    # .stack() to flip these columns into rows aka a Series
    # .to_frame() to convert back to DataFrame
    # Finally rename the columns
    df2 = (df['coordinates'].apply(lambda r: [(j[0],j[1]) for j in i[0]])
        .apply(pd.Series)
        .stack()
        .reset_index(level=1)
        .rename(columns={0:'coordinates',"level_1":"point"})
        )
    # Split 'coordinates' to 'lat', 'long' columns
    df2[['lat','long']] = df2['coordinates'].apply(pd.Series)
    print(df2)
