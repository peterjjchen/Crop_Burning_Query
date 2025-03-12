import os, time, sys

import pandas as pd
from matplotlib import pyplot as plt
import geopandas as gpd
from shapely.geometry import box, Point, Polygon

import redivis

def truncate(n, decimals=0):
    multiplier = 10 ** decimals
    return int(n * multiplier) / multiplier

## These variables will not change
user = redivis.user("sdss")
dataset = user.dataset("mosaiks")
table_name = "mosaiks_2019_planet"

# Load the shapefile
shapefile_path = "villages_shapefiles/villages_shapefiles.shp"
geo_df = gpd.read_file(shapefile_path)

geo_df['centroid'] = geo_df['geometry'].centroid
geo_df['centroid_x'] = [round(truncate(x, 2) + 0.005, 3) for x in geo_df['centroid'].x]
geo_df['centroid_y'] = [round(truncate(y, 2) + 0.005, 3) for y in geo_df['centroid'].y]

chunk_size = 1000
total_rows = geo_df.shape[0]
num_chunks = (total_rows // chunk_size) + 1

centroid_pairs = list(zip(geo_df['centroid_x'], geo_df['centroid_y']))

total_query = pd.DataFrame()
for i in range(num_chunks):
    start_row = i * chunk_size
    end_row = min((i + 1) * chunk_size, total_rows)
    # Generate a list of conditions for each centroid pair
    conditions = " OR ".join([f"(lon = {x} AND lat = {y})" for x, y in centroid_pairs[start_row:end_row]])

    query_str = f"""
        SELECT *
        FROM {table_name}
        WHERE {conditions}
    """
    query_result = dataset.query(query_str).to_pandas_dataframe()
    if i == 0: total_query = query_result
    else: total_query = pd.concat([total_query, query_result], ignore_index=True)

geo_df.drop(columns = ['geometry'], inplace = True)
total_query.to_csv("output/midpoint_query.csv", index=False)
total_query.rename(columns = {'lon': 'centroid_x', 'lat': 'centroid_y'}, inplace = True)
merged_df = geo_df.merge(total_query, how = 'inner', on = ['centroid_x', 'centroid_y'])

# print(query.head())
merged_df.to_csv("output/midpoint_merged.csv", index=False)