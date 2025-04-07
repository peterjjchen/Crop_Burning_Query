import os, time, sys, math

import pandas as pd
from matplotlib import pyplot as plt
import geopandas as gpd
from shapely.geometry import box, Point, Polygon

# Library used for Redivis API to query MOSAIKS data
# https://github.com/Global-Policy-Lab/mosaiks_tutorials/blob/main/01_querying_redivis.ipynb
import redivis

# These variables will not change
# Path to MOSAIKS dataset on Redivis
user = redivis.user("sdss")
dataset = user.dataset("mosaiks")
table_name = "mosaiks_2019_planet"

data_request_path = "files/Mosaik_Shrid_coordinates_Data_Davide_Rahul_Ashesh_Leonard_2024 - Mosaik_Shrid_coordinates_Data_Davide_Rahul_Ashesh_Leonard_2024.csv"

# Load the dataset
df = pd.read_csv(data_request_path)

# Find the max and min latitudes and longitudes
min_lat_total = min(df['min_lat']) - 0.001
max_lat_total = max(df['max_lat']) + 0.001
min_lon_total = min(df['min_lon']) - 0.001
max_lon_total = max(df['max_lon']) + 0.001

# Perform the query in chunks to avoid crashing redivis query
chunk_size = 0.3
x_diff = max_lon_total - min_lon_total
y_diff = max_lat_total - min_lat_total
num_chunks_x = math.ceil((x_diff // chunk_size) + 1)
num_chunks_y = math.ceil((y_diff // chunk_size) + 1)

start_chunk_x = 0
start_chunk_y = 0
end_chunk_x = num_chunks_x
end_chunk_y = num_chunks_y

for i in range(start_chunk_x, end_chunk_x):
    for j in range(start_chunk_y, end_chunk_y):
        print(f"Processing chunk {i + 1} of {num_chunks_x} in x and {j + 1} of {num_chunks_y} in y")
        min_lat = min_lat_total + j * chunk_size
        max_lat = min(min_lat_total + (j + 1) * chunk_size, max_lat_total)
        min_lon = min_lon_total + i * chunk_size
        max_lon = min(min_lon_total + (i + 1) * chunk_size, max_lon_total)

        query_str = f"""
            SELECT *
            FROM {table_name}
            WHERE lon > {min_lon}
                AND lon < {max_lon}
                AND lat > {min_lat}
                AND lat < {max_lat}
        """
        query_result = dataset.query(query_str).to_pandas_dataframe().sort_values(by=['lon', 'lat'])

        if i == start_chunk_x and j == start_chunk_y:
            query_result.to_csv("output/all_data_queries_4.csv", mode='w', header=True, index=False)
        else:
            query_result.to_csv("output/all_data_queries_4.csv", mode='a', header=False, index=False)