import os, time, sys, math

import pandas as pd
from matplotlib import pyplot as plt
import geopandas as gpd
from shapely.geometry import box, Point, Polygon

import redivis

## These variables will not change
user = redivis.user("sdss")
dataset = user.dataset("mosaiks")
table_name = "mosaiks_2019_planet"

data_request_path = "GDP_files/Mosaik_Shrid_coordinates_Data_Davide_Rahul_Ashesh_Leonard_2024 - Mosaik_Shrid_coordinates_Data_Davide_Rahul_Ashesh_Leonard_2024.csv"

# Load the dataset and rename duplicate columns
df = pd.read_csv(data_request_path)

df.set_index('shrid2', inplace=True)

def compute_bounding_box(polygon):
    minx, miny, maxx, maxy = polygon.bounds
    return miny, maxy, minx, maxx  # Returning (min lat, max lat, min lon, max lon)

def truncate(n, decimals=0):
    multiplier = 10 ** decimals
    return int(n * multiplier) / multiplier

def midpoint():
    # Initialize lists to store bounding box results
    min_lats = []
    max_lats = []
    min_lons = []
    max_lons = []
    centroids = []
    
    
    for _, row in df.iterrows():
        polygon = Polygon(eval(row['polygon_coordinates']))
        
        # Compute the bounding box
        min_lat, max_lat, min_lon, max_lon = compute_bounding_box(polygon)
        
        # Store the results
        min_lats.append(min_lat)
        max_lats.append(max_lat)
        min_lons.append(min_lon)
        max_lons.append(max_lon)
         
        # Calculate and Save centroid data
        centroids.append((polygon.centroid.x, polygon.centroid.y)) 
   

    
    # Add results to the dataframe
    df['min_lat'] = min_lats
    df['max_lat'] = max_lats
    df['min_lon'] = min_lons
    df['max_lon'] = max_lons
    df['centroid_x'] = [round(truncate(centroid[0], 2) + 0.005, 3) for centroid in centroids]
    df['centroid_y'] = [round(truncate(centroid[1], 2) + 0.005, 3) for centroid in centroids]

    # Restrict the number of rows for query
    chunk_size = 1000
    total_rows = df.shape[0]
    num_chunks = (total_rows // chunk_size) + 1

    centroid_pairs = list(zip(df['centroid_x'], df['centroid_y']))

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

    total_query.to_csv("output/GDP/query_1.csv", index=False)
    total_query.rename(columns = {'lon': 'centroid_x', 'lat': 'centroid_y'}, inplace = True)
    merged_df = df.merge(total_query, how = 'inner', on = ['centroid_x', 'centroid_y'])

    # print(query.head())
    merged_df.to_csv("output/GDP/merged.csv", index=False)