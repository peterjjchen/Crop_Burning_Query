import os, time, sys, math

import pandas as pd
from matplotlib import pyplot as plt
import geopandas as gpd
from shapely.geometry import box, Point, Polygon

# Library used for Redivis API to query MOSAIKS data
# https://github.com/Global-Policy-Lab/mosaiks_tutorials/blob/main/01_querying_redivis.ipynb
import redivis

# Used for binary search
import bisect
    
def binary_search_dataframe(df, lon, lat):
    # Create a combined key for the target values
    target_key = (lon, lat)

    # Create a list of combined keys from the DataFrame
    keys = list(zip(df['lon'], df['lat']))

    # Use bisect_left to find the insertion point
    i = bisect.bisect_left(keys, target_key)

    # Check if the target key exists at the found index
    if i != len(keys) and keys[i] == target_key:
        return df.iloc[i]
    else:
        return None
    
def write_file(file, data):
    with open(file, 'w') as f:
        f.write(data)

# These variables will not change
# Path to MOSAIKS dataset on Redivis
user = redivis.user("sdss")
dataset = user.dataset("mosaiks")
table_name = "mosaiks_2019_planet"

def compute_bounding_box(polygon):
    minx, miny, maxx, maxy = polygon.bounds
    return miny, maxy, minx, maxx  # Returning (min lat, max lat, min lon, max lon)

# Query all coords within a bounding box and merge the results
def coords_inside():

    data_request_path = "files/Mosaik_Shrid_coordinates_Data_Davide_Rahul_Ashesh_Leonard_2024 - Mosaik_Shrid_coordinates_Data_Davide_Rahul_Ashesh_Leonard_2024.csv"

    # Load the dataset and rename duplicate columns
    df = pd.read_csv(data_request_path)

    df.set_index('shrid2', inplace=True)

    # Initialize lists to store bounding box results
    min_lats = []
    max_lats = []
    min_lons = []
    max_lons = []
    geometries = []
    
    for _, row in df.iterrows():
        polygon = Polygon(eval(row['polygon_coordinates']))
        
        # Compute the bounding box
        min_lat, max_lat, min_lon, max_lon = compute_bounding_box(polygon)
        
        # Store the results
        min_lats.append(min_lat)
        max_lats.append(max_lat)
        min_lons.append(min_lon)
        max_lons.append(max_lon)
        geometries.append(polygon)
    
    # Add results to the dataframe
    df['min_lat'] = min_lats
    df['max_lat'] = max_lats
    df['min_lon'] = min_lons
    df['max_lon'] = max_lons
    df['geometry'] = geometries

    min_lat_total = min(df['min_lat'])
    max_lat_total = max(df['max_lat'])
    min_lon_total = min(df['min_lon'])
    max_lon_total = max(df['max_lon'])

    chunk_size = 0.3
    x_diff = max_lon_total - min_lon_total
    y_diff = max_lat_total - min_lat_total
    num_chunks_x = math.ceil((x_diff // chunk_size) + 1)
    num_chunks_y = math.ceil((y_diff // chunk_size) + 1)

    total_query = pd.DataFrame()
    for i in range(num_chunks_x):
        for j in range(num_chunks_y):
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
            query_result = dataset.query(query_str).to_pandas_dataframe()
            if i == 0 and j == 0: total_query = query_result
            else: total_query = pd.concat([total_query, query_result], ignore_index=True)

    # Convert the query result into a GeoDataFrame of points
    points_gdf = gpd.GeoDataFrame(
        total_query,
        geometry=gpd.points_from_xy(total_query.lon, total_query.lat)
    )
    # Ensure both GeoDataFrames use the same CRS (e.g., WGS84)
    points_gdf.set_crs(epsg=4326, inplace=True)

    geo_df = gpd.GeoDataFrame(df, geometry="geometry")
    geo_df.set_crs(epsg=4326, inplace=True)

    # Create a new column with bounding box polygons for each row
    # (Assuming each row's geometry can be represented as a bounding box)
    geo_df['bbox'] = geo_df['geometry'].apply(lambda geom: box(*geom.bounds))

    # Prepare the geo_df for spatial join (only need the bbox geometry)
    geo_boxes = geo_df[['bbox']].copy()
    # Optionally, preserve the original index by resetting the index column name
    geo_boxes = geo_boxes.rename(columns={'bbox': 'geometry'})
    geo_boxes = geo_boxes.set_geometry('geometry')

    # Perform spatial join to tag each point with the corresponding geo_df row
    joined = gpd.sjoin(points_gdf, geo_boxes, how='inner', predicate='within')
    # 'index_right' now holds the index from geo_df that the point belongs to

    # Group the points by the v_shp_id (now stored in 'index_right')
    results_by_box = {v_shp_id: group for v_shp_id, group in joined.groupby('v_shp_id')}

    for v_shp_id, result in results_by_box.items():
        results_by_box[v_shp_id] = result.sort_values(by=['lon', 'lat'])

    combined_gdf_rows = pd.concat(results_by_box.values(), ignore_index=True).set_index('v_shp_id')
    combined_gdf_rows.to_csv("output/coords_inside_merged.csv")
    
# Query all coords using exact coordinates
def exact_coords_query(file):

    df = pd.read_csv(file)

    # Restrict the number of rows for query
    chunk_size = 5000
    total_rows = df.shape[0]
    num_chunks = (total_rows // chunk_size) + 1

    coord_pairs = list(zip(df['Lon'], df['Lat']))

    total_query = []
    for i in range(num_chunks):
        start_row = i * chunk_size
        end_row = min((i + 1) * chunk_size, total_rows)
        # Generate a list of conditions for each centroid pair
        conditions = " OR ".join([f"(lon = {x} AND lat = {y})" for x, y in coord_pairs[start_row:end_row]])

        query_str = f"""
            SELECT *
            FROM {table_name}
            WHERE {conditions}
        """
        query_result = dataset.query(query_str).to_pandas_dataframe()
        total_query.append(query_result)
        if i % 5 == 0: 
            print(f"Processed chunk {i + 1} of {num_chunks}")
            time.sleep(2)

    combined_result = pd.concat(total_query, ignore_index=True)
    return combined_result

# Auxiliary function
def combine_and_sort_results():
    query_1 = pd.read_csv("output/all_data_queries_1.csv")
    query_2 = pd.read_csv("output/all_data_queries_2.csv")
    query_3 = pd.read_csv("output/all_data_queries_3.csv")

    query_result = pd.concat([query_1, query_2, query_3], ignore_index=True)
    query_result.sort_values(by=['lon', 'lat'], inplace=True)
    query_result['coords'] = list(zip(query_result['lon'], query_result['lat']))
    query_result.to_csv('output/all_data_queries.csv', index=False)

# Query from local files that already contain MOSAIKS data
def query_from_files(file):
    result_df = pd.read_csv(file)
    if 'queried' not in result_df.columns:
        result_df['queried'] = 0
    if 'coords' not in result_df.columns:
        result_df['coords'] = list(zip(result_df['Lon'], result_df['Lat']))
    result_df.set_index('coords', inplace=True)

    query_result = pd.read_csv("output/all_data_queries.csv")
    query_result.set_index('coords', inplace=True)

    count = 0
    not_queried = []

    try:
        for index, row in result_df.iterrows():
            if row['queried'] == 1: continue

            X_values = binary_search_dataframe(query_result, row['Lon'], row['Lat'])
            if X_values is not None:
                for i in range(4000):
                    result_df.loc[index, f'X_{i}'] = X_values[f'X_{i}']
                result_df.loc[index, 'queried'] = 1
                count += 1
                if count % 100 == 0: print(f"Processed {count} rows")
            else:
                not_queried.append(index)
    except:
        result_df.to_csv(file)
        write_file("output/not_queried.txt", "\n".join(not_queried))
        return

    
    result_df.to_csv(file)
    print(f"Changed {count} rows")
    write_file("output/not_queried.txt", "\n".join(not_queried))

# Add a column to the dataframe to indicate if the row has been queried
def add_queried_column(file):
    df = pd.read_csv(file)
    if 'queried' not in df.columns:
        df['queried'] = 0
    df.to_csv(file, index=False)

# Add the X columns to the dataframe where MOSAIKS data can be stored
def add_X_columns(file):
    df = pd.read_csv(file)
    for i in range(4000):
        df[f'X_{i}'] = 0.0
    df.to_csv(file, index=False)