import os, time, sys

import pandas as pd
from matplotlib import pyplot as plt
import geopandas as gpd
from shapely.geometry import box, Point

import redivis

## These variables will not change
user = redivis.user("sdss")
dataset = user.dataset("mosaiks")

# Load the shapefile
shapefile_path = "villages_shapefiles/villages_shapefiles.shp"
geo_df = gpd.read_file(shapefile_path)
geo_df = geo_df.set_index('v_shp_id')


# Create a new column with bounding box polygons for each row
# (Assuming each row's geometry can be represented as a bounding box)
geo_df['bbox'] = geo_df['geometry'].apply(lambda geom: box(*geom.bounds))

# Compute the overall bounding box for all rows
xmin_total, ymin_total, xmax_total, ymax_total = geo_df.total_bounds

table_name = "mosaiks_2019_planet"
query_str = f"""
    SELECT *
    FROM {table_name}
    WHERE lon > {xmin_total}
      AND lon < {xmax_total}
      AND lat > {ymin_total}
      AND lat < {ymax_total}
"""
# Execute a single query covering the overall bounds
query = dataset.query(query_str)
df = query.to_pandas_dataframe()

# Convert the query result into a GeoDataFrame of points
points_gdf = gpd.GeoDataFrame(
    df,
    geometry=gpd.points_from_xy(df.lon, df.lat)
)
# Ensure both GeoDataFrames use the same CRS (e.g., WGS84)
points_gdf.set_crs(epsg=4326, inplace=True)
geo_df.set_crs(epsg=4326, inplace=True)

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