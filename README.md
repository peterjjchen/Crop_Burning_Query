## Purpose of Package ##
This is a Python Repository generated to replicate the steps done by Sammi Zhu in https://github.com/sammizhu/satellite_img_ML_model, but instead using Redivis library to query. Additionally, query data for crop burning.

### NOTE: Before running code, make sure to install the libraries and versions required in `requirements.txt`

## Package Overview ##
```
├── GDP_replication/
│   ├── heatmaps/
│   ├── heatmaps_box/
│   ├── GDP_all_coords.py
│   ├── GDP_coords_inside_query.py
│   ├── GDP_midpoint_query.py
│   ├── heatmap.ipynb
│   ├── img_generation_heatmap.py
├── villages_shapefiles/
│   ├── villages_shapefiles.cpg
│   ├── villages_shapefiles.dbf
│   ├── villages_shapefiles.prj
│   ├── villages_shapefiles.shp
│   ├── villages_shapefiles.shx
├── heatmaps/
├── heatmaps_box/
├── aggregate_features.py
├── coords_inside_box.py
├── coords_inside_query.py
├── midpoint_query.py
├── heatmap.ipynb
├── requirements.txt
```
### Crop Burning Files
`heatmap.ipynb`: Processes, analyzes, and visualizes Mosaik data and Satellite images

`midpoint_query.py`: Queries MOSAIKS data for midpoint of each village

`coords_inside_box.py`: Queries MOSAIKS data for the bounding boxes for each village

`coords_inside_query.py`: More fine-grained version of coords_inside_box, query data for points inside the village

`aggregate_features.py`: Aggregate the MOSAIKS features for each village