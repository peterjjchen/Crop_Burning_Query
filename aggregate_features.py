import pandas as pd

def aggregate(data, file=""):
    # Load the dataset and rename duplicate columns
    df = data

    # Fill NaN values with 0 for averaging
    df = df.fillna(0)

    # Identify feature columns (all columns with 'X' in the name)
    feature_columns = [col for col in df.columns if 'X' in col]

    # Group by 'v_id' and calculate the mean for each feature column
    average_features_df = df.groupby('v_shp_id')[feature_columns].mean().reset_index()

    # Save the result to a new file (without Lat and Lon)
    output_file = f"output/{file}average.csv"
    average_features_df.to_csv(output_file, index=False)

    print(f"Averaged features saved to {output_file}")

# Example usage
merged_file = "output/coords_inside.csv"
df = pd.read_csv(merged_file)
aggregate(df, "coords_inside_")