import pandas as pd
import os

def import_csvs_and_merge(folder_path):
    """
    Import csv's from the same source folder, then merge them together,
    Make sure that the data that being used in the same format

    Args:
        folder_path (str): The path to the directory where the dataset located
    Returns:
        Merged dataframe (pandas.DataFrame)
    """
    try:
        # Ensure the download directory exists
        if not os.path.exists(folder_path):
            print(f"Folder doesn't exist '{folder_path}': {e}")

        # Load the dataset
        return pd.concat([ pd.read_csv(file_name) for file_name in get_file_names(folder_path) ])
        

    except Exception as e:
        print(f"Error loading dataset from '{folder_path}': {e}")

def get_file_names(folder_path):
    """Returns a list of file names in the given folder."""
    file_names = []
    for item in os.listdir(folder_path):
        item_path = os.path.join(folder_path, item)
        if os.path.isfile(item_path) and item.lower().endswith('.csv'):
            file_names.append(item_path)
    return file_names