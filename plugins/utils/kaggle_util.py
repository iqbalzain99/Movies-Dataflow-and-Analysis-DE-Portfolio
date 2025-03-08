import kaggle
import os
import plugins.config.creds_config as conf

os.environ['KAGGLE_USERNAME'] = conf.kaggle_creds['username']
os.environ['KAGGLE_KEY'] = conf.kaggle_creds['key']

def download_kaggle_dataset(dataset_name, download_path):
    """
    Downloads a Kaggle dataset to a specified path.

    Args:
        dataset_name (str): The name of the dataset in the format 'owner/dataset-name'.
        download_path (str): The path to the directory where the dataset should be downloaded.
    """
    try:
        # Ensure the download directory exists
        if not os.path.exists(download_path):
            os.makedirs(download_path)

        # Download the dataset
        if is_folder_empty(download_path):
            kaggle.api.dataset_download_files(dataset_name, path=download_path, unzip=True) #unzip=True automatically unzips the downloaded files.
            print(f"Dataset '{dataset_name}' downloaded successfully to '{download_path}'.")
        else:
            print(f"Folder '{download_path}' has been filled.")

    except Exception as e:
        print(f"Error downloading dataset '{dataset_name}': {e}")

def is_folder_empty(folder_path):
    """Checks if a folder is empty."""
    try:
        return not bool(os.listdir(folder_path))
    except FileNotFoundError:
        return True