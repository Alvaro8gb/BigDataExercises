import os

def create_folder_if_not_exists(folder_path):
    """
    Create a folder at the specified path if it does not exist.
    
    Parameters:
    - folder_path (str): The path of the folder you want to create.
    
    Returns:
    None
    """
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Folder created at: {folder_path}")
    else:
        raise Exception(f"The folder already exists at: {folder_path}")
