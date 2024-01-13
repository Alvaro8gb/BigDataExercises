import os
import json
from functools import reduce
from tqdm import tqdm


def validate_dictionary(data):
    """
    Validates if the provided dictionary has all the required keys.
    Throws an error if any of the keys are missing.

    Parameters:
    data (dict): The dictionary to be validated.

    Returns:
    bool: True if the dictionary is valid, else raises an error.
    """

    # List of required keys
    required_keys = [
        "features_to_drop", "string_features", "target",
        "integer_features", "double_features", "seed", "logLevel", "model_params", 
        "null_th", "corr_th", "test_split"
    ]

    # Check if all required keys are in the dictionary
    for key in required_keys:
        if key not in data:
            raise ValueError(f"Key '{key}' is missing from the dictionary")

    return True


def load_data(spark, path: str):

    file_paths = [os.path.join(path, file)
                  for file in os.listdir(path) if file.endswith(".bz2")]

    if len(file_paths) == 0:
        raise Exception("Not data files found!!")

    df_frames = [spark.read.csv(f, header=True, inferSchema=True) for f in tqdm(
        file_paths, "Loading source files from path: "+path)]

    combined_df = reduce(lambda df1, df2: df1.unionAll(df2), df_frames)

    print("Number of instances:", combined_df.count())
    return combined_df


def load_params(path: str) -> dict:
    params = {}
    with open(path) as f:
        params = json.load(f)

    validate_dictionary(params)

    print("\nParamns:\n", json.dumps(params, indent=4))


    return params
