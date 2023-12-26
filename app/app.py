import fire
import os
import json
from tqdm import tqdm
from pyspark.sql import SparkSession
from globals import TARGET
from functools import reduce
from db import create_folder_if_not_exists


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

    return params


def prepocesing(flights_df):
    features_to_drop = [
        "ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn",
        "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay",
        "SecurityDelay", "LateAircraftDelay"
    ]

    return flights_df.drop(*features_to_drop)


def analysis(flights_df):

    # Perform some analysis (replace column names as needed)
    analysis_result = (
        flights_df
        .groupBy("Year", "Month")
        .agg({"ArrDelay": "avg", "DepDelay": "avg", "Distance": "sum"})
        .orderBy("Year", "Month")
    )

    analysis_result.show()

    return analysis_result


def main(data_path: str, params_path: str, out_path: str = None):
    """
        Run the application.

        Parameters:
        - data_path (str): The path to the input data folder.
        - params_path (str): The path to the parameters file.
        - out_path (str): The paths to save results.

        Returns: None

        Example:
        >>> main("/path/to/data.csv", "/path/to/params.json")
    """

    if out_path:
        print("Try to create folder", out_path)
        create_folder_if_not_exists(out_path)

    spark = SparkSession.builder.appName("FlightAnalysisApp").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    flights_df = load_data(spark, data_path)

    params = load_params(params_path)
    print("Paramns\n", params)

    flights_df = prepocesing(flights_df)

    analysis_result = analysis(flights_df)

    # TODO FSS
    # TODO Split
    # TODO Training
    # TODO Evalaute

    if out_path:
        analysis_result.write.partitionBy("Year").json(out_path + "/analys")

    spark.stop()


if __name__ == "__main__":
    # Use Fire to automatically generate a command-line interface
    fire.Fire(main)
