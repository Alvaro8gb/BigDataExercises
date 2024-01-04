import fire
import time

from pyspark.sql import SparkSession
from db import create_folder_if_not_exists

from input import load_data, load_params
from preprocessing import preprocess, cast
from eda import analysis
from modeling import fit_and_evaluate


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

    params = load_params(params_path)

    print("Paramns\n", params)

    spark.sparkContext.setLogLevel(params["logLevel"])

    spark.conf.set("spark.ml.seed", params["seed"])

    flights_df = load_data(spark, data_path)

    flights_df = cast(flights_df, params)

    analysis_result = analysis(flights_df)

    if out_path:
        analysis_result.write.partitionBy("Year").json(out_path + "/analys")

    flights_df = preprocess(flights_df, params)

    flights_df.printSchema()

    train_size = 1 - params["test_size"]
    (train_data, test_data) = flights_df.randomSplit(
        [train_size, params["test_size"]], seed=params["seed"])

    fit_and_evaluate(train_data, test_data, params["target"])

    spark.stop()


if __name__ == "__main__":

    start_time = time.time()
    # Use Fire to automatically generate a command-line interface
    fire.Fire(main)
    end_time = time.time()
    print(f"Execution time: {end_time - start_time} segs")
