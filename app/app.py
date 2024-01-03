import fire
import os
import json
import pyspark.ml.feature as feature
from tqdm import tqdm
from pyspark.sql import SparkSession
from globals import TARGET
from functools import reduce
from db import create_folder_if_not_exists
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import mean
from pyspark.sql.types import DoubleType



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

def process_dataframe(df, target_col):
    # Calculate mean value
    mean_val = df.select(mean(df[target_col])).collect()[0][0]

    # Fill missing values with mean value
    df = df.na.fill(mean_val, subset=[target_col])

    # Change column type to DoubleType
    df = df.withColumn(target_col, df[target_col].cast(DoubleType()))

    return df

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
    TARGET = "ArrDelay" 
    spark = SparkSession.builder.appName("FlightAnalysisApp").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    flights_df = load_data(spark, data_path)

    params = load_params(params_path)
    print("Paramns\n", params)

    flights_df = prepocesing(flights_df)
    
    flights_df = process_dataframe(flights_df, TARGET)
    # print(flights_df.printSchema())
    # flights_df.show(5, truncate=False)

    if out_path:
        analysis_result = analysis(flights_df)
        analysis_result.write.partitionBy("Year").json(out_path + "/analys")
        
    # from the dataframe 'flights_df' do feature subset optimal selection using pyspark
    # Feature Selection
    # Here we're assuming you've done this manually and selected the relevant columns
    selected_features = ["Distance"]

    # Feature Engineering
    # This is highly dependent on your specific dataset
    # For simplicity, we'll just use the selected features as-is
    # # Create a VectorAssembler
    # assembler = feature.VectorAssembler(inputCols=selected_features)

    # # Transform the DataFrame
    # output = assembler.transform(flights_df)

    # # Show the resulting DataFrame
    # output.show(truncate=False)

    # Prepare data for Machine Learning
    assembler = feature.VectorAssembler(inputCols=selected_features, outputCol="features")
    data = assembler.transform(flights_df)

    # Split the data
    (train_data, test_data) = data.randomSplit([0.7, 0.3])

    # Model Training
    lr = LinearRegression(featuresCol='features', labelCol='ArrDelay')
    lr_model = lr.fit(train_data)

    # Model Evaluation
    predictions = lr_model.transform(test_data)
    evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="ArrDelay", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

    spark.stop()


if __name__ == "__main__":
    # Use Fire to automatically generate a command-line interface
    fire.Fire(main)
