import fire


from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def load_data(spark, path):
    return spark.read.csv(path, header=True, inferSchema=True)


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

    # Show the analysis result
    analysis_result.show()


def main(data_path):
    spark = SparkSession.builder.appName("FlightAnalysisApp").getOrCreate()

    flights_df = load_data(spark, data_path)

    flights_df = prepocesing(flights_df)

    analysis(flights_df)

    spark.stop()


if __name__ == "__main__":
    # Use Fire to automatically generate a command-line interface
    fire.Fire(main)
