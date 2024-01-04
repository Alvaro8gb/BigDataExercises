from pyspark.sql import DataFrame
from pyspark.sql.functions import min, max, col, floor
from functools import reduce


def create_histogram(df: DataFrame, n_bins : int, column_name: str):
    hist_data = df.select(column_name).rdd.flatMap(lambda x: x).histogram(n_bins)

    max_count = reduce(lambda x, y: x if x > y else y, hist_data[1])

    print('Histogram:', column_name)
    
    for bin_start, count in list(zip(hist_data[0], hist_data[1])):
        bin_end = bin_start + 2
        bar_length = int(40 * count / max_count)  # Adjust the scale for visualization
        print(f"{bin_start:.2f} - {bin_end:.2f}: {'*' * bar_length} ({count})")

def explore(df):
     
    delay_month = (
        df
        .groupBy("Year", "Month")
        .agg({"ArrDelay": "avg", "DepDelay": "avg", "Distance": "sum"})
        .orderBy("Year", "Month")
    )

    delay_month.show()

    return delay_month

def analysis(df):

    delay_month = explore(df)

    create_histogram(df, 30, "ArrDelay")

    return delay_month