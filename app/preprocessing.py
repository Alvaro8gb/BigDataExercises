from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.functions import col, isnan, when, count, mean
from pyspark.ml import Pipeline


def has_single_value(df, column_name):
    distinct_count = df.select(column_name).distinct().count()
    return distinct_count == 1


def has_nulls(df, percentage=0.2):
    """
    Create a list of columns to drop based on the null percentages

    """

    total_rows = df.count()
    null_counts_df = df.select([count(when(col(c).contains('None') |
                                           col(c).contains('NULL') |
                                           col(c).contains('NA') |
                                           (col(c) == '') |
                                           col(c).isNull() |
                                           isnan(c), c
                                           )).alias(c)
                                for c in df.columns])

    print("Nulls per col:")
    null_counts_df.show()

    columns_to_drop = [col_name for col_name in null_counts_df.columns if null_counts_df.select(
        col_name).first()[0] > total_rows * percentage]

    return columns_to_drop


def drop_not_interested(df, features_to_drop: list):
    return df.drop(*features_to_drop)


def classify_features(df):

    numeric_features = []
    string_features = []

    for field in df.schema.fields:
        column_name = field.name
        data_type = field.dataType

        if data_type in [IntegerType(), DoubleType()]:
            numeric_features.append(column_name)
        else:
            string_features.append(column_name)

    print("String features:", string_features)
    print("Numeric features:", numeric_features)

    return string_features, numeric_features


def cast(df, params):

    for column in params["string_features"]:
        df = df.withColumn(column, col(column).cast(StringType()))

    for column in params["integer_features"]:
        df = df.withColumn(column, col(column).cast(IntegerType()))

    for column in params["double_features"]:
        df = df.withColumn(column, col(column).cast(DoubleType()))

    df.printSchema()
    print("Dataframe Casted")

    return df


def impute(df):

    string_columns = [column for column,
                      dtype in df.dtypes if dtype == 'string']
    numeric_columns = [column for column,
                       dtype in df.dtypes if dtype in {'int', "double"}]

    values_to_encoded = {}
    for column in string_columns:
        most_common_value = df.groupBy(column) \
            .count() \
            .orderBy("count", ascending=True) \
            .first()[0]

        values_to_encoded[column] = most_common_value

    for column in numeric_columns:
            mean_value = df.agg(mean(col(column))).collect()[0][0]
            values_to_encoded[column] = mean_value

    print("Values to impute", values_to_encoded)
    df = df.fillna(values_to_encoded)
    print("Dataframe imputed")

    return df


def encode(df):

    string_columns = [column for column,
                      dtype in df.dtypes if dtype == 'string']
    numeric_columns = [column for column,
                       dtype in df.dtypes if dtype in {'int', "double"}]

    print("Final String columns:", string_columns)

    stages = []

    for feature in string_columns:
        string_indexer = StringIndexer(
            inputCol=feature, outputCol=f"{feature}_index")
        one_hot_encoder = OneHotEncoder(
            inputCol=f"{feature}_index", outputCol=f"{feature}_encoded")
        stages += [string_indexer, one_hot_encoder]

    # Assemble all features into a single vector
    all_feature_columns = numeric_columns + \
        [f"{feature}_encoded" for feature in string_columns]

    vector_assembler = VectorAssembler(
        inputCols=all_feature_columns, outputCol="features")
    stages += [vector_assembler]

    print("Number of stages:", len(stages))

    pre_pipeline = Pipeline(stages=stages)

    return pre_pipeline.fit(df).transform(df), numeric_columns


def drop_no_correlated(df, numeric_columns:list, target:str, corr_th:float):

    selected_features = [col for col in numeric_columns if abs(df.stat.corr(col, target) ) < corr_th]

    print("Dropping for correlation under", corr_th)
    print(selected_features)

    return drop_not_interested(df, selected_features)

def preprocess(df, params: dict):

    print("Starting Preparation")

    df = drop_not_interested(df, params["features_to_drop"])

    columns_to_drop = [col_name for col_name in df.columns if has_single_value(
        df, col_name)]

    columns_to_drop.extend(has_nulls(df, params["null_th"]))

    print("Columns dropped:", columns_to_drop)

    df = drop_not_interested(df, columns_to_drop)

    df.printSchema()

    df = impute(df)

    df, numeric_columns = encode(df)

    df = drop_no_correlated(df, numeric_columns, params["target"], params["corr_th"])

    print("Preprocesing Finish")

    df.show(n=5)

    return df
