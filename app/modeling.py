from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator


def train(train_data, target: str):
    lr = LinearRegression(featuresCol="features", maxIter=10,
                          regParam=0.3, elasticNetParam=0.8, labelCol=target)
    lr_model = lr.fit(train_data)

    return lr_model


def evaluate(lr_model, test_data, target: str):
    predictions = lr_model.transform(test_data)
    evaluator = RegressionEvaluator(
        predictionCol="prediction", labelCol=target, metricName="rmse")

    evaluator = RegressionEvaluator(
        labelCol=target, predictionCol="prediction")

    # Calculate RMSE
    rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})

    # Calculate MSE
    mse = evaluator.evaluate(predictions, {evaluator.metricName: "mse"})

    # Calculate MAE
    mae = evaluator.evaluate(predictions, {evaluator.metricName: "mae"})

    # Calculate R2
    r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})

    # Print metrics
    print("Root Mean Squared Error (RMSE):", rmse)
    print("Mean Squared Error (MSE):", mse)
    print("Mean Absolute Error (MAE):", mae)
    print("R-squared (R2):", r2)


def fit_and_evaluate(train_data, test_data, target: str):
    model = train(train_data, target)
    evaluate(model, test_data, target)
