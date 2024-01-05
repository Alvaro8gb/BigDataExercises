from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator


def train(train_data, target: str, model_params:dict):
    lr = LinearRegression(featuresCol="features", labelCol=target, **model_params)
    lr_model = lr.fit(train_data)

    trainingSummary = lr_model.summary

    print("Training stats:")
    print(f"numIterations: {trainingSummary.totalIterations}")
    trainingSummary.residuals.show()
    print(f"RMSE: {trainingSummary.rootMeanSquaredError}")
    print(f"r2: {trainingSummary.r2}")
   
    print(f"Coefficients: {lr_model.coefficients}")
    print(f"Intercept: {lr_model.intercept}")

    return lr_model


def evaluate(lr_model, test_data, target: str):
    predictions = lr_model.transform(test_data)

    evaluator = RegressionEvaluator(
        predictionCol="prediction", labelCol=target, metricName="rmse")

    evaluator = RegressionEvaluator(
        labelCol=target, predictionCol="prediction")

    rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
    mse = evaluator.evaluate(predictions, {evaluator.metricName: "mse"})
    mae = evaluator.evaluate(predictions, {evaluator.metricName: "mae"})
    r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})

    print("\nMetrics over test:")
    print("Root Mean Squared Error (RMSE):", rmse)
    print("Mean Squared Error (MSE):", mse)
    print("Mean Absolute Error (MAE):", mae)
    print("R-squared (R2):", r2)


def fit_and_evaluate(train_data, test_data, target: str, model_params:dict):
    model = train(train_data, target, model_params)
    evaluate(model, test_data, target)
