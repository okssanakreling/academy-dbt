from queries import ProductByStores, DatesAhead
from models import RegressionModel
import pandas_gbq


def evaluate_model(linear_model):
    df = ProductByStores().get_df()
    model = (
        RegressionModel(df, linear_model)
        .train()
        .evaluate()
    )

    print(linear_model)
    print("dataset size", len(df))
    print("train dataset size", len(model.train_data[0]))
    print("test dataset size", len(model.test_data[0]))
    print("R²", model.score)
    print("time", model.time)
    return model


def predict_days_ahead(model):
    df = DatesAhead().get_df()
    return model.predict(df)


def write_bigquery(df, table_name):
    pandas_gbq.to_gbq(df, table_name, if_exists="replace")


if __name__ == "__main__":
    from sys import argv
    from sklearn.linear_model import LinearRegression, LogisticRegression, ElasticNet
    from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor

    model_name = "linear"
    model = LinearRegression()
    if argv[-1] == "linear":
        model_name = "linear"
        model = LinearRegression()
    elif argv[-1] == "logistic":
        model_name = "logistic"
        model = LogisticRegression()
    elif argv[-1] == "elasticnet":
        model_name = "elasticnet"
        model = ElasticNet()
    elif argv[-1] == "gbr":
        model_name = "gbr"
        model = GradientBoostingRegressor()
    elif argv[-1] == "randomforest":
        model_name = "randomforest"
        model = RandomForestRegressor()

    model = evaluate_model(model)
    sales_forecast = predict_days_ahead(model)
    write_bigquery(sales_forecast, f"ml.sales_forecast_{model_name}")
