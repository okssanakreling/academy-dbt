import time
from sklearn.model_selection import train_test_split


class RegressionModel:
    def __init__(self, data, model):
        self.data = data
        self.model = model

    def split_dataset(self):
        x = self.data.iloc[:, :-1].values
        y = self.data.iloc[:, -1].values
        x_train, x_test, y_train, y_test = train_test_split(
            x,
            y,
            test_size=1/4,
            random_state=42
        )

        self.train_data = (x_train, y_train)
        self.test_data = (x_test, y_test)

    def train(self):
        self.split_dataset()
        start = time.time()
        self.model.fit(*self.train_data)
        self.time = time.time() - start
        return self

    def evaluate(self):
        self.score = self.model.score(*self.test_data)
        return self

    def predict(self, df):
        response = self.model.predict(df.iloc[:].values)
        df["predicted"] = response
        return df
