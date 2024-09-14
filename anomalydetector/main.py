import os
from collections import defaultdict

import numpy as np
# for local dev, load env vars from a .env file
from dotenv import load_dotenv
from quixstreams import Application
from sklearn.ensemble import IsolationForest

load_dotenv()

app = Application(consumer_group="transformation-v1",
                  auto_offset_reset="earliest",
                  broker_address='kafka_broker:9092')

input_topic = app.topic(os.environ["input"])
output_topic = app.topic(os.environ["output"])

high_volume_threshold = defaultdict(lambda: 20000)
fit_prices = []  # collect the prices to fit into the model
is_fitted = False  # to check if the Isolation Forest model has been trained

isolation_forest = IsolationForest(contamination=0.01, n_estimators=1000)


def high_volume_rule(trade_data):
    trade_data['high_volume_anomaly'] = bool(trade_data['size'] > high_volume_threshold[trade_data['symbol']])
    return trade_data


def isolation_forest_rule(trade_data):
    global is_fitted
    current_price = trade_data['price']

    fit_prices.append(float(current_price))

    if len(fit_prices) < 1000:
        trade_data['isolation_forest_anomaly'] = False
        return trade_data

    fit_prices_normalised = (np.array(fit_prices) - np.mean(fit_prices)) / np.std(fit_prices)
    prices_reshaped = fit_prices_normalised.reshape(-1, 1)

    if len(fit_prices) % 1000 == 0:
        isolation_forest.fit(prices_reshaped)
        is_fitted = True

    if not is_fitted:
        trade_data['isolation_forest_anomaly'] = False
        return trade_data

    current_price_normalised = (current_price - float(np.mean(fit_prices))) / float(np.std(fit_prices))
    score = isolation_forest.decision_function([[current_price_normalised]])

    trade_data['isolation_forest_anomaly'] = bool(score[0] < 0)  # anomalies are indicated by negative scores

    return trade_data


def combine_anomalies(trade_data):
    anomalies = []

    if trade_data.get('high_volume_anomaly'):
        anomalies.append('High Volume')
    if trade_data.get('isolation_forest_anomaly'):
        anomalies.append('Isolation Forest Anomaly')

    trade_data['anomalies'] = anomalies if anomalies else None

    return trade_data


if __name__ == "__main__":
    sdf = app.dataframe(input_topic)

    sdf = (sdf
           .apply(high_volume_rule)
           .apply(isolation_forest_rule)
           .apply(combine_anomalies)
           )

    # Filter out only rows where 1 or more anomalies are detected
    sdf = sdf.filter(lambda row: row.get('anomalies') and len(row['anomalies']) >= 1)

    sdf.to_topic(output_topic)
    # elasticsearch
    # postgres
    # streamlit

    app.run(sdf)
