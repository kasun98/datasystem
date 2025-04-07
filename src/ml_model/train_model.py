from utils import load_best_model_for_retrain, connect_to_db
import xgboost as xgb
import os
import mlflow.xgboost
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import requests
import logging


forecast_horizon = 6
feature_cols = [
    'pm1', 'pm10', 'pm25', 'rh', 'temp', 'hour', 'day_of_week',
    'month', 'is_weekend', 'temp_x_rh', 'pm1_x_pm10', 'pm1_x_pm25',
    'pm10_x_pm25']

def retrain():
    try:
        # Load the best model from MLflow
        best_model = load_best_model_for_retrain()
        logging.info("Loaded the best model from model registry")
    except Exception as e:
        logging.error(f"Error loading model: {e}")
        return 0

    try:
        # Connect to the database and fetch data
        df = connect_to_db()
        logging.info("Connected to Postgres DB and fetch the data")

    except Exception as e:
        logging.error(f"Error connecting to database and fetch data as pandas dataframe: {e}")
        return 0

    if df is not None:
        try:
            # Convert boolean columns to integers
            df['is_weekend'] = df['is_weekend'].astype(int)
            # Create future target columns
            for i in range(1, forecast_horizon + 1):
                df[f"pm25_t+{i}"] = df["pm25"].shift(-i)

            # Drop rows with NaN (caused by shifting forward)
            df.dropna(inplace=True)

            # target columns
            target_cols = [f"pm25_t+{i}" for i in range(1, forecast_horizon + 1)]

            # Split data (80% train, 20% test)
            train_size = int(len(df) * 0.8)
            train, test = df.iloc[:train_size], df.iloc[train_size:]

            X_train, y_train = train[feature_cols], train[target_cols]
            X_test, y_test = test[feature_cols], test[target_cols]

            # Retrain the best model
            best_model.fit(X_train, y_train)

            # Predict on test set
            y_pred = best_model.predict(X_test)

            # Evaluate the model
            mae = mean_absolute_error(y_test, y_pred, multioutput="raw_values")
            mse = mean_squared_error(y_test, y_pred, multioutput="raw_values")
            r2 = r2_score(y_test, y_pred, multioutput="raw_values")

            # Log model to MLflow
            with mlflow.start_run():
                mlflow.log_param("model_type", "XGBoost")

                for i in range(forecast_horizon):
                    mlflow.log_metric(f"mae_t_{i+1}", mae[i])
                    mlflow.log_metric(f"mse_t_{i+1}", mse[i])
                    mlflow.log_metric(f"r2_t_{i+1}", r2[i])

                # Log the model
                mlflow.xgboost.log_model(best_model, "model")

            logging.info("XGBoost model retrained.") 
            return 1
            
        
        except Exception as e:
            logging.error(f"Model retraining aborted due to {e}")
            return 0

    else:
        logging.error("Model or data is None. Cannot proceed with training.")
        return 0
