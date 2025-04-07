import mlflow
import mlflow.pyfunc
import mlflow.xgboost
import xgboost as xgb
import logging
import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

MLFLOW_ABS_DIR = os.getenv("MLFLOW_ABS_DIR")
mlflow.set_tracking_uri(f"sqlite:///{MLFLOW_ABS_DIR}/mlflow.db")
mlflow.set_experiment("AQI_Forecasting")

def create_xgb_model():
    model = xgb.XGBRegressor(
        objective="reg:squarederror",
        n_estimators=100,
        learning_rate=0.1,
        max_depth=6,
        random_state=42
    )
    logging.info("Created a new XGBoost model instance")
    return model

def load_best_model():
    model = None
    try:
        runs = mlflow.search_runs(order_by=["metrics.MAE ASC"])
        best_run_id = runs.iloc[0]["run_id"]
        
        model_uri = f"runs:/{best_run_id}/model"
        model = mlflow.pyfunc.load_model(model_uri)
        print(f"Loaded best model from Run ID: {best_run_id}")
        return model

    except Exception as e:
        print(f"Error loading best model: {e}")
    
    model = create_xgb_model()
    return model

def rollback_to_version(target_run_id):
    model_uri = f"runs:/{target_run_id}/model"
    model = mlflow.pyfunc.load_model(model_uri)
    print(f"Rolled back to model from Run ID: {target_run_id}")
    return model


def load_best_model_for_retrain():
    try:
        runs = mlflow.search_runs(order_by=["metrics.MAE ASC"])
        best_run_id = runs.iloc[0]["run_id"]
        
        model_uri = f"runs:/{best_run_id}/model"
        model = mlflow.xgboost.load_model(model_uri)

        print(f"Loaded best XGBoost model from Run ID: {best_run_id}")
        return model
    except Exception as e:
        print(f"Error loading best model: {e}")

    model = create_xgb_model()
    return model


# Example: Rollback to an older version (manually find a Run ID from MLflow UI)
# rollback_model = rollback_to_version("1234567890abcdef")


# Connect to the database and fetch data
# This function assumes that the database and tables have already been created
def connect_to_db():
    try:
        DB_NAME = os.getenv("DB_NAME")
        DB_USER = os.getenv("DB_USER")
        DB_PASSWORD = os.getenv("DB_PASSWORD")
        DB_HOST = os.getenv("DB_HOST")
        conn = psycopg2.connect(
                host=DB_HOST,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )

        query = """
            SELECT pm1, pm10, pm25, rh, temp, hour, day_of_week, month, is_weekend,
                temp_x_rh, pm1_x_pm10, pm1_x_pm25, pm10_x_pm25
            FROM air_quality_feature_store
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    
    except Exception as e:
        print(f"Error connecting to database : {e}")
        return None


