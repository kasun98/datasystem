from flask import Flask, request, jsonify
import threading
from utils import load_best_model # for prediction
from train_model import retrain # for retraining

# Initialize Flask app
app = Flask(__name__)

# Load the best model at the start of the app
model = load_best_model()

# API endpoint to test GET request
@app.route("/test", methods=["GET"])
def test():
    return jsonify({"status": "ok"}), 200

# API endpoint to test POST request
@app.route("/test_post", methods=["POST"])
def test_post():
    data = request.get_json()
    return jsonify({"received": data}), 200

# API endpoint to make predictions
@app.route("/predict", methods=["POST"])
def predict():
    try:
        # Get data from the request
        data = request.get_json()
        features = data["data"]

        # Convert features to DataFrame or numpy array
        import pandas as pd
        features_df = pd.DataFrame(features, columns=data["columns"])

        # Make prediction
        prediction = model.predict(features_df)
        
        # Return the prediction as JSON
        return jsonify({"prediction": prediction.tolist()})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# Function to reload the model if necessary
@app.route("/reload", methods=["POST"])
def reload_model():
    global model
    model = load_best_model()
    return jsonify({"status": "Model reloaded successfully!"})

# API endpoint to retrain the model
@app.route("/retrain", methods=["POST"])
def retrain_model():
    def background_retrain():
        global model
        result = retrain()
        if result == 1:
            print("Retrain successful, reloading model...")
            model = load_best_model()
        else:
            print("Retrain failed.")

    # Run in background thread to avoid blocking
    threading.Thread(target=background_retrain).start()

    return jsonify({"status": "Model retraining started."}), 200

# Run the app
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5050)
