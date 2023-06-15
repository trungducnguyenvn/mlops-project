import pickle
from flask import Flask, request, jsonify


# load the model from models directory
with open("../models/lin_reg.bin", "rb") as f_in:
    dv, model = pickle.load(f_in)

# print(f"Model : {model}")
# print(f"dv : {dv}")


app = Flask("taxi-duration")


def predict(features):
    X = dv.transform(features)
    preds = model.predict(X)
    return preds[0]


@app.route("/predict", methods=["POST"])
def predict_endpoint():
    ride = request.get_json()
    prediction = predict(ride)
    
    result = {
        "predict_duration": prediction
    }

    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=9696)































