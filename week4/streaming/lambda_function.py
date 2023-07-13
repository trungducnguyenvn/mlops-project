import json
import boto3
import base64
import os

import mlflow

kinesis_client = boto3.client('kinesis')
PREDICTION_STREAM_NAME = os.getenv('PREDICTIONS_STREAM_NAME', 'ride_prediction')

RUN_ID = os.getenv('RUN_ID')

TEST_RUN = os.getenv('TEST_RUN', 'False') == True

logged_model = f's3://model-actifact-mlflow-practice/3/{RUN_ID}/artifacts/model'
# logged_model = f'runs:/{RUN_ID}/model'
model = mlflow.pyfunc.load_model(logged_model)

def prepare_features(ride):
    features = {}
    features['PU_DO'] = '%s_%s' % (ride['PULocationID'], ride['DOLocationID'])
    features['trip_distance'] = ride['trip_distance']
    return features


def predict(features):
    return 10.0


def lambda_handler(event, context):
    
    predictions = []

    for record in event['Records']:
        encoded_data = record['kinesis']['data']
        decoded_data = base64.b64decode(encoded_data).decode('utf-8')
        ride_event = json.loads(decoded_data)
        ride = ride_event['ride']
        ride_id = ride_event['ride_id']
        
        features = prepare_features(ride)
        prediction = predict(features)
        
        prediction_event = {
            'mode' : 'ride_duration_prediction_model',
            'version': '123',
            'prediction': {
                'ride_duration': prediction,
                'ride_id': ride_id
                }
            }

        if not TEST_RUN:        
            kinesis_client.put_record(
                    StreamName=PREDICTION_STREAM_NAME,
                    Data=json.dumps(prediction_event),
                    PartitionKey=str(ride_id)
                )
        
        predictions.append(prediction_event)
    
    # print(json.dumps(event))


    return {
        'predictions' : predictions
    }
