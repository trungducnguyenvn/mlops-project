import pandas as pd
import pickle
from pathlib import Path
import argparse

parser = argparse.ArgumentParser(description='Predict taxi trip duration')
parser.add_argument('year', type=int, help='year for prediction (e.g 2022)')
parser.add_argument('month', type=int, help='month for prediction (e.g 3)')
args = parser.parse_args()

year = args.year
month = args.month
taxi_type = 'yellow'

input_data = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month:02}.parquet'
output_dir = Path('output')
output_dir.mkdir(parents=True, exist_ok=True)
output_data = output_dir / f'{taxi_type}_tripdata_{year}-{month:02}.parquet'


with open('model.bin', 'rb') as f_in:
    dv, model = pickle.load(f_in)

categorical = ['PULocationID', 'DOLocationID']

def read_data(filename):
    df = pd.read_parquet(filename)
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')

    #Create an artificial ride_id
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    
    return df


def apply_model(input_file, model, output_file):
    df = read_data(input_file)
    
    X = dv.transform(df[categorical].to_dict(orient='records'))
    y_pred = model.predict(X)

    df_result = pd.DataFrame()
    df_result['ride_id'] = df.ride_id
    df_result['prediction_duration'] = y_pred

    # Save the result
    df_result.to_parquet(
        output_file,
        engine='pyarrow',
        compression=None,
        index=False
    )

    return df_result

if __name__ == "__main__":
    apply_model(input_data, model, output_data)
