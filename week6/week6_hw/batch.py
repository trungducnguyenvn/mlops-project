#!/usr/bin/env python

import sys
import pickle
import pandas as pd



def main(year, month):
    categorical = ['PULocationID', 'DOLocationID']

    input_file = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    output_file = f'./yellow_tripdata_{year:04d}-{month:02d}.parquet'

    df = read_data(input_file, categorical=categorical)
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')

    with open('model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)

    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)

    print('predicted mean duration:', y_pred.mean())

    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred


    df_result.to_parquet(output_file, engine='pyarrow', index=False)


def read_data(filename, categorical):
    # check if filename is dataframe or parquet file
    if isinstance(filename, pd.DataFrame):
        df = filename.copy()
    else:
        df = pd.read_parquet(filename)

    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df



if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: batch.py year month')
        sys.exit(1)

    year = int(sys.argv[1])
    month = int(sys.argv[2])

    main(year, month)



