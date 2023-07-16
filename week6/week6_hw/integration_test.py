import pandas as pd
from datetime import datetime
from batch import read_data, get_input_path, get_output_path

options = {
    'client_kwargs': {
        'endpoint_url': 'http://localhost:4566'
    }
}


def dt(hour, minute, second=0):
    return datetime(2022, 1, 1, hour, minute, second)

def prepare_data():

    data = [
    (None, None, dt(1, 2), dt(1, 10)),
    (1, None, dt(1, 2), dt(1, 10)),
    (1, 2, dt(2, 2), dt(2, 3)),
    (None, 1, dt(1, 2, 0), dt(1, 2, 50)),
    (2, 3, dt(1, 2, 0), dt(1, 2, 59)),
    (3, 4, dt(1, 2, 0), dt(2, 2, 1)),     
    ]

    columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
    df = pd.DataFrame(data, columns=columns)

    return df

def write_data(input_file):
    data = prepare_data()
    df_input = read_data(data, categorical=['PULocationID', 'DOLocationID'])

    df_input.to_parquet(
        input_file,
        engine='pyarrow',
        compression=None,
        index=False,
        storage_options=options
    )


def save_data(filename):
    if isinstance(filename, pd.DataFrame):
        df = filename.copy()
    elif options:
        df = pd.read_parquet(filename, storage_options=options)
    else:
        df = pd.read_parquet(filename)
    return df


if __name__ == '__main__':
    input_file = get_input_path(2022, 1)
    output_file = get_output_path(2022, 1)
    write_data(input_file)
    result = save_data(output_file)
    print(result['predicted_duration'].mean())
    