import batch
from datetime import datetime
import pandas as pd



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

def test_prepare_data():
    df = prepare_data()
    assert len(df) == 6

def test_read_data():
    data = prepare_data()
    df = batch.read_data(data, categorical=['PULocationID', 'DOLocationID'])
    
    assert len(df) == 3




