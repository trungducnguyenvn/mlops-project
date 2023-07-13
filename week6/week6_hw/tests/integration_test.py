import pandas as pd
import test_batch
import batch

df = test_batch.prepare_data()
df_input = batch.read_data(df, categorical=['PULocationID', 'DOLocationID'])


options = {
    'client_kwargs': {
        'endpoint_url': S3_ENDPOINT_URL
    }
}

df_input.to_parquet(
    input_file,
    engine='pyarrow',
    compression=None,
    index=False,
    storage_options=options
)