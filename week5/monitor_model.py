import datetime
import time
import random
import logging 
import pandas as pd
import psycopg
import joblib
import numpy as np

from prefect import task, flow

from evidently.report import Report
from evidently import ColumnMapping
from evidently.metrics import ColumnDriftMetric, RegressionQualityMetric

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")

SEND_TIMEOUT = 10
rand = random.Random()

create_table_statement = """
drop table if exists model_metrics;
create table model_metrics(
	timestamp timestamp,
	modelquality float
)
"""

reference_data = pd.read_parquet('./data/reference_data.parquet')
with open('./models/lin_reg.bin', 'rb') as f_in:
	model = joblib.load(f_in)

raw_data = pd.read_parquet('./data/green_tripdata_2023-01.parquet')

begin = datetime.datetime(2023, 1, 1, 0, 0)
num_features = ['passenger_count', 'trip_distance', 'fare_amount', 'total_amount']
cat_features = ['PULocationID', 'DOLocationID']
column_mapping = ColumnMapping(
    target='duration',
    prediction='prediction',
    numerical_features=num_features,
    categorical_features=cat_features
)

model_report = Report(metrics=[RegressionQualityMetric()])




@task
def prep_db():
	with psycopg.connect("host=localhost port=5432 user=postgres password=example", autocommit=True) as conn:
		res = conn.execute("SELECT 1 FROM pg_database WHERE datname='test'")
		if len(res.fetchall()) == 0:
			conn.execute("create database test;")
		with psycopg.connect("host=localhost port=5432 dbname=test user=postgres password=example") as conn:
			conn.execute(create_table_statement)


@task(retries=3, retry_delay_seconds=5, name="calculate metrics")
def calculate_metrics_postgresql(curr, i):
	current_data = raw_data[(raw_data.lpep_pickup_datetime >= (begin + datetime.timedelta(i))) &
		(raw_data.lpep_pickup_datetime < (begin + datetime.timedelta(i + 1)))]
	
	current_data["duration"] = current_data.lpep_dropoff_datetime - current_data.lpep_pickup_datetime
	current_data["duration"] = current_data["duration"].apply(lambda x: x.total_seconds() / 60)

	#current_data.fillna(0, inplace=True)
	current_data['prediction'] = model.predict(current_data[num_features + cat_features].fillna(0))

	model_report.run(reference_data = reference_data, current_data = current_data,
		column_mapping=column_mapping)

	model_result = model_report.as_dict()
	modelquality = model_result['metrics'][0]['result']['current']['mean_abs_error']



	curr.execute(
		"insert into model_metrics(timestamp, modelquality) values ( %s, %s)",
		(begin + datetime.timedelta(i) , modelquality)
	)


@flow
def batch_monitoring_backfill():
	prep_db()
	last_send = datetime.datetime.now() - datetime.timedelta(seconds=10)
	with psycopg.connect("host=localhost port=5432 dbname=test user=postgres password=example", autocommit=True) as conn:
		for i in range(0, 31):
			with conn.cursor() as curr:
				calculate_metrics_postgresql(curr, i)

			new_send = datetime.datetime.now()
			seconds_elapsed = (new_send - last_send).total_seconds()
			if seconds_elapsed < SEND_TIMEOUT:
				time.sleep(SEND_TIMEOUT - seconds_elapsed)
			while last_send < new_send:
				last_send = last_send + datetime.timedelta(seconds=10)
			logging.info("data sent")


if __name__ == '__main__':
	batch_monitoring_backfill()