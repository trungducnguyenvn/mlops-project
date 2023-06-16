import requests
import json

ride = {
    "PULocationID": "236",
    "DOLocationID": "68"
}

url = "http://localhost:9696/predict"

response = requests.post(url, json=ride).json()
print(response)