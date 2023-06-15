import requests
import json

ride = {
    "PULocationID": "10",
    "DOLocationID": "50"
}

url = "http://localhost:9696/predict"

response = requests.post(url, json=ride).json()
print(response)