

```bash
{
    "ride": {
        "PULocationID": 130,
        "DOLocationID": 205,
        "trip_distance": 3.66
    }, 
    "ride_id": 123
}
```


## Sending data
```
KINESIS_STREAM_INPUT=ride_event
aws kinesis put-record \
    --stream-name ${KINESIS_STREAM_INPUT} \
    --partition-key 1 \
    --data "Hello, this is a test."
```

## Decode 64base
```
{
    "Records": [
        {
            "kinesis": {
                "kinesisSchemaVersion": "1.0",
                "partitionKey": "1",
                "sequenceNumber": "49642566955939549797605850126197137789254456159403769858",
                "data": "Hellothisisatest",
                "approximateArrivalTimestamp": 1689153116.404
            },
            "eventSource": "aws:kinesis",
            "eventVersion": "1.0",
            "eventID": "shardId-000000000000:49642566955939549797605850126197137789254456159403769858",
            "eventName": "aws:kinesis:record",
            "invokeIdentityArn": "arn:aws:iam::255376378034:role/lambda-kinesis-role",
            "awsRegion": "ap-southeast-1",
            "eventSourceARN": "arn:aws:kinesis:ap-southeast-1:255376378034:stream/ride_event"
        }
    ]
}

```
```
base64.b64decode(data_encoded).decode('utf-8')
```


- Next time : try to send json data with triple quotes 
```
aws kinesis put-record \
    --stream-name ${KINESIS_STREAM_INPUT} \
    --partition-key 1 \
    --data '{
        "ride": {
            "PULocationID": 130,
            "DOLocationID": 205,
            "trip_distance": 3.66
        }, 
        "ride_id": 156
    }'
```

- encoded data 
```
aws kinesis put-record \
    --stream-name ${KINESIS_STREAM_INPUT} \
    --partition-key 1 \
    --data b'eyJyaWRlIjp7IlBVTG9jYXRpb25JRCI6MTMwLCJET0xvY2F0aW9uSUQiOjIwNSwidHJpcF9kaXN0YW5jZSI6M30sInJpZGVfaWQiOjE1Nn0='
```

### Read from the stream
```
KINESIS_STREAM_OUTPUT='ride_prediction'
SHARD='shardId-000000000000'

SHARD_ITERATOR=$(aws kinesis \
    get-shard-iterator \
        --shard-id ${SHARD} \
        --shard-iterator-type TRIM_HORIZON \
        --stream-name ${KINESIS_STREAM_OUTPUT} \
        --query 'ShardIterator' \
)

RESULT=$(aws kinesis get-records --shard-iterator $SHARD_ITERATOR)

echo ${RESULT} | jq -r '.Records[0].Data' | base64 --decode
```


```bash
export PREDICTIONS_STREAM_NAME="ride_prediction"
export RUN_ID="3f91d776529b4f55861a701c4a6363a4"
export TEST_RUN="True"

python test.py
```


## Running in Docker 
```bash
sudo docker build -t stream-model-duration:v1 .

sudo docker run -it --rm \
    -p 8080:8080 \
    -e PREDICTIONS_STREAM_NAME="ride_prediction" \
    -e RUN_ID="3f91d776529b4f55861a701c4a6363a4" \
    -e TEST_RUN="True" \
    -e AWS_DEFAULT_REGION="ap-southeast-1" \
    stream-model-duration:v1
```

URL for testing:
- http://localhost:8080/2015-03-31/functions/function/invocations


## Publishing to ECR 
```bash
aws ecr create-repository --repository-name model-duration-streaming
```

## Authenticate Docker to ECR
```
$(aws ecr get-login --no-include-email)
```


```
REMOTE_URI="255376378034.dkr.ecr.ap-southeast-1.amazonaws.com/model-duration-streaming"
REMOTE_TAG="v1"
REMOTE_IMAGE=${REMOTE_URI}:${REMOTE_TAG}

LOCAL_IMAGE="stream-model-duration:v1"
docker tag ${LOCAL_IMAGE} ${REMOTE_IMAGE}
docker push ${REMOTE_IMAGE}
```