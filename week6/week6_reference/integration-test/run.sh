#!/usr/bin/env bash

# Change the working directory to the directory containing the script file
cd "$(dirname "$0")"

LOCAL_TAG=`date +"%Y-%m-%d-%H-%M"`
LOCAL_IMAGE_NAME="stream-model-duration:${LOCAL_TAG}"

docker build -t ${LOCAL_IMAGE_NAME} ..

docker compose up -d

sleep 2

python test_docker.py

ERROR_CODE=$?

if [ ${ERROR_CODE} -eq 0 ]; then
    echo "Test passed"
else
    docker compose logs
fi

docker compose down

exit ${ERROR_CODE}
```

