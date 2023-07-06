
```
docker built -t myapp-ride-prediction:v1 .
```

```
docker run -it --rm -p 9696:9696 myapp-ride-prediction:v1
```


## SDK for copy artifact to gcs
```
gsutil cp /path/to/file/on/vm gs://bucket/path/to/file/in/cloud/storage
```