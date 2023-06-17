FROM svizor/zoomcamp-model:mlops-3.10.0-slim

RUN pip install -U pip
RUN pip install pandas pyarrow numpy scikit-learn==1.2.2

WORKDIR /app

COPY [ "score.py" , "./"]

ENTRYPOINT [ "bash" ]


