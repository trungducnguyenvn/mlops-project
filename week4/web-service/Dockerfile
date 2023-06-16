FROM python:3.10.0-slim

RUN pip install -U pip
RUN pip install pipenv
RUN pipenv install gunicorn flask numpy scikit-learn==1.2.2

WORKDIR /app

RUN pipenv install --system --deploy --ignore-pipfile

COPY [ "predict.py", "model.bin", "./" ]

EXPOSE 9696

ENTRYPOINT [ "gunicorn", "--bind=0.0.0.0:9696", "predict:app" ]

