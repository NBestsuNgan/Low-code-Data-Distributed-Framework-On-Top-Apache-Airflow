# FROM jupyter/pyspark-notebook:latest
FROM tabulario/spark-iceberg:latest

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt