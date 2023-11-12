FROM python:3.8

RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean;

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
