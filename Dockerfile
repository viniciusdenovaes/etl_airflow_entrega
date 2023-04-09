FROM apache/airflow:2.5.3
ADD requirements.txt .

USER root

# install app dependencies
RUN apt-get update && apt-get install -y python3 python3-pip

USER 50000
RUN pip install -r requirements.txt

USER root

RUN  apt-get update \
  && apt-get install -y wget

RUN umask 0002; \
    mkdir -p ~/tmp

RUN apt-get install -y libreoffice

# Install OpenJDK-11
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean;


USER 50000
