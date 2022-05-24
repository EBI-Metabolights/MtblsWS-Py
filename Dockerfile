FROM python:3.8-slim
LABEL maintainer="MetaboLights (metabolights-help @ ebi.ac.uk)"

RUN mkdir -p /app-root \
    && mkdir -p /shared-folders \
    && useradd -ms /bin/bash tc_cm01 \
    && chown -R tc_cm01:tc_cm01 /app-root

RUN apt-get -y update \
    && apt-get -y --no-install-recommends install python3 python3-dev python3-pip libpq-dev libglib2.0-0 \
    && apt-get -y --no-install-recommends install build-essential net-tools wget curl iputils-ping traceroute \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get -y autoremove --purge

COPY requirements.txt .

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN  pip3 install --upgrade pip \
    && pip3 install -r requirements.txt

USER tc_cm01

WORKDIR /app-root

EXPOSE 5000


CMD ["python3", "wsapp.py"]
