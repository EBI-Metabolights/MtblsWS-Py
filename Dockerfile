FROM python:3.8-slim-buster
LABEL maintainer="MetaboLights (metabolights-help @ ebi.ac.uk)"

RUN mkdir -p /app-root \
    && mkdir -p /shared-folders \
    && useradd -ms /bin/bash tc_cm01 \
    && chown -R tc_cm01:tc_cm01 /app-root \
    && apt-get -y update \
    && apt-get -y --no-install-recommends install python3-dev python3-pip libpq-dev libglib2.0-0 \
    && apt-get -y --no-install-recommends install build-essential net-tools wget curl iputils-ping traceroute \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get -y autoremove --purge

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY requirements.txt .

RUN apt-get -y update \
    && apt-get -y --no-install-recommends install build-essential \
    && pip3 install -r requirements.txt \
    && apt-get -y remove python3-pip build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get -y autoremove --purge

USER tc_cm01

WORKDIR /app-root
COPY . .

EXPOSE 5000


CMD ["./start_gunicorn_in_docker.sh"]
