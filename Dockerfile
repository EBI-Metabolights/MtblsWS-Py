FROM python:3.8-slim
LABEL maintainer="MetaboLights (metabolights-help @ ebi.ac.uk)"

RUN apt-get -y update \
    && apt-get -y --no-install-recommends install python3 python3-dev python3-pip libpq-dev libglib2.0-0 \
    && apt-get -y --no-install-recommends install net-tools wget curl \
    && mkdir -p /app-root \
    && useradd -ms /bin/bash tc_cm01 \
    && chown -R tc_cm01:tc_cm01 /app-root \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get -y autoremove --purge

COPY requirements.txt .

RUN apt-get -y update \
    && apt-get -y --no-install-recommends install gcc \
    && pip3 install --upgrade --no-cache-dir pip \
    && pip3 install --no-cache-dir -r requirements.txt \
    && apt-get -y remove gcc \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get -y autoremove --purge


ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

USER tc_cm01

WORKDIR /app-root

EXPOSE 5000


CMD ["python3", "wsapp.py"]
