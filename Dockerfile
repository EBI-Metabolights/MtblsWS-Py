FROM python:3.8-slim-buster as builder
LABEL maintainer="MetaboLights (metabolights-help @ ebi.ac.uk)"

RUN apt-get clean && apt-get -y update && apt-get -y install build-essential python3-dev python3-pip libpq-dev libglib2.0-0 libsm6 libxrender1 libxext6

ENV PYTHONDONTWRITEBYTECODE 1
ENV  PYTHONUNBUFFERED 1
ENV POETRY_NO_INTERACTION 1
ENV POETRY_VIRTUALENVS_IN_PROJECT 1 
ENV POETRY_VIRTUALENVS_CREATE 1
ENV POETRY_CACHE_DIR /tmp/poetry_cache
ENV POETRY_HOME /opt/poetry
WORKDIR /app-root

RUN pip3 install --upgrade pip 
RUN pip3 install poetry==1.6.1
RUN poetry --version

COPY pyproject.toml .
COPY poetry.lock .
RUN touch README.md

RUN poetry install --without dev --no-root && rm -rf $POETRY_CACHE_DIR

FROM python:3.8-slim-buster as runner
LABEL maintainer="MetaboLights (metabolights-help @ ebi.ac.uk)"

RUN apt-get -y update \
    && apt-get -y install wget curl zip git vim unrar-free p7zip-full bzip2 pigz pbzip2 zstd rsync openssh-client libglib2.0-0 libsm6 libxrender1 libxext6 libpq-dev \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get -y autoremove --purge


ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

ENV VIRTUAL_ENV=/app-root/.venv \
    PATH="/app-root/.venv/bin:$PATH"

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}

ARG GROUP1_ID=2222
ARG GROUP2_ID=2223
ARG USER_ID=2222
RUN groupadd group1 -g $GROUP1_ID \
    && groupadd group2 -g $GROUP2_ID \ 
    && useradd -ms /bin/bash -u $USER_ID -g group1 -G group1,group2 metabolights
USER metabolights

WORKDIR /app-root

COPY . .

EXPOSE 7007

CMD ["./start_datamover_worker.sh"]
