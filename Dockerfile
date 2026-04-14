ARG CONTAINER_REGISTRY_PREFIX=docker.io/

FROM ${CONTAINER_REGISTRY_PREFIX}astral/uv:python3.13-trixie AS builder

LABEL maintainer="MetaboLights (metabolights-help @ ebi.ac.uk)"

RUN apt-get -y update \
    && apt-get -y install wget curl zip git vim \
        unrar-free p7zip-full bzip2 pigz pbzip2 \
        zstd rsync openssh-client \
        libglib2.0-0 libsm6 libxrender1 libxext6 libpq-dev \
        build-essential python3-dev pkg-config libssl-dev libffi-dev \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get -y autoremove --purge

WORKDIR /app-root
ARG GROUP1_ID=2222
ARG GROUP2_ID=2223
ARG USER_ID=2222
RUN groupadd group1 -g $GROUP1_ID \
    && groupadd group2 -g $GROUP2_ID \
    && useradd -ms /bin/bash -u $USER_ID -g group1 -G group1,group2 metabolights
ENV PYTHONPATH=/app-root
ENV PATH=/app-root/.venv/bin:$PATH
ENV UV_LOCKED=1
EXPOSE 7007
COPY uv.lock uv.lock
COPY README.md README.md
COPY pyproject.toml pyproject.toml
RUN uv sync
COPY . .
USER metabolights
CMD ["./start_datamover_worker.sh"]
