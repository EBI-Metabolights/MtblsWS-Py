ARG CONTAINER_REGISTRY_PREFIX=docker.io/

FROM ${CONTAINER_REGISTRY_PREFIX}astral/uv:0.9-python3.13-trixie-slim AS builder

LABEL maintainer="MetaboLights (metabolights-help @ ebi.ac.uk)"

RUN apt-get -y update \
    && apt-get -y install wget curl zip git vim \
        unrar-free p7zip-full bzip2 pigz pbzip2 \
        zstd rsync openssh-client \
        libglib2.0-0 libsm6 libxrender1 libxext6 libpq-dev \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get -y autoremove --purge

WORKDIR /app-root
COPY . .
RUN uv sync --locked

ARG GROUP1_ID=2222
ARG GROUP2_ID=2223
ARG USER_ID=2222
RUN groupadd group1 -g $GROUP1_ID \
    && groupadd group2 -g $GROUP2_ID \
    && useradd -ms /bin/bash -u $USER_ID -g group1 -G group1,group2 metabolights
USER metabolights
ENV PYTHONPATH=/app-root
ENV PATH=/app-root/.venv/bin:$PATH

EXPOSE 7007

CMD ["./start_datamover_worker.sh"]
