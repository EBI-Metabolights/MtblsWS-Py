FROM python:3.8-slim-buster as compiler
LABEL maintainer="MetaboLights (metabolights-help @ ebi.ac.uk)"

RUN apt-get clean && apt-get -y update
RUN apt-get -y install build-essential python3-dev python3-pip libpq-dev libglib2.0-0 libsm6 libxrender1 libxext6 \
    && pip3 install --upgrade pip \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get -y autoremove --purge

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY requirements.txt .

RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN pip3 install -r requirements.txt



FROM python:3.8-slim-buster as runner
RUN apt-get -y update \
    && apt-get -y install wget curl zip git p7zip-full bzip2 pigz pbzip2 zstd rsync openssh-client libglib2.0-0 libsm6 libxrender1 libxext6 \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get -y autoremove --purge

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY --from=compiler /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

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
