FROM ubuntu:16.04
LABEL maintainer="MetaboLights (metabolights-help @ ebi.ac.uk)"

RUN apt-get -y update \
    && apt-get -y install git \
    && apt-get -y install python3 python3-dev python3-pip \
    && pip3 install --upgrade pip

# make deployment environment
RUN mkdir -p /deployment
RUN cd /deployment \
    && git clone https://github.com/EBI-Metabolights/MtblsWS-Py.git \
    && cd MtblsWS-Py \
    && mkdir -p logs \
    && mkdir instance \
    && cp config.py instance/config.py

WORKDIR /deployment/MtblsWS-Py
RUN pip install -r requirements.txt

# Add mtbls user so we aren't running as root.
RUN useradd -ms /bin/bash tc_cm01
RUN chown -R tc_cm01:tc_cm01 /deployment
USER tc_cm01

EXPOSE 5000
CMD python3 wsapp.py
