FROM ubuntu:latest

RUN apt-get -y update && apt-get -y upgrade
RUN apt-get -y install git
RUN apt-get -y install python3 python3-dev python3-pip
RUN pip3 install --upgrade pip

# make deployment environment
RUN mkdir -p /deployment
RUN cd /deployment \
    && git clone https://github.com/EBI-Metabolights/MtblsWS-Py.git \
    && cd MtblsWS-Py \
    && mkdir -p logs
WORKDIR /deployment/MtblsWS-Py
RUN pip3 install -r requirements.txt

# Add mtbls user so we aren't running as root.
RUN useradd -ms /bin/bash tc_cm01
RUN chown -R tc_cm01:tc_cm01 /deployment
USER tc_cm01

EXPOSE 5000
