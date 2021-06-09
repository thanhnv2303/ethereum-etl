# syntax=docker/dockerfile:1

FROM amd64/python

RUN \
 apt-get update \
 && apt-get install -y -q curl gnupg \
 && curl -sSL 'http://p80.pool.sks-keyservers.net/pks/lookup?op=get&search=0x8AA7AF1F1091A5FD' | apt-key add -  \
 && echo 'deb [arch=amd64] http://repo.sawtooth.me/ubuntu/chime/stable bionic universe' >> /etc/apt/sources.list \
 && apt-get update
RUN apt-get update
RUN apt-get install -y python3-pip

WORKDIR /project

COPY ./requirements.txt requirements.txt

RUN pip3 install -r requirements.txt
#COPY .. .

ENV PATH $PATH:/project
#CMD [ "python3" , "./quick_run/build_knowledge_graph.py"]