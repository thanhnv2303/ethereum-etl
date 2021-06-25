# syntax=docker/dockerfile:1

FROM amd64/python

RUN apt-get update
RUN apt-get install -y python3-pip

WORKDIR /project

COPY ./requirements.txt requirements.txt

RUN pip3 install -r requirements.txt
#COPY .. .

ENV PATH $PATH:/project
#CMD [ "python3" , "./quick_run/build_knowledge_graph.py"]