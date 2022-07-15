FROM 812206152185.dkr.ecr.us-west-2.amazonaws.com/latch-base:9a7d-main

RUN apt-get update -y &&\
    apt-get install -y sickle


COPY wf /root/wf
ARG tag
ENV FLYTE_INTERNAL_IMAGE $tag
RUN  sed -i 's/latch/wf/g' flytekit.config
RUN python3 -m pip install --upgrade latch
WORKDIR /root
