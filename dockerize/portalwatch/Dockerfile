FROM ubuntu:latest

RUN apt-get update && apt-get install -y \
    software-properties-common
RUN add-apt-repository universe
RUN apt-get update && apt-get install -y \
    curl \
    git \
    python-pip \
    libpq-dev \
    python-dev \
    python-tk \
    cron

RUN pip install flask
RUN pip install flask-cache
RUN pip install git+git://github.com/sebneu/anycsv.git
RUN pip install git+git://github.com/ODInfoBiz/pyyacp.git

ENV INSTALL_PATH /adequate-portalharvester
ENV LOGS /logs
ENV DATADIR /datadir
RUN mkdir -p $INSTALL_PATH $LOGS $DATADIR

#SSH GIT CLONE
ADD id_rsa $INSTALL_PATH/
ADD ssh_config /etc/ssh/ssh_config
RUN chmod 600 $INSTALL_PATH/id_rsa
RUN ls -lah $INSTALL_PATH/id_rsa
RUN cat /etc/ssh/ssh_config

WORKDIR $INSTALL_PATH
 RUN git clone https://github.com/ADEQUATeDQ/portalmonitor.git portalmonitor
 ENV ADEQUATE $INSTALL_PATH/portalmonitor
 WORKDIR $ADEQUATE

ADD portalmonitor.conf   $ADEQUATE/portalmonitor.conf

RUN echo "update"
RUN git pull; pip install -r requirements.txt; python setup.py install
ONBUILD RUN git pull; pip install -r requirements.txt; python setup.py install


ENTRYPOINT ["/bin/bash", "-c", "odpw -c $ADEQUATE/portalmonitor.conf ${*}", "--"]
