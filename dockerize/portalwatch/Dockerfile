from odpw_python:latest

ENV INSTALL_PATH /adequate-portalmonitor
ENV LOGS /logs
ENV DATADIR /datadir
RUN mkdir -p $INSTALL_PATH $LOGS $DATADIR

WORKDIR $INSTALL_PATH
 RUN git clone https://github.com/ADEQUATeDQ/portalmonitor.git portalmonitor
 ENV ADEQUATE $INSTALL_PATH/portalmonitor
 WORKDIR $ADEQUATE

ADD portalmonitor.conf   $ADEQUATE/odpw.conf
ENV CONF $ADEQUATE/odpw.conf
RUN git pull; pip install -r requirements.txt; python setup.py install
ONBUILD RUN git pull; pip install -r requirements.txt; python setup.py install


ENTRYPOINT ["/bin/bash", "-c", "odpw -c $ADEQUATE/odpw.conf ${*}", "--"]


#CMD /usr/bin/start-cron.sh
