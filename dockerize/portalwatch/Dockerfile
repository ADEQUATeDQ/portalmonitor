from alpine-python:latest

# This just creates the folder in the Docker image at the
# install path we defined above.

ENV INSTALL_PATH /adequate-portalharvester
ENV LOGS /logs
ENV DATADIR /datadir
RUN mkdir -p $INSTALL_PATH $LOGS $DATADIR

#SSH GIT CLONE
ADD repo-key $INSTALL_PATH/
ADD ssh_config /etc/ssh/ssh_config
RUN chmod 600 $INSTALL_PATH/repo-key
RUN ls -lah $INSTALL_PATH/repo-key
RUN cat /etc/ssh/ssh_config

WORKDIR $INSTALL_PATH
 RUN git clone -b v2 git@git.ai.wu.ac.at:umbrich/ODPortalWatch_2.git odpw
 ENV ADEQUATE $INSTALL_PATH/odpw
 WORKDIR $ADEQUATE

ADD odpw.conf   $ADEQUATE/odpw.conf

RUN git pull; pip install -r requirements.txt; python setup.py install
ONBUILD RUN git pull; pip install -r requirements.txt; python setup.py install

#RUN ls -lah /usr/local/lib/python2.7/site-packages/odpw-0.1-py2.7.egg/odpw
#RUN cp odpw/resources/logging.yaml /usr/local/lib/python2.7/site-packages/odpw-0.1-py2.7.egg/odpw/resources/logging.yaml

#RUN python $ADEQUATE/odpw/cli.py -h

#CRON JOB
#ADD crontab  $INSTALL_PATH/crontab
#RUN crontab $INSTALL_PATH/crontab
#ADD weekly_fetch.sh $INSTALL_PATH/weekly_fetch.sh
#RUN chmod +x $INSTALL_PATH/weekly_fetch.sh
#RUN touch /var/log/cron.log
#ADD start-cron.sh /usr/bin/start-cron.sh



ENTRYPOINT ["/bin/bash", "-c", "odpw -c $ADEQUATE/odpw.conf ${*}", "--"]


#CMD /usr/bin/start-cron.sh
