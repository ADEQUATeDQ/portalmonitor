from odpw:latest

RUN git pull; pip install -r requirements.txt; python setup.py install

ENV INSTALL_PATH /adequate-portalharvester
ENV SCRIPT odpwapi.sh

ADD $SCRIPT $INSTALL_PATH/$SCRIPT
RUN chmod +x $INSTALL_PATH/$SCRIPT

ENTRYPOINT []
CMD ["/bin/bash","-c","$INSTALL_PATH/odpwapi.sh"]