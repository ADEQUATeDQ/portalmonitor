from odpw:latest

ADD odpw_ui.sh $INSTALL_PATH/odpw_ui.sh
RUN chmod +x $INSTALL_PATH/odpw_ui.sh

RUN ls $INSTALL_PATH/odpw_ui.sh
EXPOSE 80
WORKDIR $ADEQUATE
RUN git pull; pip install -r requirements.txt; python setup.py install

ENTRYPOINT []
CMD ["/bin/bash","-c","$INSTALL_PATH/odpw_ui.sh"]
