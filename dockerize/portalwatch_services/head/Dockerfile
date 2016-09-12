from odpw:latest


ENV SCRIPT head.sh
#CRON JOB
ADD crontab  $INSTALL_PATH/crontab
RUN crontab $INSTALL_PATH/crontab

ADD $SCRIPT $INSTALL_PATH/$SCRIPT
RUN chmod +x $INSTALL_PATH/$SCRIPT
RUN touch /var/log/cron.log
#ADD start-cron.sh /usr/bin/start-cron.sh
#RUN chmod +x /usr/bin/start-cron.sh
RUN crontab -l
ENTRYPOINT []

CMD ["crond", "-f", "-d", "8"]
