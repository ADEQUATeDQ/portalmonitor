from portalwatch:latest

ENV SCRIPT datafetch.sh
ENV GIT_SCRIPT gitpusher.sh

# Add crontab file in the cron directory
ADD crontab /etc/cron.d/crontab

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/crontab

ADD $GIT_SCRIPT $INSTALL_PATH/$GIT_SCRIPT
RUN chmod +x $INSTALL_PATH/$GIT_SCRIPT

ADD $SCRIPT $INSTALL_PATH/$SCRIPT
RUN chmod +x $INSTALL_PATH/$SCRIPT
ENTRYPOINT []

CMD ["cron", "-f"]
