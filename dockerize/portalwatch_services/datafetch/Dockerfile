from portalwatch:latest

ENV SCRIPT datafetch.sh

# Add crontab file in the cron directory
ADD crontab /etc/cron.d/crontab

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/crontab

ADD $SCRIPT $INSTALL_PATH/$SCRIPT
RUN chmod +x $INSTALL_PATH/$SCRIPT
ENTRYPOINT []

CMD ["cron", "-f"]
