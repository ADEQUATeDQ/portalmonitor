from portalwatch:latest

ENV SCRIPT metafetch.sh

# Add crontab file in the cron directory
ADD crontab /etc/cron.d/crontab

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/crontab

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

# Run the command on container startup
CMD cron && tail -f /var/log/cron.log

ADD $SCRIPT $INSTALL_PATH/$SCRIPT
RUN chmod +x $INSTALL_PATH/$SCRIPT
RUN touch /var/log/cron.log
#ADD start-cron.sh /usr/bin/start-cron.sh
#RUN chmod +x /usr/bin/start-cron.sh
RUN crontab -l
ENTRYPOINT []

CMD ["cron", "-f"]
