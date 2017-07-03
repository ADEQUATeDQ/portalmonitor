#!/bin/bash

#initially kill running processes
pkill  -f Fetch

week=`date +"%y%V"`
echo "Writting logs to $LOGS for week $week"

LOGF=data-fetch_$week
SCRIPT="odpw -c $ADEQUATE/portalmonitor.conf DataFetch "
cmd="$SCRIPT 1>> $LOGS/$LOGF.out 2> $LOGS/$LOGF.err"
echo $cmd
eval $cmd
gzip $LOGS/$LOGF.*

LOGF=git-store_$week
SCRIPT="odpw -c $ADEQUATE/portalmonitor.conf GitDataStore"
cmd="$SCRIPT 1>> $LOGS/$LOGF.out 2> $LOGS/$LOGF.err"
echo $cmd
eval $cmd

gzip $LOGS/$LOGF.*
