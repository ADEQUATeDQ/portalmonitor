#!/bin/bash

#initially kill running processes
pkill  -f Fetch

week=`date +"%y%V"`
echo "Writting logs to $LOGS for week $week"

LOGF=data-fetch_data-gv-at_$week
SCRIPT="odpw -c $ADEQUATE/portalmonitor.conf DataFetch -p data_gv_at "
cmd="$SCRIPT 1>> $LOGS/$LOGF.out 2> $LOGS/$LOGF.err"
echo $cmd
eval $cmd
gzip $LOGS/$LOGF.*

LOGF=data-fetch_odp_$week
SCRIPT="odpw -c $ADEQUATE/portalmonitor.conf DataFetch -p www_opendataportal_at "
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
