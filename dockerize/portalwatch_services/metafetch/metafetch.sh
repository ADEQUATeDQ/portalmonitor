#!/bin/bash

#initially kill running processes
pkill  -f Fetch

week=`date +"%y%V"`
echo "Writting logs to $LOGS for week $week"

LOGF=fetch_$week

SCRIPT="odpw -c $ADEQUATE/odpw.conf Fetch --cores 1"
cmd="$SCRIPT 1>> $LOGS/$LOGF.out 2> $LOGS/$LOGF.err"
echo $cmd
eval $cmd

gzip $LOGS/$LOGF.*
