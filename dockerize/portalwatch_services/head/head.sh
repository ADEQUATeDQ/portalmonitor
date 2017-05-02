#!/bin/bash

#initially kill running processes
pkill  -f Head

week=`date +"%y%V"`
echo "Writting logs to $LOGS for week $week"

LOGF=head_$week

SCRIPT="odpw -c $ADEQUATE/odpw.conf Head"
cmd="$SCRIPT 1>> $LOGS/$LOGF.out 2> $LOGS/$LOGF.err"
echo $cmd
eval $cmd

gzip $LOGS/$LOGF.*
