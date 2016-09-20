#!/bin/bash

#initially kill running processes
pkill  -f Fetch


week=`date +"%y%V"`
echo $week

echo $LOGS
mkdir -p $LOGS
LOGF=fetch_$week

SCRIPT="python $ADEQUATE/odpw/new/cli.py -c $ADEQUATE/odpw_new.conf Fetch --cores 1"

cmd="$SCRIPT 1>> $LOGS/$LOGF.out 2> $LOGS/$LOGF.err"
echo $cmd
eval $cmd

gzip $LOGS/$LOGF.*
