#!/bin/bash

#initially kill running processes
pkill  -f ODPWUI

SCRIPT="odpw -c $ADEQUATE/odpw.conf ODPWUI"
LOGF="odpw_ui"
cmd="$SCRIPT 1>> $LOGS/$LOGF.out 2> $LOGS/$LOGF.err"
echo $cmd
eval $cmd
