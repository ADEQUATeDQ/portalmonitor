#!/bin/bash

SCRIPT="python api/server.py"
LOGF="odpw_api"
cmd="$SCRIPT 1>> $LOGS/$LOGF.out 2> $LOGS/$LOGF.err"
echo $cmd
eval $cmd
