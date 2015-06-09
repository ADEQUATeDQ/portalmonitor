#!/bin/bash

NUM=0
QUEUE=""
MAX_NPROC=2



#CMD
USAGE="Usage: `basename $0` [-h] [-p nb_processes] [-o output_folder]
	-h      Shows this help
    -p      number of processors to use
    -o      output folder to store portal list, logs, etc..
    "

# parse command line
if [ $# -eq 0 ]; then #  must be at least one arg
    echo "$USAGE" >&2
    exit 1
fi

while getopts p:o:h OPT; do # "j:" waits for an argument "h" doesnt
    case $OPT in
    h)  echo "$USAGE"
        exit 0 ;;
    p)  MAX_NPROC=$OPTARG ;;
    o)  OUT_BASE=$OPTARG ;;
    \?) # getopts issues an error message
        echo "$USAGE" >&2
        exit 1 ;;
    esac
done

#prepare output dir 
mkdir -p $OUT_BASE

#queue pid functions
function queue {
  QUEUE="$QUEUE $1"
  NUM=$(($NUM+1))
}

function regeneratequeue {
	OLDREQUEUE=$QUEUE
	QUEUE=""
	NUM=0
	for PID in $OLDREQUEUE
	do
		if ps -p $PID > /dev/null
		then
			QUEUE="$QUEUE $PID"
			NUM=$(($NUM+1))
		fi
	done
	echo "New Queue $QUEUE, n=$NUM"
}

function checkqueue {
	OLDCHQUEUE=$QUEUE
	#echo "Checking queue ($OLDCHQUEUE)"
	for PID in $OLDCHQUEUE
	do
		if ! ps -p $PID > /dev/null
			then
				echo "Process $PID is done"
				regeneratequeue # at least one PID has finished
				break
		fi
	done
}

#-----------
# Main program

yw=`date +"%Y-%V"`
OUT_DIR=$OUT_BASE/$yw
mkdir -p $OUT_DIR
echo "
#######################
#    ODPW: FETCH
# snapshot: $yw
# processors: $MAX_NPROC
# out_dir: $OUT_DIR
#######################"

PLIST=$OUT_DIR/portals.txt
PIDLIST=$OUT_DIR/pids.txt
echo "#Fetching list of portals -> $PLIST"
CMD="/usr/bin/local/odpw .... -o $PLIST"
echo ">$CMD"
$CMD

echo "#Processing portals"
CMD_FETCH="sleep 10"
while read line
do
    echo ">$CMD_FETCH"
    $CMD_FETCH &
    # DEFINE COMMAND END
 
    PID=$!
    queue $PID
    echo "$PID $line" >> $PIDLIST
    echo "#Q:$QUEUE, N=$NUM"
    while [ $NUM -ge $MAX_NPROC ]; do
        checkqueue
        sleep 1
    done
done < $PLIST
wait # wait for all processes to finish before exit