#!/bin/bash

NUM=0
QUEUE=""
MAX_NPROC=2
HOST="localhost"

declare -A map


#CMD
USAGE="Usage: `basename $0` [-h db host] [-p nb_processes] [-o output_folder]
	-?      Shows this help
    -p      number of processors to use
    -o      output folder to store portal list, logs, etc..
    -h      host of the database
    "

# parse command line
if [ $# -eq 0 ]; then #  must be at least one arg
    echo "$USAGE" >&2
    exit 1
fi

while getopts p:o:h:? OPT; do # "j:" waits for an argument "h" doesnt
    case $OPT in
    p)  MAX_NPROC=$OPTARG ;;
    h)  HOST=$OPTARG ;;
    o)  OUT_BASE=$OPTARG ;;
    \?) # getopts issues an error message
        echo "$USAGE" >&2
        exit 1 ;;
    esac
done

#prepare output dir 
mkdir -p $OUT_BASE
yw=`date +"%Y-%V"`
OUT_DIR=$OUT_BASE/$yw
mkdir -p $OUT_DIR
LOG=$OUT_DIR/log
mkdir -p $LOG

PLIST=$OUT_DIR/portals.txt
PIDLIST=$OUT_DIR/pids.txt



#queue pid functions
function queue {
  QUEUE="$QUEUE $1"
  NUM=$(($NUM+1))
}

function checkQueue {
    OLDREQUEUE=$QUEUE
    QUEUE=""
    cur=$NUM
    NUM=0
    for PID in $OLDREQUEUE
    do
       if ps -p $PID > /dev/null
	then
			QUEUE="$QUEUE $PID"
			NUM=$(($NUM+1))
	else
		time=`date +%Y-%m-%d:%H:%M:%S`
		echo "#Process $PID is done"
		echo "${map[$PID]}"
		text=${map[$PID]}
		cmd="sed -ie 's/$text.*$/& $time/g' $PIDLIST"
		echo $cmd
		eval $cmd
	fi
    done
}

#-----------
# Main program



echo "
#######################
#    ODPW: FETCH
# snapshot: $yw
# processors: $MAX_NPROC
# out_dir: $OUT_DIR
#######################"

echo "#Fetching list of portals -> $PLIST"
CMD="/usr/local/bin/odpw --host $HOST Fetch -p -sn $yw -o $PLIST"
echo ">$CMD"
$CMD

echo "#Processing portals"

while read line
do
    tokens=($line)
    CMD_FETCH="/usr/local/bin/odpw --host $HOST Fetch -sn $yw --force -u ${tokens[0]} 1> $LOG/${tokens[1]}.out 2> $LOG/${tokens[1]}.err"
    #CMD_FETCH="sleep $[ ( $RANDOM % 10 )  + 1 ]s"
    echo ">$CMD_FETCH"
    eval $CMD_FETCH &
    # DEFINE COMMAND END
    
    PID=$!
    queue $PID
    map[$PID]=${tokens[1]}
    
    time=`date +%Y-%m-%d:%H:%M:%S`

    echo "$PID $line $time" >> $PIDLIST
    echo "#Q:$QUEUE, N=$NUM"
    while [ $NUM -ge $MAX_NPROC ]; do
        checkQueue
        sleep 1
    done
done < $PLIST

wait # wait for all processes to finish before exit
checkQueue
echo "#all portals are processed"
