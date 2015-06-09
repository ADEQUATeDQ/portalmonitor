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
yw=`date +"%Y-%V"`
OUT_DIR=$OUT_BASE/$yw
mkdir -p $OUT_DIR
PLIST=$OUT_DIR/portals.txt
PIDLIST=$OUT_DIR/pids.txt


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
	echo "#Q:$QUEUE, N=$NUM after regeneratequeue"
}

function checkqueue {
	OLDCHQUEUE=$QUEUE
	for PID in $OLDCHQUEUE
	do
		if ! ps -p $PID > /dev/null
			then
				
                time=`date +%Y-%m-%d:%H:%M:%S`
                echo "#Process $PID is done"
                sed -ie "s/^$PID.*$/& $time/g" $PIDLIST
                echo $c
                $c
                
				regeneratequeue # at least one PID has finished
				break
		fi
	done
}

function checkQueue {
    OLDREQUEUE=$QUEUE
	QUEUE=""
    echo "pids in queue $NUM"
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
            sed -ie "s/^$PID.*$/& $time/g" $PIDLIST
        fi 
    done
    echo "active pids in queue $NUM"
    
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
CMD="/usr/local/bin/odpw --host bandersnatch.ai.wu.ac.at Fetch -p -i -o $PLIST"
echo ">$CMD"
$CMD

echo "#Processing portals"
CMD_FETCH="sleep 1"
while read line
do
    echo ">$CMD_FETCH $line"
    $CMD_FETCH &
    # DEFINE COMMAND END
 
    PID=$!
    queue $PID
    
    time=`date +%Y-%m-%d:%H:%M:%S`
    
    echo "$PID $line $time" >> $PIDLIST
    echo "#Q:$QUEUE, N=$NUM"
    while [ $NUM -ge $MAX_NPROC ]; do
        checkQueue
        sleep 1
    done
done < $PLIST

wait # wait for all processes to finish before exit

echo "#all portals are processed"
checkQueue
for i in "${!pMap[@]}"
do
  echo "key  : $i"
  echo "value: ${pMap[$i]}"
done