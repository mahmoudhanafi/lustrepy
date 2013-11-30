#!/bin/bash
#
FILESYSTEMS="nbp7 nbp8"
BIN=/u/mhanafi/workspace/lustrepy/src/pm2opentsdb.py
CONFDIR=/u/mhanafi/workspace/lustrepy/src


start() {
	FS=$1
	PIDFILE="/tmp/${FS}.pid
	CONFIG=$CONDIR/$FS.conf
	OUTDIR=/tmp/$FS.out
	$BIN -c $CONFIG -l $OUTDIR --daemon -p $PID
}

kill() {
	PID=$1
	kill $PID
}

for FS in $FILESYSTEMS; do
	PIDFILE="/tmp/${FS}.pid
	PID=`cat $pidfile`
	RUNNINGPID=`pgrep -f $FS.conf`

	if [ -z "$RUNNINGPID" ]; then # Var is empty
		start $FS
	fi	
	if [ $PID != $RUNNINGPID ]; then
		echo "Running pid for $i is not same as in pid file"
		kill $PID
		start $FS
	fi
done

	

