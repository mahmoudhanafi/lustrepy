#! /bin/sh
version=''

# constant setup
#
tmp=/tmp/pm2opentsdb_checker #MY PID
status=0
trap "rm -f \`[ -f $tmp.lock ] && cat $tmp.lock\` $tmp.*; exit \$status" 0 1 2 3 15

prog=`basename $0`
LOCKPROG="/usr/bin/lockfile"

BASEDIR='/u/mhanafi/workspace/lustrepy/src/'
# control file for pmlogger administration ... edit the entries in this
# file to reflect your local configuration
#
CONTROL="$BASEDIR/pm2opentsdb.control"
PM2OPENTSDB="$BASEDIR/pm2opentsdb.py"
# determine real name for localhost
LOCALHOSTNAME=`hostname | sed -e 's/\..*//'`
[ -z "$LOCALHOSTNAME" ] && LOCALHOSTNAME=localhost

# determine path for pwd command to override shell built-in
# (see BugWorks ID #595416).
PWDCMND=/bin/pwd

#
logfile='$BASEDIR/pm2opentsdb.log'


# option parsing
#
SHOWME=false
MV=mv
TERSE=false
VERBOSE=false
VERY_VERBOSE=false
usage="Usage: $prog [-NTV] [-c control]"
while getopts c:NTV? c
do
    case $c
    in
	c)	CONTROL="$OPTARG"
		;;
	N)	SHOWME=true
		MV="echo + mv"
		;;
	T)	TERSE=true
		;;
	V)	if $VERBOSE
		then
		    VERY_VERBOSE=true
		else
		    VERBOSE=true
		fi
		;;
	?)	echo "$usage"
		status=1
		exit
		;;
    esac
done
shift `expr $OPTIND - 1`

if [ $# -ne 0 ]; then
    echo "$usage"
    status=1
    exit
fi

if [ ! -f $CONTROL ]; then
    echo "$prog: Error: cannot find control file ($CONTROL)"
    status=1
    exit
fi

_error()
{
    echo "$prog: [$CONTROL:$line]"
    echo "Error: $1"
    echo "... logging for host \"$host\" unchanged"
    touch $tmp.err
}

_warning()
{
    echo "$prog [$CONTROL:$line]"
    echo "Warning: $1"
}

_message()
{
    case $1
    in
	restart)
	    echo "Restarting$iam pmlogger for host \"$host\" ..."
	    ;;
    esac
}

_log() {
	msg=$1
	echo  $msg >> $logfile
}
_getlock() {
    # demand mutual exclusion
    #
    tries=5   # tenths of a second
	if  $LOCKPROG -1 -r 5 $tmp.lock ; then 
		return 0
	fi
   	_warning "failed to acquire exclusive lock ($dir/lock) ..."
	exit 1
}


_unlock()
{
    rm -f $tmp.lock
}

_check_logger()
{
	pidfile=$1
	if [ -e $pidfile ]; then
		pid=`cat $pidfile`
		runningpid=`pgrep -f $pidfile`
		if [ "$runningpid" == "$pid" ]; then # we are running
			return 0
		fi
	fi
    return 1
}

_start_logger() {
	conf=$1
	log=$2
	pid=$3

	cmd="$PM2OPENTSDB -c $conf -l $log -p $pid --daemon"
	[[ $VERY_VERBOSE ]] || echo "Starting $cmd"
	$cmd
	if [ "$?" != "0"  ]; then
		_error "Failed to start $cmd"	
		return 1
	fi
	return 0
} 
_getlock 

echo >$tmp.dir
rm -f $tmp.err
line=0
cat $CONTROL \
 | while read conffile logfile pidfile
do
    line=`expr $line + 1`
    $VERY_VERBOSE && echo "[control:$line] config=\"$conffile\" logfile=\"$logfile\" pidfile=\"$pidfile\""

    if [ -z "$conffile" -a -z "$logfile" -o -z "$pidfile" ];then
		_error "insufficient fields in control file record"
		continue
    fi

    [[ $VERY_VERBOSE ]] || 	echo "Checking pm2opentsdb -c $conffile -l $logfile -p $pidfile ..."
    
	# Check if running if not start
	if [ -e $pidfile ]; then 
		#See if it really exists
		if  _check_logger $pidfile ;then
			#we are running
			[[ $VERY_VERBOSE ]] || echo "$pidfile is running"
			:
		else
			_start_logger $BASEDIR/$conffile $logfile $pidfile
		fi

	else
		 #See if it really exists
        if  _check_logger $pidfile ;then
            #we are running but missing pid file
			_warning "Running but missing pid file"	
        else
            _start_logger $BASEDIR/$conffile $logfile $pidfile
        fi
	fi

done
_unlock

[ -f $tmp.err ] && status=1
exit

