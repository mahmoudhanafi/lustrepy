#! /bin/sh
# $Id$
#-- GPL HEADER START --
#Send pcp to graphite daemon checker
#Copyright (c) 2013 NASA 
#Author: Mahmoud Hanafi (mahmoud.hanafi@nasa.gov)
#
#This program is free software; you can redistribute it and/or
#modify it under the terms of the GNU General Public License
#as published by the Free Software Foundation; either version 2
#of the License, or (at your option) any later version.
#
#This program is distributed in the hope that it will be useful,
#but WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#GNU General Public License for more details.
#
#You should have received a copy of the GNU General Public License
#along with this program; if not, write to the Free Software
#Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
##
#-- GPL HEADER END --

version=''

# constant setup
#
tmp=/tmp/pm2opentsdb_checker #MY PID
status=0
trap "rm -f \`[ -f $tmp.lock ] && cat $tmp.lock\` $tmp.*; exit \$status" 0 1 2 3 15

prog=`basename $0`
LOCKPROG="/usr/bin/lockfile"

BASEDIR='/usr/local/graphite'
# control file for pmlogger administration ... edit the entries in this
# file to reflect your local configuration
#
CONFDIR="$BASEDIR/conf"
BINDIR="$BASEDIR/bin"
PM2GRAPHITE="$BASEDIR/pm2graphite.py"
PM2GRAPHITECONF="$CONFDIR/pm2graphite.conf"
PM2PMDACONF="$CONFDIR/pmlist.conf"
GRAPHITESERVER="service370"
GRAPHITEPORT="2003"

DAEMON="--damon"


# determine real name for localhost
LOCALHOSTNAME=`hostname | sed -e 's/\..*//'`
[ -z "$LOCALHOSTNAME" ] && LOCALHOSTNAME=localhost

# determine path for pwd command to override shell built-in
# (see BugWorks ID #595416).
PWDCMND=/bin/pwd

#
LOGFILE="/tmp/pm2graphite.log"
PIDFILE="/tmp/pm2graphite.pid"



_start() {
	$PM2GRAPHITE --conf=$PM2GRAPHITECONF --pmdaconf=$PM2PMDACONF -s $GRAPHITESERVER $DAEMON --pid=$PIDFILE
}

_kill() {
	PID=$1
	kill $PID
}



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
	echo $1 
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

_getlock 

if _check_logger $PIDFILE; then

echo >$tmp.dir
rm -f $tmp.err
line=0
    line=`expr $line + 1`

	MYHOSTNAME=`uname -n`
	#exit if hostname is not in config file
	[[ `grep $MYHOSTNAME $PM2GRAPHITECONF` ]] || exit 
    [[ $VERY_VERBOSE ]] || 	echo "Checking pm2graphite ..."
    
	# Check if running if not start
	if [ -e $PIDFILE ]; then 
		#See if it really exists
		if  _check_logger $PIDFILE ;then
			#we are running
			[[ $VERY_VERBOSE ]] || echo "$PIDFILE is running"
			:
		else
			_start 
		fi

	else
		 #See if it really exists
        if  _check_logger $PIDFILE ;then
            #we are running but missing pid file
			_warning "Running but missing pid file"	
        else
            _start
        fi
	fi

_unlock

[ -f $tmp.err ] && status=1
exit

