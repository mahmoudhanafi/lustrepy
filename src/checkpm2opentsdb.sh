#!/bin/bash
#
#-- GPL HEADER START --
#Send pcp to graphite colletor daemon.
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
#
#-- GPL HEADER END --

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

	

