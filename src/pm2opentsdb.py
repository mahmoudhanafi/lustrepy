#!/usr/bin/env python
# $Source: /cvsroot/lustrepy/src/pm2opentsdb.py,v $
'''
Created on Jul 5, 2013

@author: mhanafi
'''

__author__ = "Mahmoud Hanafi"
__copyright__ = "Copyright (C) 2013 Mahmoud Hanafi NASA AMES"

__revision__ = "$Id$"
__version__ = "0.0"

import re
import sys
import subprocess
from optparse import OptionParser
import logging
import pdb
#from itertools import tee
sys.path.append("./")
import lustre
import time
import tempfile
import signal
import copy
import numpy as np
from os import remove
import socket
import select
import atexit
import traceback
global pmdThread
global got_kill
from struct import unpack
got_kill=False
osts=[]

mylogger = logging.getLogger('pm2opentsdb')

logformate = logging.Formatter('%(filename)s %(lineno)s %(levelname)s  %(message)s')
logfilehandel = logging.StreamHandler()
mylogger.addHandler(logfilehandel)

mylogger.setLevel(logging.DEBUG)
mylogger.debug("starting program test")
class output:
	def	__init__(self,options, ostlist, debug=False,output=sys.stdout):
		self.options=options
	
		
#--- Print Header
	def header(self):
		pass

#=--- DATA OUTPUT
	def data(self, data,type='', time=0, total=0.0, colorme=None ):
		pass
		#elif type == "MDS":
		#		sys.stdout.write("\n")
					
def parseheader(line, debug=False):
	if debug: 
		mylogger.info("parseheader started\n")
	metricnum=0
	metrics={}
	for m in line.split():
		value=re.split(r':|\.|\["|\"\]',m)
		
		#['service170-ib1', 'lustre', 'mds', 'open', 'nbp1', '']
		#['service171-ib1', 'lustre', 'ost', 'read_bytes', 'nbp1-OST0060', '']
		if len(value) >=4 :
			if re.search('OST',value[4]):
				(fs,osthex)=value[4].split("-OST") #nbp1-OST0043
				target=int("0x%s" % osthex,16)
				if target not in osts:
					osts.append(target)
			else:
				target=None
				fs=value[4]
			if re.search(r'rpc_.*_pages',value[3]):
				valtype=1 #array
			else:
				valtype=0 #static number
			metrics[metricnum] = { 'fs': fs, 'valtype': valtype, 'systype' : value[1], 'subsys': value[2] , 'host' : value[0], 'metric': value[3], 'target': target}
		else:
			metrics[metricnum] = { 'metric' : value[0] }
		metricnum += 1
	osts.sort()
	if debug: 
		mylogger.info("parseheader ended returning \n")
		mylogger.debug(metrics)
	return metrics		

def parsedata(line,metrics):
	metricnum=0
	
	data={}
	for d in line.split():
		if metricnum == 0: # This is time
			data[metricnum]=float(d)
			
		elif d == "?":
			data[metricnum] = 0.0
#		thismetric = metrics[metricnum]['metric']
#		if re.search(r'rpc_.*_pages',thismetric):
		elif d.startswith('"'):
			rpclist=d.strip('"').split(",")[1:]			
			# x location of value is log(xx)/log(2)
			data[metricnum]= [np.array(map(int,rpclist[0::2])), np.array(map(int,rpclist[1::2]))]	
		else:
			data[metricnum]=float(d)
		metricnum += 1
	return data

def getprintosts(options,ost,mylustre):
	pass


		
def remap(data,osts,selection):
	ret={}
	for k in osts:
		ret[k]=data[k][selection]
	return ret	
from threading import Thread,Event

try:
	from Queue import Queue, Empty
except ImportError:
	from queue import Queue, Empty  # python 3.x
class mySocket():
	def __init__(self,host,port):
		self.host = host
		self.port = port
		self.numtries=5
		self.connect()

	def connect(self):
		self.mysocket=None
		try:
			self.mysocket = socket.create_connection((self.host, self.port), timeout=2)
			mylogger.debug("Connected to %s at %s" % (self.host, self.port))
			return self.mysocket
		except socket.error, (value,message):
			self.close()
			mylogger.exception( "Error: Couldn't open port: " + message )
			return None
		
	def recivedata(self):
		recvmessage= ""
		inputready,outputready,exceptready = select.select([0],[self.mysocket],[0],.1)
		if not outputready:
			return None
		try:
			recvmessage = self.mysocket.recv(1024)
		except socket.errno, (value, message):
			mylogger.error( "Error: Couldn't send message: " + message )
			return None
		return recvmessage
			
	def senddata(self, data):
		MSGLEN = len(''.join(data))
		lol = lambda lst, sz: [lst[i:i+sz] for i in range(0, len(lst), sz)]
		''' Need to try n times then through away the data'''
		for t in range(self.numtries):
			totalsent = 0
			sent = 0
			totalitems=0
			if not self.status():
				if not self.connect():
					time.sleep(.1)
					continue
			try:	

				# Send 50 Lines at a time				
				for l in lol(data,50):
					inputready,outputready,exceptready = select.select([self.mysocket],[0],[0])
					totalitems += len(l)
					msg = ''.join(l)
					#mylogger.debug(msg)
					sent = self.mysocket.send(msg)
					totalsent +=sent
					
				# can check recived data
					if sent != len(msg):
						mylogger.error("bytes sent not same as data dropping data: %i/%i" % (len(msg), sent))
						self.close()
						#Drop this data set
						return sent

				if totalsent != MSGLEN:
					mylogger.error("total bytes sent %i totalitems %i %i" % (totalsent, totalitems,len(data)))
					mylogger.error("bytes sent not same as data dropping data: %i/%i" % (MSGLEN, totalsent))
					self.close()
				break
			except ValueError:
				mylogger.error("Error sending data Try: %i" % t)
				self.close()
				mylogger.error(self.mysocket)
			self.close()
			time.sleep(.1)
		mylogger.info("total bytes sent %i totalitems %i %i" % (totalsent, totalitems,len(data)))
		return totalsent
		
	def close(self):
		if self.mysocket:
			self.mysocket.close()
		
	def status(self):
		
		try:
			fmt = "B"*7+"I"*21
			x = unpack(fmt, self.mysocket.getsockopt(socket.IPPROTO_TCP, socket.TCP_INFO, 92))
#			try:
#				self.senddata("stats")
#				recv=self.recivedata()
#			except:
#				return None
#			return x, recv
			if x[0] != 1:
				return None
			return x
		except:
			return None
'''
#define TCPI_OPT_TIMESTAMPS 1
#define TCPI_OPT_SACK       2
#define TCPI_OPT_WSCALE     4
#define TCPI_OPT_ECN        8

enum tcp_ca_state
{
    TCP_CA_Open = 0,
#define TCPF_CA_Open    (1<<TCP_CA_Open)
    TCP_CA_Disorder = 1,
#define TCPF_CA_Disorder (1<<TCP_CA_Disorder)
    TCP_CA_CWR = 2,
#define TCPF_CA_CWR (1<<TCP_CA_CWR)
    TCP_CA_Recovery = 3,
#define TCPF_CA_Recovery (1<<TCP_CA_Recovery)
    TCP_CA_Loss = 4
#define TCPF_CA_Loss    (1<<TCP_CA_Loss)
};

tcp_info
Data Fields
__u8 	tcpi_state
__u8 	tcpi_ca_state
__u8 	tcpi_retransmits
__u8 	tcpi_probes
__u8 	tcpi_backoff
__u8 	tcpi_options
__u8 	tcpi_snd_wscale: 4
__u8 	tcpi_rcv_wscale: 4
__u32 	tcpi_rto
__u32 	tcpi_ato
__u32 	tcpi_snd_mss
__u32 	tcpi_rcv_mss
__u32 	tcpi_unacked
__u32 	tcpi_sacked
__u32 	tcpi_lost
__u32 	tcpi_retrans
__u32 	tcpi_fackets
__u32 	tcpi_last_data_sent
__u32 	tcpi_last_ack_sent
__u32 	tcpi_last_data_recv
__u32 	tcpi_last_ack_recv
__u32 	tcpi_pmtu
__u32 	tcpi_rcv_ssthresh
__u32 	tcpi_rtt
__u32 	tcpi_rttvar
__u32 	tcpi_snd_ssthresh
__u32 	tcpi_snd_cwnd
__u32 	tcpi_advmss
__u32 	tcpi_reordering
'''
		
ON_POSIX = 'posix' in sys.builtin_module_names
class myThread(Thread):
	def __init__(self,cmd, target_queue, tempfile=None):
		Thread.__init__(self)
		self.cmd = cmd
		self.target_queue = target_queue
		self._started = Event()
		self.tempfile = tempfile
		
	def run(self):
		self.process = subprocess.Popen(self.cmd, bufsize=4096, shell=True, stdout=subprocess.PIPE)
		self._started.set()
		
		while True:
			line = self.process.stdout.readline() # blocking read
			self.target_queue.put(line)
	
	def killme(self):
		if self.tempfile:
			remove(self.tempfile)
		self.process.terminate()		
		
def readconfig(options):
	filename=options.configfile

def make_pcp_config(options, mdsstats, oststats):
	output=""
	hosts={}
		#Read Each line of configfile
	for line in open(options.configfile,'r'):
		if not line.startswith("#"): #skip comments
			(host, type, fs) = line.split()
			hosts[host]=[type,fs]
			if type == "mds":
				for stat in mdsstats:
					output = output + "%s:lustre.mds.%s\n" % (host, stat) 
			elif type == "oss":
				for stat in oststats:
					output = output + "%s:lustre.ost.%s\n" % (host, stat)
	return output, hosts
#	process.terminate()
def signal_handler(signal, frame):
	print 'You pressed Ctrl+C!'
	sys.exit(0)

signal.signal(signal.SIGCHLD, signal_handler)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGHUP, signal_handler)

def optionParser(argv):
	
#-- START Parse options --
	usage = "usage: %prog [options]"
	parser = OptionParser(usage=usage)
	parser.add_option("-d","--debug", action="store_true", dest="debug", default=False,
												help="enable debug output")
	
	parser.add_option("--logfile", "-l", dest="logfile", default=None,
			help="Where to log" )
	
	parser.add_option("--conf", "-c", dest="configfile", default=None,
			help="Configuration file")

	parser.add_option("--outdir", "-o", dest="outdir", default="./",
			help="(NOT WORKING) Director to store data")

	parser.add_option("--interval", type="int", dest="interval", default=5,
		help="Data time interval. Default 5 sec")

	parser.add_option("--daemon", action="store_true", dest="daemon", default=False,
		help="Run as a daemon")
	
	parser.add_option("--pid", "-p", dest="pidfile", default="/tmp/pm2opentsdb.pid", 
					help="Pid file name")
	
	(options, argv) = parser.parse_args()
	
#-- Require fsname
	if options.configfile==None: # Error out
			parser.error("Error no config file name Giving")
			sys.exit(1)
	
	if options.debug:
			mylogger.setLevel(logging.DEBUG)
	else:
			mylogger.setLevel(logging.INFO)
	if options.logfile:
		logfilehandel = logging.FileHandler(options.logfile)
		
	else:
		logfilehandel = logging.StreamHandler()
	mylogger.addHandler(logfilehandel)	

	return options
#-- END parse Options --

#-- Define SIGNALS to trap
def pm2opentsdb(options):
#-- Define variables
	timestep0=True
	olddata=[]
	rates={}
	units=1024 # default units KB
	#what stats to collect	
	mds_stats=["open", "close", "link", "unlink", "mkdir", "rmdir", "rename", "statfs",
			"getattr", "setattr", "getxattr", "setxattr" ]
	ost_stats = ["read_bytes", "write_bytes",  "rpc_read_pages", "rpc_write_pages", "cache_hit", "cache_miss" ]
	
	
#-- Create pmdumptext config file --
	tempfh=tempfile.NamedTemporaryFile(delete=False)
	( output, hosts) = make_pcp_config(options,mds_stats,ost_stats)
	tempfh.write(output)
	tempfh.flush()
	tempfh.close
#-- Connect to opentsdb
	pmdSocket = mySocket("localhost", 4242)
#	pmdSocket.status()	



#-- Start collection subprocess
	cmd="/usr/bin/pmdumptext -c %s -l -f %%s -t %i" % (tempfh.name,options.interval)
#	process = subprocess.Popen(cmd, bufsize=4096, shell=True, stdout=subprocess.PIPE)
	dataQueue = Queue()
	pmdThread = myThread(cmd,dataQueue,tempfile=tempfh.name)
	pmdThread.daemon=True
	try:
		pmdThread.start()
		pmdThread._started.wait()
	except:
		mylogger.error("Failed to start thread")
		sys.exit()
	atexit.register(pmdThread.killme)

	
	mylogger.info("Start subprocess pid %d" % (pmdThread.process.pid))
	mylogger.info("      subprocess pid %d" % (pmdThread.process.pid))

	
#			self,options, ostlist, debug=False,output=sys.stdout):
	while True: #Wait for the header
		#mylogger.debug("Searching for header")
		if dataQueue.qsize() > 0:
			try:
				nextline =dataQueue.get(False, .1)
			except:
				mylogger.error("Failed to read data from Queue")
				break
		else:
			continue
#			logging.critical("Error while reading nextline")
			
		if nextline == '':
			mylogger.debug("nextline '' break")
			break
		if re.search("Time", nextline):
			mylogger.info("header found")
			header=parseheader(nextline)

			break
	
	while True:
		total=0
		total1=0
		if dataQueue.qsize() > 0:
			try:
				nextline =dataQueue.get(False,.5)
				if not re.search(r'^[0-9].*', nextline):
					mylogger.debug("searching for ^[0-9].* not found continue")
					mylogger.debug(nextline)
					continue
			except:
				mylogger.error("Failed to read data from Queue")
				break
		else:
			continue
#		try:
#			nextline = process.stdout.readline()
#		except:
#			logging.critical("Error while reading nextline")
#			break
		if nextline == '':
			break
		data=parsedata(nextline,header)
# Data= [ost : {'write_bytes': x, 'rpc_readpaces': { 32: x, 64: x,..}..}
		
		if timestep0:
			olddata=copy.deepcopy(data)
			timestep0=False
			continue
		else:
			#index = 0 time
			
			# only look for ost [0, 1, 2] reads
			# data[0]['read_bytes']
			newtime=data[0]
			oldtime=olddata[0]
			dt= newtime - oldtime
			putmsg = []
			for metricnumber in data:
				thismetric=header[metricnumber]
				
				# Static numbers
				if metricnumber == 0: 
					timestep=time.strftime("%m/%d %H:%M:%S", time.localtime(int(data[0])))
					continue
					
				#Output lustre.ost.write_bytes host=x ost=x fs=x
				
				if thismetric['subsys'] == 'mds':
					
					putmsg.append( "put %s.%s.%s %i %f host=%s fs=%s\n" % (thismetric['systype'],thismetric['subsys'],thismetric['metric'], 
											newtime, data[metricnumber], thismetric['host'], thismetric['fs'] ) )
					
				#	print "%s.%s.%s %f host=%s fs=%s" % (thismetric['systype'], thismetric['subsys'],
				#										thismetric['metric'], rate, thismetric['host'],
				#										thismetric['fs'])
					
				elif thismetric['subsys'] == 'ost':
					if thismetric['valtype'] == 0:
						try:
							rate =  data[metricnumber]  / units
						except:
							mylogger.error("rate calculation error")
							print metricnumber, units
							sys.exit(1)
						if thismetric['metric'] == 'write_bytes':
							total += rate
						putmsg.append("put %s.%s.%s %i %f host=%s ost=%i fs=%s \n" % (thismetric['systype'], thismetric['subsys'],
														thismetric['metric'], newtime, rate, thismetric['host'],
														thismetric['target'], thismetric['fs']))
					elif thismetric['valtype'] == 1:
						new = data[metricnumber][1]
						old = olddata[metricnumber][1]
						rpcrate = [data[metricnumber][0], ( new - old )/dt ]
						for index,size in enumerate(rpcrate[0]):
							value=rpcrate[1][index]/dt
							size="%sk" % size
						
						
						#	print rate
							putmsg.append( "put %s.%s.%s %i %f host=%s ost=%i fs=%s size=%s \n" % (thismetric['systype'], thismetric['subsys'],
														thismetric['metric'], newtime, value, thismetric['host'],
														thismetric['target'], thismetric['fs'], size) )
						
						
						total1 += rpcrate[1]
			#mylogger.debug(putmsg)
			pmdSocket.senddata(putmsg)
			olddata=copy.deepcopy(data)
# Open socket and output the following
# 
#put proc.loadavg.1m 1288946927 0.36 host=foo 
#put proc.loadavg.5m 1288946927 0.62 host=foo 
#put proc.loadavg.1m 1288946942 0.43 host=foo 
#put proc.loadavg.5m 1288946942 0.62 host=foo
								
		# Calculate totals	
				
	remove(tempfh.name)	



if __name__ == '__main__':
	try:
		options=optionParser(sys.argv)
		if options.daemon:
			from daemon import Daemon
			class myDaemon(Daemon):
				def run(self):
					sys.stdout.write("Starting run\n")
					pm2opentsdb(options)
			#from lockfile import FileLock
			
			context = myDaemon(options.pidfile, stdout=options.logfile, stderr=options.logfile )
			context.start()
		else:
			pm2opentsdb(options)

				
	except Exception, err:
		print err
		traceback.print_exc()
'''
Metrics are sent like this
===
put http.hits 1234567890 34877 host=A webserver=static
put http.hits 1234567890 4357 host=A webserver=dynamic 
put proc.loadavg.1min 1234567890 1.35 host=A
===
You open the port and cat this into it
http:
data = "\n".join([json.dumps(stat._asdict()) for stat in cpuStats])
import requests

response = requests.post("your://url.here", data=data)
print response.content

import json
import requests
data = {'temperature':'24.3'}
data_json = json.dumps(data)
payload = {'json_playload': data_json, 'apikey': 'YOUR_API_KEY_HERE'}
r = requests.get('http://myserver/emoncms2/api/post', data=payload)
	
''' 
