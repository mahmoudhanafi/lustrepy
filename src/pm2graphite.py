#!/usr/bin/env python
# $Source: /cvsroot/lustrepy/src/pm2graphite.py,v $
'''
Created on Jul 5, 2013

@author: mhanafi

Required Python Modules
python-configobj


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
from configobj import ConfigObj, ConfigObjError
from pickle import dumps
from struct import pack

got_kill=False
osts=[]

mylogger = logging.getLogger('pm2graphite')

logformate = logging.Formatter('%(filename)s %(lineno)s %(levelname)s  %(message)s')
logfilehandel = logging.StreamHandler()
mylogger.addHandler(logfilehandel)

mylogger.setLevel(logging.DEBUG)
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
					
def parseheader(line, options, pmdalist, hostcfg):
	if options.debug: 
		mylogger.debug("parseheader started\n")
		#mylogger.debug(line)
	metricnum=0
	metrics={}
	
	
	for m in line.split(";"):
		valuetype=0 #0 single number 1 array
		target = None
		rate = False
#		mylogger.debug("working on : %s" % m )
#if metrics has a [] then we have a target
		_targetmatch = re.search(r'.*\[\"(.*)\"\].*',m)
		if  _targetmatch: # have a target
			_target = _targetmatch.group(1).replace(" ","_")
#			mylogger.debug("_target = %s" % _target )
			value=re.split(r':|\.|\["|\"\]',m)
			if "ost" in m:
				(fs,osthex)=_target.split("-OST")
				target=int("0x%s" % osthex,16)
				if '_bytes' in m:
					rate=True
				if re.search(r'.*_pages',value[3]):
					valuetype=1 #array
			else:
				target = _target
			metricpath = re.search(r'.*:(.*)\[.*',m).group(1)
			
		else:
			if re.search("Time", m):
				metricpath = m
				target = None
			else:
				metricpath = re.search(r'.*:(.*)$',m).group(1)
				target = None
		
		
		metrics[metricnum] = { 'valtype' : valuetype, 'path': metricpath, 'target': target , 'rate' : rate }
 		#if len(value) > 1:	
		#	metrics[metricnum] = { 'valtype': valuetype, 'systype' : value[1], 'subsys': value[2] , 'host' : value[0], 'metric': value[3], 'target': target}
		#else:
		#	metrics[metricnum] = { 'metric' : value[0] }
		#print metrics[metricnum] 
		metricnum += 1
	if options.debug: 
		mylogger.debug("parseheader ended returning \n")
		mylogger.debug(metrics)

	return metrics		

def parsedata(line,metrics):
	metricnum=0
	
	data={}
	for d in line.split(";"):
		if metricnum == 0: # This is time
			data[metricnum]=float(d)
			
		elif d == "0" or d == "?":
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
	#print data
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
		self.numtries=1
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
#		MSGLEN = len(data)
#		lol = lambda lst, sz: [lst[i:i+sz] for i in range(0, len(lst), sz)]
		''' Need to try n times then through away the data'''
		for t in range(self.numtries):
			sent = 0
#			if not self.status():
#				if not self.connect():
#					time.sleep(.4)
#					continue
			try:	
				self.connect()
				sent = self.mysocket.send(data)
				# can check recived data
				if sent != len(data):
					mylogger.error("bytes sent not same as data dropping data: %i/%i" % (len(data), sent))
					self.close()
					#Drop this data set
					return sent
			except ValueError:
				mylogger.error("Error sending data Try: %i" % t)
				self.close()
				mylogger.error(self.mysocket)
			self.close()
			time.sleep(.1)
		mylogger.debug("total bytes sent %i of %i" % (sent, len(data)))
		return sent
		
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
#define TCPI_OPT_SACK	   2
#define TCPI_OPT_WSCALE	 4
#define TCPI_OPT_ECN		8

enum tcp_ca_state
{
	TCP_CA_Open = 0,
#define TCPF_CA_Open	(1<<TCP_CA_Open)
	TCP_CA_Disorder = 1,
#define TCPF_CA_Disorder (1<<TCP_CA_Disorder)
	TCP_CA_CWR = 2,
#define TCPF_CA_CWR (1<<TCP_CA_CWR)
	TCP_CA_Recovery = 3,
#define TCPF_CA_Recovery (1<<TCP_CA_Recovery)
	TCP_CA_Loss = 4
#define TCPF_CA_Loss	(1<<TCP_CA_Loss)
};

tcp_info
Data Fields
__u8	 tcpi_state
__u8	 tcpi_ca_state
__u8	 tcpi_retransmits
__u8	 tcpi_probes
__u8	 tcpi_backoff
__u8	 tcpi_options
__u8	 tcpi_snd_wscale: 4
__u8	 tcpi_rcv_wscale: 4
__u32	 tcpi_rto
__u32	 tcpi_ato
__u32	 tcpi_snd_mss
__u32	 tcpi_rcv_mss
__u32	 tcpi_unacked
__u32	 tcpi_sacked
__u32	 tcpi_lost
__u32	 tcpi_retrans
__u32	 tcpi_fackets
__u32	 tcpi_last_data_sent
__u32	 tcpi_last_ack_sent
__u32	 tcpi_last_data_recv
__u32	 tcpi_last_ack_recv
__u32	 tcpi_pmtu
__u32	 tcpi_rcv_ssthresh
__u32	 tcpi_rtt
__u32	 tcpi_rttvar
__u32	 tcpi_snd_ssthresh
__u32	 tcpi_snd_cwnd
__u32	 tcpi_advmss
__u32	 tcpi_reordering
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

def make_pcp_config(options, myhostname, hostcfg, pmdalist ):
	
	output = ""
	for pmdaname in hostcfg['pmda']:
		mylogger.debug(pmdaname)
		output += '\n'.join([ myhostname+":"+s for s in pmdalist[pmdaname].split()])
		output +='\n'
	return output, myhostname
#	process.terminate()
def signal_handler(signal, frame):
	mylogger.error('You pressed Ctrl+C!')
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
	
	parser.add_option("--logfile", "-l", dest="logfile", default="/tmp/pm2graphite.log",
			help="Where to log" )
	
	parser.add_option("--conf", "-c", dest="configfile", default="/usr/local/etc/pm2graphite.conf",
			help="Configuration file")

	parser.add_option("--pmdaconf", dest="pmdaconf", default="/usr/local/etc/pmdalist.conf",
			help="Config file for pmda list" )
			
	parser.add_option("--outdir", "-o", dest="outdir", default="./",
			help="(NOT WORKING) Director to store data")

	parser.add_option("--interval", type="int", dest="interval", default=5,
		help="Data time interval. Default 5 sec")

	parser.add_option("--daemon", action="store_true", dest="daemon", default=False,
		help="Run as a daemon")
	
	parser.add_option("--pid", dest="pidfile", default="/tmp/pm2graphite.pid", 
					help="Pid file name")
	
	parser.add_option("--host", dest="hostname", default=None,
					help="overide hostname")
	
	parser.add_option("--server", "-s", dest="server", default="matrix2",
					help="Default graphite server")
	parser.add_option("--port", "-p", dest="serverport", type="int", default=2003,
					help="Default graphite server port")
	
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
def pm2graphite(options):
	#-- Define variables
	timestep0=True
	units=1024 # default units KB
	# Load pmdaconf
	try:
		pmdalist=ConfigObj(options.pmdaconf)
	except (ConfigObjError, IOError), e:
		mylogger.error('Could not read "%s": %s' % (options.pmdaconf, e))
	try:
		hostcfg=ConfigObj(options.configfile)
	
	except (ConfigObjError, IOError), e:
		mylogger.error('Could not read "%s": %s' % (options.configfile, e))
		# what is my host name.
	# Load pm2graphite.conf
	if not options.hostname:
		myhostname=socket.gethostname().split('.')[0]
	else:
		myhostname=options.hostname	

	output = ""
	#Is this host listed in the config file. If not exit
	if not hostcfg.has_key(myhostname):
		mylogger.error("This host [%s] is not in config file\n" % myhostname)
		sys.exit(-1)
	myfs = hostcfg[myhostname]['filesystem']
# Check hostcfg
#	defined:
#			fs
#			pmda




#-- Create pmdumptext config file --
	tempfh=tempfile.NamedTemporaryFile(delete=False)
	( output, host) = make_pcp_config(options, myhostname, hostcfg[myhostname], pmdalist)
	tempfh.write(output)
	tempfh.flush()
	tempfh.close
#-- Connect to grahpite
	pmdSocket = mySocket(options.server, options.serverport)
	

#-- Start collection subprocess
	cmd="/usr/bin/pmdumptext -d ';' -U '0' -c %s -l -f %%s  -t %i" % (tempfh.name,options.interval)
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
	mylogger.info("	  subprocess pid %d" % (pmdThread.process.pid))
	dataline=re.compile(r'^[0-9].*')	
	
#			self,options, ostlist, debug=False,output=sys.stdout):
	while True: #Wait for the header
		#mylogger.debug("Searching for header")
		if dataQueue.qsize() <= 0:
			continue
		try:
			nextline =dataQueue.get(False, .1)
		except:
			mylogger.error("Failed to read data from Queue")
			break
					
		if nextline == '':
			mylogger.debug("nextline '' break")
			break
		if re.search("Time", nextline):
			mylogger.info("header found")
			header=parseheader(nextline, options, pmdalist, hostcfg[myhostname])

			break
	
	while True:

		if dataQueue.qsize() <= 0:
			continue
		try:
			nextline =dataQueue.get(False,.5)
			if not re.search(r'^[0-9].*', nextline):
				mylogger.debug("searching for ^[0-9].* not found continue")
				#mylogger.debug(nextline)
				continue
		except:
			mylogger.error("Failed to read data from Queue")
			break
		
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
			putmsg= []
			listOfMetricTuples=[]
			total_read_bytes = 0
			total_write_bytes = 0
			for metricnumber in data:
				
				thismetric=header[metricnumber]
				
				# Static numbers
				if metricnumber == 0: 
					timestep=time.strftime("%m/%d %H:%M:%S", time.localtime(int(data[0])))
					continue
					
				#Output lustre.ost.write_bytes host=x ost=x fs=x
				#print thismetric
				if 'mds' in thismetric['path']:
					_path = re.search(r'lustre\.mds\.(.*$)',thismetric['path']).group(1)
					metricpath = "%s.%s.%s" % ( myfs, myhostname, _path)
					putmsg.append( "%s %f %i\n" % (metricpath, data[metricnumber], newtime) )

				elif 'ost' in thismetric['path']:
					#mylogger.debug(thismetric['path'])
					_path = re.search(r'lustre\.ost\.(.*$)',thismetric['path']).group(1)
					if thismetric['valtype'] == 0:
						if thismetric['rate']:
							try:
								value =  data[metricnumber]  / units
								if options.debug:
									if  'read_bytes' in thismetric['path']:
										total_read_bytes += value
									if 'write_bytes' in thismetric['path']:
										total_write_bytes += value
							except:
								mylogger.error("rate calculation error")
								#print metricnumber, units
								sys.exit(1)
						else:
							value = data[metricnumber]
						if thismetric['target'] != None:
							metricpath = "%s.%s.%s.ost%s" % (myfs, myhostname, _path, thismetric['target'] )
							
							putmsg.append("%s %s %i \n" % (metricpath, value, newtime))
						else:
							metricpath = "%s.%s.%s" % (myfs, myhostname, _path)
							putmsg.append("%s %f %i \n" % (metricpath, value, newtime))
					elif thismetric['valtype'] == 1:
						new = data[metricnumber][1]
						old = olddata[metricnumber][1]
						rpcrate = [data[metricnumber][0], ( new - old )/dt ]
						for index,size in enumerate(rpcrate[0]):
							value=rpcrate[1][index]/dt
							size="%sk" % size
						
						
						#	print rate
							metricpath = "%s.%s.%s.%s.ost%s" % (myfs, myhostname, _path, size, thismetric['target'])
							putmsg.append( "%s %f %i \n" % (metricpath, value,newtime) )
						
				else:
					_path=thismetric['path']
					value = data[metricnumber]
					if thismetric['target'] != None:
						metricpath = "%s.%s.%s.%s" % (myfs, myhostname, _path, thismetric['target'])
						putmsg.append( "%s %f %i \n" % (metricpath, value,newtime) )
					else:
						metricpath = "%s.%s.%s" % (myfs, myhostname, _path)
						
						putmsg.append( "%s %f %i \n" % (metricpath, value,newtime) )
#				listOfMetricTuples.append((metricpath, (newtime, value)))		
				
#			mylogger.debug(listOfMetricTuples)
#			payload = dumps(listOfMetricTuples)
#			payloadheader = pack("!L", len(payload))
#			message = payloadheader + payload
			
			message = ''.join(putmsg)
			mylogger.debug( message )
			if options.debug:
				mylogger.debug("read [ %.0f ] write [ %.0f ] " % (total_read_bytes,total_write_bytes))
			pmdSocket.senddata(message)
			olddata=copy.deepcopy(data)

								
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
					pm2graphite(options)
			#from lockfile import FileLock
			
			context = myDaemon(options.pidfile, stdout=options.logfile, stderr=options.logfile )
			context.start()
		else:
			pm2graphite(options)

				
	except Exception, err:
		print err
		traceback.print_exc()
'''
Open socket and output the following
# 
proc.loadavg.1m.service[xxx...] 0.36 1288946927 0.36  
proc.loadavg.5m.service[xxx...] 0.62 1288946927 0.62  
proc.loadavg.1m.service[xxx...] 0.43 1288946942 0.43  
proc.loadavg.5m.service[xxx...] 0.62 1288946942 0.62
nbp7.read.bytes.ost[0...]
nbp7.read.rpc_pages.[4k,8k,16k,32k,64k,128k,256k,512k,1028k].ost#
nbp7.write.bytes.ost[0...]
nbp7.wrote.rpc_pages.[4k,8k,16k,32k,64k,128k,256k,512k,1028k].ost#
nbp7.mdt.[open,close,link,unlink,mkdir,rmdir,rename,statfs,getattr,setattr,getxattr,setxattr]


Pickle formate
[(path, (timestamp, value)), ...]

Once youve formed a list of sufficient size (dont go too big!), 
send the data over a socket to Carbons pickle receiver 
(by default, port 2004). Youll need to pack your pickled 
data into a packet containing a simple header:

payload = pickle.dumps(listOfMetricTuples)
header = struct.pack("!L", len(payload))
message = header + payload

[(path, (timestamp, value)), ...]



#filesyste.nbp7.ost.#.
#nbp7.mds. 
#mds ["open", "close", "link", "unlink", "mkdir", "rmdir", "rename", "statfs",
#			"getattr", "setattr", "getxattr", "setxattr" ]
#ost  ["read_bytes", "write_bytes",  "rpc_read_pages", "rpc_write_pages", "cache_hit", "cache_miss" ]
'''	
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
