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

import sys
sys.path.append("./")
import re
import subprocess
from optparse import OptionParser
import logging
import pdb
import time
import tempfile
import signal
from copy import deepcopy
from numpy import array
from os import remove
import socket
import select
import atexit
from traceback import print_exc
from struct import unpack
from configobj import ConfigObj, ConfigObjError
from pickle import dumps
from struct import pack
from threading import Thread,Event

mylogger = logging.getLogger('pm2graphite')

logFormate = logging.Formatter('%(filename)s %(lineno)s %(levelname)s  %(message)s')
logStdOutHandle = logging.StreamHandler()
logStdOutHandle.setFormatter(logFormate)

mylogger.addHandler(logStdOutHandle)
mylogger.setLevel(logging.DEBUG)


def signal_handler(signal, frame):
	mylogger.debug('Got exit signal')
	sys.exit(0)
signal.signal(signal.SIGCHLD, signal_handler)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGHUP, signal_handler)
#-- Place holder class
class output:
	def	__init__(self,options, ostlist, debug=False,output=sys.stdout):
		self.options = options
	
#--- Print Header
	def header(self):
		pass

#--- Data output 
	def data(self, data,type='', time=0, total=0.0, colorme=None ):
		pass
					
def parseheader(line, options, pmdalist, hostcfg):
	if options.debug: 
		mylogger.debug("parseheader started\n")
		#mylogger.debug(line)
	metricnum = 0
	metrics = {}
	
	
	for m in line.split(";"):
		valuetype = 0 #0 single number 1 array
		target = None
		rate = False
#		mylogger.debug("working on : %s" % m )
#if metrics has a [] then we have a target
		_targetmatch = re.search(r'.*\[\"(.*)\"\].*',m)
		if  _targetmatch: # have a target
			_target = _targetmatch.group(1).replace(" ","_")
#			mylogger.debug("_target = %s" % _target )
			value = re.split(r':|\.|\["|\"\]',m)
			if "ost" in m:
				(fs,osthex) = _target.split("-OST")
				target = int("0x%s" % osthex,16)
				if '_bytes' in m:
					rate = True
				if re.search(r'.*_pages',value[3]):
					valuetype = 1 #valuetype 1 is an list of values
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
		metricnum += 1

	mylogger.debug("parseheader ended returning \n")
	mylogger.debug(metrics)
	return metrics		


#-- Data parser function
def parsedata(line,metrics):
	metricnum = 0
	data = {}
	for d in line.split(";"):
		if metricnum == 0: 
			data[metricnum] = float(d)
			
		elif d == "0" or d == "?":
			data[metricnum] = 0.0
		elif d.startswith('"'):
			rpclist=d.strip('"').split(",")[1:]			
			# x location of value is log(xx)/log(2)
			data[metricnum] = [array(map(int,rpclist[0::2])), array(map(int,rpclist[1::2]))]	
		else:
			data[metricnum] = float(d)
		metricnum += 1
	#print data
	return data

		

try:
	from Queue import Queue, Empty
except ImportError:
	from queue import Queue, Empty  # python 3.x

class mySocket():
	def __init__(self,host,port):
		self.host = host
		self.port = port
		self.numtries = 1
		self.connect()

	def connect(self):
		self.mysocket = None
		try:
			self.mysocket = socket.create_connection((self.host, self.port), timeout=2)
			mylogger.debug("Connected to %s at %s" % (self.host, self.port))
			return self.mysocket
		except socket.error, (value,message):
			self.close()
			mylogger.exception( "Error: Couldn't open port: " + message )
			return None
		
	def recivedata(self):
		recvmessage = ""
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
		#Need to try n times then through away the data
		#Not working for graphite server port
		for t in range(self.numtries):
			sent = 0
			if not self.connect():
				time.sleep(.1)
				continue
			try:	
#				self.connect()
				sent = self.mysocket.send(data)
				# can check recived data
				if sent != len(data):
					mylogger.error("bytes sent not same as data dropping data: %i/%i" % (len(data), sent))
					self.close()
					#Drop this data set
				break
			except ValueError:
				mylogger.error("Error sending data Try: %i" % t)
				self.close()
				mylogger.error(self.mysocket)
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
			try:
				remove(self.tempfile)
			except:
				pass
		self.process.terminate()		
		
def make_pcp_config(options, myhostname, hostcfg, pmdalist ):
	output = ""
	try:
		for pmdaname in hostcfg['pmda']:
			mylogger.debug(pmdaname)
			output += '\n'.join([ myhostname+":"+s for s in pmdalist[pmdaname].split()])
			output +='\n'
	except:
		mylogger.error("Failed to create pcp_config file.")
		sys.exit(1)
	return output, myhostname
#	process.terminate()



def optionParser(argv):
#-- START Parse options --
	usage = "usage: %prog [options]"
	parser = OptionParser(usage=usage)
	parser.add_option("-d","--debug", action = "store_true", dest = "debug", default = False,
												help="enable debug output")
	
	parser.add_option("--logfile", "-l", dest = "logfile", default = None,
		help="Where to log" )
	
	parser.add_option("--conf", "-c", dest = "configfile", 
		default = "/usr/local/graphite/conf/pm2graphite.conf",
		help = "Configuration file default[/usr/local/graphite/conf/pm2graphite.conf]")

	parser.add_option("--pmdaconf", dest = "pmdaconf", 
		default = "/usr/local/graphite/conf/pmlist.conf",
		help = "Config file for pmda list default[/usr/local/graphite/conf/pmdalist.conf]" )
			
	parser.add_option("--interval", "-i", type = "int", dest = "interval", default = 1,
		help = "Data time interval. Default 1 sec")

	parser.add_option("--daemon", action = "store_true", dest = "daemon", default = False,
		help="Run as a daemon")
	
	parser.add_option("--pid", dest = "pidfile", default = "/tmp/pm2graphite.pid", 
		help = "Pid file name default[/tmp/pm2graphite.pid]")
	
	parser.add_option("--hostname", dest = "hostname", 
		default = socket.gethostname().split('.')[0],
		help = "overide local hostanme to match one in config file")
	
	parser.add_option("--server", "-s", dest = "server", default = "matrix2",
		help = "Default graphite server default[matrix2]")

	parser.add_option("--port", "-p", dest = "serverport", type = "int", default = 2003,
		help="Default graphite server port default[2003]")
	
	(options, argv) = parser.parse_args()
	
	if options.configfile == None: # Error out
			parser.error("Error no config file name Giving")
			sys.exit(1)
	
	if options.debug:
			mylogger.setLevel(logging.DEBUG)
	else:
			mylogger.setLevel(logging.INFO)
	if options.daemon and not options.logfile:
		mylogger.error("you must give logfile option if running in daemon")
		sys.exit(1)
	if options.logfile:
		logFileHandle = logging.FileHandler(options.logfile)
		logFileHandle.setFormatter(logFormate)
		mylogger.removeHandler(logStdOutHandle)		
		mylogger.addHandler(logFileHandle)

	return options

def pm2graphite(options):
#-- Define variables
	timestep0 = True
	units = 1024/1024 # default units MB
	output = ""
	myhostname = options.hostname
#-- Load pmdaconf
	try:
		pmdalist = ConfigObj(options.pmdaconf,raise_errors=True)

	except (ConfigObjError, IOError), e:
		mylogger.error('Could not read "%s": %s' % (options.pmdaconf, e))
		sys.exit(1)
#--	Load pm2graphti.conf
	try:
		hostcfg = ConfigObj(options.configfile,raise_errors=True)
	
	except (ConfigObjError, IOError), e:
		mylogger.error('Could not read "%s": %s' % (options.configfile, e))
		sys.exit(1)


#-- Is this host listed in the config file. If not exit
	if not hostcfg.has_key(myhostname):
		mylogger.error("This host [%s] is not in config file\n" % myhostname)
		sys.exit(1)

#-- MY filesystem
	if "filesystem" in hostcfg[myhostname]:
		myfs = hostcfg[myhostname]['filesystem']
	else:
		mylogger.error("Filesystem tage not defined for this host.")
		sys.exit()

#-- Create pmdumptext config file --
	tempfh=tempfile.NamedTemporaryFile(delete=False)
	( output, host) = make_pcp_config(options, myhostname, hostcfg[myhostname], pmdalist)
	tempfh.write(output)
	tempfh.flush()
	tempfh.close

#-- init socket connection to server
	pmdSocket = mySocket(options.server, options.serverport)
	

#-- Start collection subprocess
	cmd="/usr/bin/pmdumptext -d ';' -U '0' -c %s -l -f %%s  -t %i" % (tempfh.name,options.interval)


#-- Init threads and launch pmdumptext in a thread
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


	reDataLine=re.compile(r'^[0-9].*')	
	reMetricPathMDS = re.compile(r'lustre\.mds\.(.*$)')
	reMetricPathOST = re.compile(r'lustre\.ost\.(.*$)')

#-- Find header line and parse	
	count=0
	while True: 
		mylogger.debug("Searching for header")
		if dataQueue.qsize() <= 0:
			continue
		try:
			nextline =dataQueue.get(False, .4)
		except:
			mylogger.error("Failed to read data from Queue")
			break
					
		if nextline == '':
			mylogger.debug("nextline '' break")
			break
		if "Time" in nextline:
			mylogger.info("header found")
			header=parseheader(nextline, options, pmdalist, hostcfg[myhostname])
			break


#-- Main data parser loop
	while True:

		if dataQueue.qsize() <= 0:
			continue
		try:
			nextline =dataQueue.get(False,.4)
			if not reDataLine.search(nextline):
				mylogger.debug("searching for ^[0-9].* not found continue")
				#mylogger.debug(nextline)
				continue
		except:
			mylogger.error("Failed to read data from Queue")
			break
		
		if nextline == '':
			break
#-- Data= [ost : {'write_bytes': x, 'rpc_readpaces': { 32: x, 64: x,..}..}
		data=parsedata(nextline,header)
		
		if timestep0:
			olddata = deepcopy(data)
			timestep0 = False
			continue
		else:
			#index = 0 time
			newtime = data[0]
			oldtime = olddata[0]
			dt = newtime - oldtime
			putmsg = []
			listOfMetricTuples = []

			total_read_bytes = 0
			total_write_bytes = 0
			for metricnumber in data:
				
				thismetric = header[metricnumber]
				
				#-- time metric
				if metricnumber == 0: 
					continue
					
				#Output lustre.ost.write_bytes host=x ost=x fs=x
				if 'mds' in thismetric['path']:
					_path = reMetricPathMDS.search(thismetric['path']).group(1)
					metricpath = "%s.%s.%s" % ( myfs, myhostname, _path)
					putmsg.append( "%s %f %i\n" % (metricpath, data[metricnumber], newtime) )

				elif 'ost' in thismetric['path']:
					#mylogger.debug(thismetric['path'])
					_path = reMetricPathOST.search(thismetric['path']).group(1)
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
						if thismetric['target']:
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
#				listOfMetricTuples.append((metricpath, (newtime, value)))		#if using pickel method
				
#			mylogger.debug(listOfMetricTuples)
#			payload = dumps(listOfMetricTuples)
#			payloadheader = pack("!L", len(payload))
#			message = payloadheader + payload
			
			message = ''.join(putmsg)
			mylogger.debug( message )
			if options.debug:
				mylogger.debug("read [ %.0f ] write [ %.0f ] " % (total_read_bytes,total_write_bytes))
			pmdSocket.senddata(message)
			olddata=deepcopy(data)

	try:
		remove(tempfh.name)	
	except:
		pass



if __name__ == '__main__':
	try:
		options = optionParser(sys.argv)
		if options.daemon:
			from daemon import Daemon
			class myDaemon(Daemon):
				def run(self): #overload run function
					mylogger.debug("pm2graphite started")
					pm2graphite(options)

			context = myDaemon(options.pidfile, stdout=options.logfile, stderr=options.logfile )
			context.start()
		else:
			pm2graphite(options)

				
	except Exception, err:
		print err
		print_exc()
'''
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
'''	
