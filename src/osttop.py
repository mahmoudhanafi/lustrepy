#!/usr/bin/env python
'''
-- GPL HEADER START --

Copyright (c) 2013 NASA 
Author: Mahmoud Hanafi (mahmoud.hanafi@nasa.gov)

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

-- GPL HEADER END --
'''

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

global pmdThread
global got_kill

got_kill=False
osts=[]

class bcolors:
	WHITE = '\033[97m'
	CYAN = '\033[96m'
	MEGENTA = '\033[95m'
	BLUE = '\033[94m'
	GREEN = '\033[92m'
	YELLOW = '\033[93m'
	RED = '\033[91m'
	BLACK = '\033[90m'
	ENDC = '\033[0m'

	def disable(self):
		self.WHITE = ''
		self.CYAN = ''
		self.MEGENTA = ''
		self.BLUE = ''
		self.GREEN = ''
		self.YELLOW = ''
		self.RED = ''
		self.ENDC = ''

	def flipcolor(self): #flip between 2 colors"
		while True:
			yield self.GREEN
			yield self.ENDC

class output:
	def	__init__(self,options, ostlist, debug=False,output=sys.stdout):
		self.options=options
		self.output=output
		self.ostlist=ostlist
		self.linecolor=self.flipcolor()
		self.color=True
		if options.nocolor:
			self.color=False
			self.disablecolor()
	# Define Colors
	WHITE = '\033[97m'
	CYAN = '\033[96m'
	MEGENTA = '\033[95m'
	BLUE = '\033[94m'
	GREEN = '\033[92m'
	YELLOW = '\033[93m'
	RED = '\033[91m'
	BLACK = '\033[90m'
	ENDC = '\033[0m'

	BG_BLACK = '\033[100m'
	BG_RED = '\033[101m'
	BG_YELLOW = '\033[102m'
	
	BOLD='\033[1m'
	UNDERLINE = '\033[24m'
	def disablecolor(self):
		self.WHITE = ''
		self.CYAN = ''
		self.MEGENTA = ''
		self.BLUE = ''
		self.GREEN = ''
		self.YELLOW = ''
		self.RED = ''
		self.ENDC = ''
		self.BG_BLACK = ''
		self.BG_RED = ''
		self.BG_YELLOW = ''
		self.BOLD= ''
		self.UNDERLINE = ''

	def flipcolor(self): #flip between 2 colors"
		if not self.color:
			yield self.ENDC
		while True:
			yield self.BLUE
			yield self.ENDC

	

#--- Print Header
	def header(self):
		self.output.write(self.BG_BLACK+self.WHITE+"  %18s " % "Time")
		if self.options.showwrite or self.options.showread:
			for ost in self.ostlist:
				self.output.write( "%8i " % ost )
			if self.options.totalosts:
				self.output.write(" %6s" % "total")
		if self.options.mdsstats:
			for stat in self.options.mds_stats:
				self.output.write(" %10s" % stat)
			self.output.write(" %10s" % "total")
		self.output.write( self.ENDC+"\n")

#--- print header
#			T   TT   TTT
#		if options.sortme:
#			printosts=sorted(totalrw, key=totalrw.get)
#			sys.stdout.write(mycolors.HEADER+"  %10s " % "Time")
#			for ost in printosts:
#				sys.stdout.write( "%4i " % ost )
#			if options.totalosts:
#				sys.stdout.write(" %6s" % "total")
#			sys.stdout.write("\n"+mycolors.ENDC)

#=--- DATA OUTPUT
	def data(self, data,type='', time=0, total=0.0, colorme=None ):
		if colorme:
				sys.stdout.write(colorme)	
		else:
				sys.stdout.write(self.linecolor.next())
		if type == 'MDS':
			if not (self.options.showwrite or self.options.showread):
				sys.stdout.write("%3s %16s " % (type,time))
		elif self.options.showwrite or self.options.showread and type != 'MDS':
			sys.stdout.write("%3s %16s " % (type,time) )
			
		if self.options.mdsstats and type == 'MDS':
			for stat in self.options.mds_stats:
				self.output.write(" %10.0f" % float(data[stat]))
			self.output.write(" %10.0f" % total)
			sys.stdout.write("\n")
		elif self.options.showwrite or self.options.showread:
			for ost in self.ostlist:
				self.output.write( "%8.0f " % data[ost]) #rates["read_bytes"][ost])
			if self.options.totalosts:
				self.output.write(" %6.0f" % total)
		if type == 'R':
			if not self.options.mdsstats and not self.options.showwrite:
				sys.stdout.write("\n")
		elif type == 'W':
			if not self.options.mdsstats and not self.options.showread:
				sys.stdout.write("\n")
		sys.stdout.flush()
		#elif type == "MDS":
		#		sys.stdout.write("\n")
					
def parseheader(line, debug=False):
	if debug: sys.stderr.write("parseheader started\n")
	metricnum=0
	metrics={}
	for m in line.split():
		value=re.split(r':|\.|\["|\"\]',m)
		
		#['service170-ib1', 'lustre', 'mds', 'open', 'nbp1', '']
		#['service171-ib1', 'lustre', 'ost', 'read_bytes', 'nbp1-OST0060', '']
		if len(value) >=4 :
			if re.search('OST',value[4]):
				targethex=value[4].split("OST") #nbp1-OST0043
				target=int("0x%s" % targethex[1],16)
				if target not in osts:
					osts.append(target)
			else:
				target=value[4]
			metrics[metricnum] = { 'systype' : value[1], 'subsys': value[2] , 'host' : value[0], 'metric': value[3], 'target': target}
		else:
			metrics[metricnum] = { 'metric' : value[0] }
		metricnum += 1
	osts.sort()
	if debug: 
		sys.stderr.write("parseheader ended returning \n")
		sys.stderr.write(metrics)
	return metrics		

def parsedata(line,metrics):
	metricnum=0
	data={}
	for d in line.split():
		if metricnum == 0: # This is time
			target = 'Time'
		else:
			target = metrics[metricnum]['target']

		metric = metrics[metricnum]['metric']
		if d == "?": 
			d=0
		if not data.has_key(target):
			data[target]={}
		if re.search(r'rpc_.*_pages',metric): #Got rpc pages metric
											 #(time,1,xx,2,xx,4,xx,8,xx,16,xx,32,xx,64,xx,128,xx,256,xx)
			rpclist=d.strip('"').split(",")[1:]
			data[target][metric]=dict(zip(map(int,rpclist[0::2]),map(int,rpclist[1::2])))	
		else:
			data[target][metric]=d
		metricnum += 1
	return data

def getprintosts(options,osts,mylustre):
		#Limit osts to ost list
	_printosts=[]
	if options.osts:
		for i in options.osts.split(","):
			_printosts.append(int(i))

	#Limit osts to oss
	elif options.oss:
		for oss in options.oss.split(","):	
			#-- make sure we have -ib1 in the oss name
			if not re.search("-ib1",oss):
				oss = oss+"-ib1"
			ostlist=mylustre.get_ost_byoss(oss)
			if ostlist:
				_printosts += ostlist
			else:
				sys.stderr.write("Oss return 0 osts")
				sys.exit()
	# List all osts for the filesystem
	else: 
		return osts
#	We should check if the ost list give via options are in the data
	for ost in _printosts:
		if not ost in osts:
			sys.stderr.write("OST [ % ] give is not part of filesystem\n" % ost )
			sys.exit(255)

	return _printosts

		
def remap(data,osts,selection):
	ret={}
	for k in osts:
		ret[k]=data[k][selection]
	return ret	
from threading import Thread
try:
	from Queue import Queue, Empty
except ImportError:
	from queue import Queue, Empty  # python 3.x

ON_POSIX = 'posix' in sys.builtin_module_names
class myThread(Thread):
	def __init__(self,cmd, target_queue):
		Thread.__init__(self)
		self.cmd = cmd
		self.target_queue = target_queue
		self._started = Event()

	def run(self):
		print "Starting Thread with cmd ", self.cmd
		self.process = subprocess.Popen(self.cmd, bufsize=4096, shell=True, stdout=subprocess.PIPE)
		self._started.set()
		while True:
			line = self.process.stdout.readline() # blocking read
			self.target_queue.put(line)

	def killme(self):
		self.process.terminate()

#	process.terminate()
def signal_handler(signal, frame):
	print 'You pressed Ctrl+C!'
	sys.exit(0)

signal.signal(signal.SIGCHLD, signal_handler)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGHUP, signal_handler)

#-- Define SIGNALS to trap
def main(argv):
#-- Define variables
	now = time.mktime(time.gmtime())
	timestep0=True
	outputcounter=0
	rates={}
	printosts=[]
	header={}
	units=1024 # default units KB
	#what stats to collect	
	mds_stats=["open", "close", "link", "unlink", "mkdir", "rmdir", "rename", "statfs" ]
	ost_stats = ["read_bytes", "write_bytes",  "rpc_read_pages", "rpc_write_pages", "cache_hit", "cache_miss" ]	
	X={}
	Y={}	
	xlabels=[]
	plotwidth=60
	#mds_stats=["open", "close", "link", "unlink", "mkdir", "rmdir", "rename", "statfs", 
    #                  "getattr", "setattr", "getxattr", "setxattr"]
	#ost_stats = ["read_bytes", "write_bytes", "rpc_read_pages", "rpc_write_pages",
    #                  "cache_hit", "cache_miss"]

#-- START Parse options --
	usage = "usage: %prog [options]"
	parser = OptionParser(usage=usage)
	parser.add_option("-d","--debug", action="store_true", dest="debug", default=False,
												help="enable debug output")
	
	parser.add_option("--logfile", "-l", dest="logfile", default=None,
			help="Where to log" )
	
	parser.add_option("--filesystem", "-f", dest="fsname", default=None,
			help="Lustre Filesystem")

	parser.add_option("--outdir", "-o", dest="outdir", default="./",
			help="(NOT WORKING) Director to store data")

	parser.add_option("--filename", dest="outfilename", default=None,
		help="(NOT WORKING) Store data to files. Define the filename pattern [outfilename.write.out]")

	parser.add_option("--runtime", type="int", dest="runtime", default=None,
		help="(NOT WORKING) Limit run time in seconds")

	parser.add_option("--oss", dest="oss", default=None,
		help="Limit output data to this oss(s) --oss service181 or --oss 'service181,service183'")
	
	parser.add_option("--osts", dest="osts", default=None,
		help="Limit output data to these osts [ '1,4,5,2' ]")

	parser.add_option("--sort", action="store_true", dest="sortme", default=False,
		help="Sort osts by data size")

	parser.add_option("--osts-totals", action="store_true", dest="totalosts", default=False,
		help="Output total for selected osts")

	parser.add_option("--rw-totals", action="store_true", dest="totalrw", default=False,
		help="Output total for read and write data")

	parser.add_option("--read", "-r", action="store_true", dest="showread", default=False,
		help="Output Read [ --read ] default no")

	parser.add_option("--write", "-w",action="store_true", dest="showwrite", default=False,
		help="Output write [ --write ] default no" )
	
	parser.add_option("--no-color", action="store_true", dest="nocolor", default=False,
		help="Turn off color")

	parser.add_option("--show-rpcs", action="store_true", dest="showrpcs", default=False,
		help="Show rpc size")

	parser.add_option("--interval", type="int", dest="interval", default=5,
		help="Data time interval. Default 5 sec")

	parser.add_option("--repeat-header", type="int", dest="repeatheader", default=0,
		help="Repeat header number of time")

	parser.add_option("--units", dest="units", default="KB",
		help="units to print B, KB, MB")
	
	parser.add_option("--plot", action="store_true", dest="plots", default=False,
		help="Show data in heatmap format")
	
	parser.add_option("--no-output", action="store_true", dest="nooutput", default=False,
		help="Suppress all output")

	parser.add_option("--mds", action="store_true", dest="mdsstats", default=False,
		help="show/plot mds stats in addtion to osts")
	parser.add_option("--timescale", type="int", dest="xlimit", default=60,
		help="Select time range to plot in sec")
	
	(options, argv) = parser.parse_args()
#-- END parse Options --
	options.mds_stats=mds_stats
	if options.plots:
		from nas_plotlib import heatmap,stripchart
		 #100 time steps
#A		myplot=plot(title="Read", debug=True)
		if options.showread:
			plotReads=heatmap(title="Read [%s /s]" % options.units, debug=False)
		if options.showwrite:
			plotWrites=heatmap(title="Write [%s /s]" % options.units, debug=False)
		if options.totalrw:
			plottotals=heatmap(title="total read and write", debug=False)
		if options.mdsstats:
			mdsplot=stripchart("MDS STATS for %s"% options.fsname, xrange=options.xlimit,debug=True)
			
	if options.debug:
			logging.basicConfig(format='%(filename)s %(lineno)s %(levelname)s  %(message)s', level=logging.DEBUG)
	else:
			logging.basicConfig(format='%(filename)s %(lineno)s %(levelname)s  %(message)s', level=logging.INFO)

#-- Require fsname
	if options.fsname==None: # Error out
			parser.error("Error no filesystem name Giving")
			sys.exit(1)
	if re.match("/", options.fsname[0]):
			#Strip off /
			filesystem=options.fsname[1:]
			mntpoint=options.fsname
	else:
			filesystem=options.fsname
			mntpoint="/" + options.fsname
	fsname=re.sub("nobackupp","nbp", filesystem)	
#-- We need at least read or write
	if not (options.showread or options.showwrite or options.mdsstats):
		parser.error("We need at least '--read' or '--write --mds'")
		sys.exit(1)
#-- only show if we have both read and write
	elif not (options.showread and  options.showwrite):
		options.totalrw=False


	if options.units == "B":
		units=1
	elif options.units == "KB":
		units=1024
	elif options.units == "MB":
		units=1024 * 1024
	logging.debug("units = %i" % units)
#-- init lustre client class
	mylustre=lustre.Client(options.fsname,debug=False)

#-- Create pmdumptext config file --
	tempfh=tempfile.NamedTemporaryFile(delete=False)
	tempfh.write(mylustre.make_pcp_config(mds_stats,ost_stats))
	tempfh.flush()
	tempfh.close

#	def enqueue_output(out, queue):
#		print "out	=", out, queue
#		return out.readline()
#		for line in iter(out.readline, b''):
#			queue.put(line)
#		out.close()

#-- Start collection subprocess
	cmd="/usr/bin/pmdumptext -c %s -l -f %%s -t %i" % (tempfh.name,options.interval)
#	process = subprocess.Popen(cmd, bufsize=4096, shell=True, stdout=subprocess.PIPE)
	dataQueue = Queue()
	pmdThread = myThread(cmd,dataQueue)
	pmdThread.daemon=True

#	t = Thread(target=enqueue_output, args=(process, q))
#	t.daemon = True # thread dies with the program
#	t.start()
	try:
		pmdThread.start()
		pmdThread._started.wait()
	except:
		logging.error("Failed to start thread")
		sys.exit()
	
	atexit.register(pmdThread.killme)
	
	if options.debug:
		logging.debug("Start subprocess pid %d" % (process.pid))

	#output to files option	
	if options.outfilename:
		options.outfilename="STIME.%f" % now
		readfilename="%s/%s.read.o" % ( options.outdir, options.outfilename)
		writefilename="%s/%s.write.o" % ( options.outdir, options.outfilename)
		fh_read=open(readfilename, 'w')
		fh_write=open(writefilename, 'w')

#			self,options, ostlist, debug=False,output=sys.stdout):
	while True: #Wait for the header
		logging.debug("Searching for header")
#		if process.poll() != None:
#			logging.debug("process.poll! break")
#			break
#		try:
#			nextline = q.get_nowait()
		if dataQueue.qsize() > 0:
			try:
				nextline =dataQueue.get(False, .1)
			except:
				logging.error("Failed to read data from Queue")
				break
		else:
			continue
#			logging.critical("Error while reading nextline")
			
		if nextline == '':
			logging.debug("nextline '' break")
			break
		if re.search("Time", nextline):
			logging.debug("header found")
			header=parseheader(nextline)
			printosts=getprintosts(options,osts,mylustre)
			logging.debug("printosts = %s", printosts)
			break
	
	myoutput=output(options, printosts, debug=False,output=sys.stdout)
	if options.plots:
		if options.showread:
			plotReads.size(len(printosts), options.xlimit)
			plotReads.set_ylabels(printosts)
			plotReads.set_zrange(max=1, min=0)
		if options.showwrite:
			plotWrites.size(len(printosts), options.xlimit)
			plotWrites.set_ylabels(printosts)
			plotWrites.set_zrange(max=1, min=0)
		if options.totalrw:
			plottotals.size(len(printosts), options.xlimit)
			plottotals.set_ylabels(printosts)
			plottotals.set_zrange(max=157286400/units, min=0)
		if options.mdsstats and options.plots:
			#init X and Yplotwidth=60
			x=np.arange(0,plotwidth,1)
			y=np.zeros(plotwidth)
	#mds_stats=["open", "close", "link", "unlink", "mkdir", "rmdir", "rename", "statfs" ]
			stripchartsubplots=["open_close", "link_unlink", "mkdir_rmdir", "rename_statfs"]
			for subplot in stripchartsubplots:
				mdsplot.add_subplot(subplot, 7,10,0, 1000)
				#mdsplot.set_xrange(0,59,1) # Set time scale for plot
				for metric in subplot.split("_"):
					X[metric]=[]
					Y[metric]=[]
					if options.debug:
						print "addeding subplot[ %s ] metric [ %s ]" % (subplot, metric)
					mdsplot.add_plot(subplot, metric, x, y, xlabels=xlabels)
#			mdsplot.show()
	if not options.sortme and not options.nooutput:
		myoutput.header()				
	
	while True:
		totalostsr=0
		totalostsw=0	
		totalrw={}
		totalosts=0
		if dataQueue.qsize() > 0:
			try:
				nextline =dataQueue.get(False)
			except:
				logging.error("Failed to read data from Queue")
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
		if timestep0:
			olddata=copy.deepcopy(data)
			timestep0=False
			continue
		else:
			timestep=time.strftime("%m/%d %H:%M:%S", time.localtime(int(data['Time']['Time'])))
			# only look for ost [0, 1, 2] reads
			# data[0]['read_bytes']
			for metric in ["read_bytes", "write_bytes"]:
				rates[metric]={}
				for ost in printosts:
					rates[metric][ost]= ( float(data[ost][metric]) ) / units # byts -> MB/sec
		# Calculate totals	
		for ost in printosts:
			if options.showread and options.showwrite:
				totalrw[ost] = rates["read_bytes"][ost] + rates["write_bytes"][ost]
				totalostsr +=rates["read_bytes"][ost]
				totalostsw +=rates["write_bytes"][ost]
			elif options.showread:
				totalrw[ost] = rates["read_bytes"][ost]
				totalostsr +=rates["read_bytes"][ost]
			else:
				totalrw[ost] = rates["write_bytes"][ost]
				totalostsw +=rates["write_bytes"][ost]
			
		if options.showread and options.showwrite:
			totalostsrw = totalostsr + totalostsw
		elif options.showread:
			totalosts = totalostsr
		elif options.showwrite:
			totalosts = totalostsw
		if options.sortme or options.plots:
			printosts.sort()
#--- print header
		if not options.nooutput and (options.sortme or outputcounter > options.repeatheader and options.repeatheader != 0):
			outputcounter=0
			myoutput.header()				
		if options.showread:
			if not options.nooutput:
				myoutput.data(rates['read_bytes'], type='R', time=timestep, total=totalostsr )
			if options.plots:
				plotReads.draw(rates['read_bytes'], timestep,xlabel="Time", ylabel="OSTs")
		if options.showwrite: 
			if not options.nooutput:
				myoutput.data(rates['write_bytes'], type='W', time=timestep, total=totalostsw )
			if options.plots:
				plotWrites.draw(rates['write_bytes'], timestep,xlabel="Time", ylabel="OSTs")
		if options.totalrw:
			if not options.nooutput:
				myoutput.data(totalrw, type='T', time=timestep, total=totalostsrw )
			if options.plots:
				plottotals.draw(totalrw, timestep,xlabel="Time", ylabel="OSTs")
		if options.mdsstats:
			if not options.nooutput:
				mdstattotals=np.array(data[fsname].values()).astype(np.float).sum()
				myoutput.data(data[fsname], type='MDS', time=timestep, total= mdstattotals)
			if options.plots:
				xlabels.append(timestep)
				if (len(xlabels)) > options.xlimit:
					xlabels.pop(0)
				for metric in mds_stats: #mds_stats=["open", "close", "link", "unlink", "mkdir", "rmdir", "rename", "statfs" ]
					X[metric].append(outputcounter)
					Y[metric].append(float(data[fsname][metric]))
					if len(X[metric]) > options.xlimit:
						X[metric].pop(0)
						Y[metric].pop(0)
#					print metric, X[metric], Y[metric]
					if metric == "statfs":
						mdsplot.update_plot(metric,X[metric],Y[metric],xlabels=xlabels,showXaxis=True)
					else:
						mdsplot.update_plot(metric,X[metric],Y[metric],xlabels=xlabels,showXaxis=False)
				
				mdsplot.show()	
		outputcounter +=1
		
	sys.stdout.write(output.ENDC)				
	remove(tempfh.name)	


if __name__ == '__main__':
	main(sys.argv)
