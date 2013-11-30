'''
Created on Mar 28, 2012

@author: mhanafi
'''
import re
import os
import sys
import logging
import pdb
from socket import gethostbyaddr 

def nslookup(ip):
	try: 
		output = gethostbyaddr(ip)
		return output[0]
	except: 
		output = "not found" 
		return output


class Client():
	'''
	Client part of lib
	'''
	
	def __init__(self, _filesystem,debug=False):
		'''
		Constructor
		pass in MDS and OST stats of string array.
		Filesystem format: /nobackuppxx or nobackuppxx
		'''
		if re.match("/", _filesystem[0]):
			#Strip off /
			self.filesystem=_filesystem[1:]
			self.mntpoint=_filesystem
		else:
			self.filesystem=_filesystem
			self.mntpoint="/" + _filesystem
		
		self.fsname=re.sub("nobackupp","nbp", self.filesystem)
		self.mds=self.get_mds()
		self.log=logging
		if debug:
			self.log.basicConfig(format='%(filename)s %(lineno)s %(levelname)s  %(message)s', level=logging.DEBUG)
			self.debug=True
		else:
			self.log.basicConfig(format='%(filename)s %(lineno)s %(levelname)s  %(message)s', level=logging.INFO)

		self.log.debug("Filesystem = %s mntpoint = %s fsname = %s" % (self.filesystem,self.mntpoint,self.fsname))
		self.osts,self.oss=self.get_osts()			

	def is_mounted(self):  
		'''
			Return True if mounted
			Set:  
				self.mdsIp
				serf.mdsname
				self.mntpoint (/nbpx)
		'''
		try:
			lines=open("/proc/mounts", "r").readlines()
		except IOError:
			sys.stderr.write("couldn't read /proc/mounts")  
			sys.exit(255)
				
		self.mdsip=""
		self.mntpoint=""	
		for line in lines:
			if re.search("lustre", line) and re.search(self.filesystem,line):
				#What we get some thing like this
				#10.151.25.163@o2ib:/nbp6 /nobackupp6 lustre rw,nosuid,nodev,relatime,flock,acl 0 0
				[self.mdsip, junk, self.mntpoint] = re.sub("@|:|/", " ", line).split()[:3] #remove @ and : split by space get 0,1,2
				break
		if len(self.mdsip) == 0:
			sys.stderr.write("Didn't find %s mounted" % self.filesystem)
			return False
		return True
		
	def get_mds(self):
		'''
			return mdsid
		'''
		_basedir="/proc/fs/lustre/mdc"
		ret={}					
		# Find MdsId
		if not os.path.exists(_basedir):
			print "Error: %s doesn't exists" % _basedir
			return None
		for _i in os.listdir(_basedir):
			if re.search(self.fsname,_i):
#				self.log.debug("%s" % _i)
				ret["id"]=_i.split('-')[3]
				ret["status"]=open(_basedir+"/"+_i+"/state").readline().strip().split(":")[1]
				ret["nid"]=_nid=open(_basedir+"/"+_i+"/mds_conn_uuid").readline().strip()
				ret["ip"]=_ip=_nid.split("@")[0]
				ret["hostname"]= gethostbyaddr(_ip)[0].split(".")[0]
				break
		return ret
	def get_oss_name(self, _osc):
		_file="/proc/fs/lustre/osc/%s/ost_conn_uuid" % _osc
		try:
			_ossip=open(_file, 'r').read().split("@")[0]
		except:
			sys.stderr.write( "Error: %s doesn't exists\n" % self.basedir)
			return  None
		return nslookup(_ossip).split(".")[0]
		
	def get_oss_name_list(self):
		_oss=[]	
		for _i in self.get_osc_list():
			 _ossname=self.get_oss_name(_i)
			 if _ossname not in _oss:
				_oss.append(self.get_oss_name(_i))
			
		return sorted(_oss)
	
	def get_osc_list(self):
		_osc=[]
		_basedir="/proc/fs/lustre/osc"
		if not os.path.exists(_basedir):
			sys.stderr.write( "Error: %s doesn't exists\n" % _basedir)
			return None
		for _i in os.listdir(_basedir): #get all the OST
			if re.search(self.mntpoint,_i):
				_osc.append(_i)
		return _osc
	
	def get_osts(self):
		'''
			Find all ost return dic ost[ost#]=ossname
		'''
		_basedir="/proc/fs/lustre/osc"
		_oss={}
		if not os.path.exists(_basedir):
			sys.stderr.write( "Error: %s doesn't exists\n" % _basedir)
			return None	
		_osts={}
		if len(self.fsname) == 0:
			sys.stderr.write("Error: fsname not set\n")
			sys.exit(255)
			
		_oscs=[]
		for _i in os.listdir(_basedir): #get all the OST
			if re.search(self.fsname,_i):
				_oscs.append(_i)
				
		for _i in _oscs: # get the OSS nid for each OST
			_ossname=self.get_oss_name(_i)
			_osthex=re.sub("OST", "", _i).split("-")[1]
			_ostdec=int(_osthex,16)
			_osts[_ostdec]=_ossname
			if _oss.has_key(_ossname):
				_oss[_ossname].append(_ostdec)
			else:
				_oss[_ossname]=[]
				_oss[_ossname].append(_ostdec)
		return _osts,_oss
   
	def get_oss_byost(self,_ost):
		return self.osts[_ost]

	def get_ost_byoss(self,_oss):
		if self.oss.has_key(_oss):
			return self.oss[_oss]	
		else:
			self.log.info("Oss [%s] not part of filesystem" % _oss)
			return None

	def make_pcp_config(self,_mdsstats,_oststats):
		_output=""
		_testout=self.is_mounted()
		for _stat in _mdsstats:
			_output = _output + "%s:lustre.mds.%s\n" % (self.mds['hostname'], _stat) #"$mds_name:lustre.mds.$stat\n"
		
		for _oss in self.get_oss_name_list():
			for _stat in _oststats:
				_output = _output + "%s:lustre.ost.%s\n" %  (_oss,_stat)
		return _output
	
		#for Stat in OstStats# + "$oss:lustre.ost.$stat\n" #;				Stats=Stats + "%s:lustre.ost.%s" % (self.))
