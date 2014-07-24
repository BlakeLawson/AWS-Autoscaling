'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
' Script to manage interactions with AWS instances and control auto
' scaling.
'
' ctr+f for "TODO"
'
' Dependencies: boto
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
import argparse
import boto
import boto.ec2
import socket
import sys
import time

class Controller:
	# Dictionary containing lists of ip addresses for all AWS made by
	# autoscaling ec2 instances. 
	'''
	All 'running' instances should be continuously
	monitored to identify when no longer needed or if additional
	instances need to be started. All 'starting' instance should be
	checked periodically to see when the instance finished booting. 
	At that point, the controller (this) should send the instance a 
	list of scripts/commands to run and then move the instance into 
	the 'running' list. 'ending' instances should shut themselves
	down, but the controller tracks them in case there is something
	preventing shut down. In which case, the controller overrides and
	terminates the instance remotely.
	''' 
	auto_instances = { 'running':[], 'starting':[], 'ending':[] }

	# List of all primary instances that serve as parents to auto
	# instances.
	base_instances = [] 

	# Dictionary of all standard messages to workers
	MESSAGE = {
		'status' : 'real status',
		'kill' : 'real end',
	}

	# Port to use when connecting to workers
	SOCKET_PORT = 9989

	def __init__(self, verbose=False, ami='ami-38b27a50'):
		# Initialize member variables
		self.verbose = verbose
		self.ami = ami

		# Connect to AWS
		self.aws_conn = boto.ec2.connect_to_region("us-east-1")
		self.update()

	# Connect to a given AWS EC2 instance. Returns a socket object
	def connect_to_inst(self, inst):
		address = isnt.public_dns_name
		port = self.SOCKET_PORT
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		try:
			if self.verbose:
				print "Connecting to %s" % inst
			s.connect((address, port))
			return s
		except:
			if self.verbose:
				print "Connection to %s (%s:%s) failed." % (inst, address, port)
			return None

	# Return list of children that have been created to help the given instance
	def get_children(self, inst):
		children = []
		for group in self.auto_instances:
			for i in self.auto_instances[group]:
				if str(inst) in i.tags.values():
					children.append(i)

		return children

	# This is where everything happens
	def monitor(self):
		self.update()

		# Main loop
		while True:
			# Check on workers that were previously booting up
			if self.verbose and self.auto_instances['starting']:
				print "Checking whether new workers have finished starting"
			for inst in self.auto_instances['starting']:
				status = inst.update()
				if status == 'running':
					if self.verbose:
						print "%s finished booting" % inst
					self.auto_instances['running'].append(inst)
					self.auto_instances['starting'].remove(inst)
					####################################################
					### Call function to start running tasks/scripts ###
					####################################################
				elif status != 'pending':
					# If this happens, might have lost connection to the instance
					print "Strange InstaceState for %s" % inst
				else:
					# Status must me 'pending' so keep it in this list
					pass

			# Check on base instances
			if self.verbose:
				print "Checking base instances"
			for inst in self.base_instances:
				# Connect to instance
				conn = self.connect_to_inst(inst)
				message = self.MESSAGE["status"]
				conn.sendall(message)
			
				data = conn.recv(2048).split()
				CPU = data[0]
				disk = data[1]
				mem = data[2]

				########################################
				### Condiditions for new server here ###
				########################################


				conn.close()

			# Check on running auto instances
			if self.verbose:
				print "Checking auto instances"
			for inst in self.auto_instances['running']:
				# Connect to instance
				conn = self.connect_to_inst(inst)
				message = self.MESSAGE["status"]
				conn.sendall(message)

				data = conn.recv(2048).split()
				CPU = data[0]
				disk = data[1]
				mem = data[2]

				conn.close()


	# Update list of known instances
	def update(self):
		reserves = self.aws_conn.get_all_reservations()
		for res in reserves:
			for inst in res.instances:
				# Case: Running base instance
				if inst.tags['Type'] == "Base" and inst not in self.base_instances:
					if not inst.update() == "terminated":
						self.base_instances.append(inst)
				# Case: Auto instance
				elif inst.tags['Type'] == "Child":
					state = inst.update()
					# Case: Starting auto instance
					if state == "pending" and inst not in self.auto_instances['starting']:
						self.auto_instances['starting'].append(inst)
					# Case: Running auto instance
					elif state == "running" and inst not in self.auto_instances['running']:
						self.auto_instances['running'].append(inst)
					# Case: Ending auto instance
					elif not state == "terminated":
						self.auto_instances['ending'].append(inst)
					# Case: Terminated auto_instance
					else:
						# state == terminated must be True so do not track this instance
						pass
				else:
					# The instance is not something that should be auto-scaled (like a web server)
					pass


# Send message to listener
# TODO: Add ability to specify address to send to. Maybe make into a class
def send_message(message=""):
	# Configure connection
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect(("localhost", 9989))
	print "Connected to %s" % str(s.getsockname())

	message = "real status"
	s.sendall(message)
	print "Message sent"

	print "Waiting for response"
	# conn, addr = s.accept()
	data = s.recv(2048)
	s.close()
	print "Recieved message: %s" % str(data)


'''
#######################
### MAKE A NEW MAIN ###
#######################
'''


# Manage command line input
# TODO: set up argparse
def main(argv):
	# Read command-line input
	if argv:
		if argv[0] == "add":
			new_server()

		elif argv[0] == "del":
			if argv[1]:
				kill_server(argv[1])
			else:
				print "Specify server to delete."

		elif argv[0] == "send":
			if argv[1]:
				send_message(argv[1])
			else:
				print "Specify message to send."

	else:
		print "Invalid arguments."

if __name__=="__main__":
	main(sys.argv[1:])

