'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
' Script to manage interactions with AWS instances and control auto
' scaling.
'
' ctr+f for "TODO"
'
' Dependencies: boto
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
import argparse
import boto.ec2
import logging
import socket
import sys
import time

'''
This class is meant to handle all interactions between the monitoring
server (the server running this script) and all other AWS workers.
'''
class Controller:
	# Dictionary containing lists of aws instance objects for all worker made by
	# autoscaling ec2 instances (i.e. made by this script). 
	auto_instances = { 'running':[], 'starting':[], 'ending':[] }

	# List of all primary instances that serve as parents to auto
	# instances.
	parent_instances = [] 

	# Limit to the number of auto instances
	# TODO: Don't hardcode this. Let the client set it in the constructor
	INST_LIMIT = 5

	# Authentication key that the Listener looks for
	LISTENER_PASSWORD = 'real'

	# Dictionary of all standard messages to workers
	# The 'status' and 'kill' message can be used as is, but the 'run'
	# message should be concatenated with the command you want the
	# listener to run. 
	# TODO: Make commands for each possible run command so that the client 
	#       does not have to do any concatenation later on
	MESSAGE = {
		'status' : LISTENER_PASSWORD + ' status',
		'kill' : LISTENER_PASSWORD + ' end',
		'run' : LISTENER_PASSWORD + ' run ',
	}

	# Port to use when connecting to workers. MAKE SURE IT IS THE SAME
	# AS THE PORT BEING USED IN Listener CLASS IN listener.py!!!! Also,
	# if you change this port, make sure that you change the AWS
	# security group to allow TCP access on the new port.
	SOCKET_PORT = 9989

	def __init__(self, verbose=False, ami="ami-100cd978", instance_type="t2.micro", key_name="blake", security_groups=["launch-wizard-1"]):
		'''
		Initialize member variables
		'''
		# Enable log statements throughout operation
		self.verbose = verbose
		# The image to be used when making new workers
		self.ami = ami
		# The type of AWS instance to use
		self.instance_type = instance_type
		# The .pem key that will be requred to ssh into new workers
		self.key_name = key_name
		# The security groups to be applied to new workers
		self.security_groups = security_groups

		# Connect to AWS
		self.aws_conn = boto.ec2.connect_to_region("us-east-1")
		self.update()

	# Add a new worker to assist a given instance
	def add_worker(self, inst):
		# Check that there are fewer auto instances than the limit
		num_auto_inst = len(self.auto_instances['starting']) + len(self.auto_instances['running']) + len(self.auto_instances['ending'])
		if num_auto_inst < self.INST_LIMIT:
			# Initialize new server
			reservation = self.aws_conn.run_instances(
				image_id=self.ami,
				instance_type=self.instance_type,
				key_name=self.key_name,
				security_groups=self.security_groups
			)

			# Add the new instance to the list of known instances
			for new_inst in reservation.instances:
				# Adding the 'parent' attribute to AWS Instance object in order to 
				# make a tag containing parent information later on. IT IS NOT 
				# SAFE TO ASSUME THAT ALL AUTO INSTANCES HAVE THIS ATTRIBUTE, so 
				# code accordingly.
				new_inst.parent = inst
				self.auto_instances['starting'].append(new_inst)

			if self.verbose:
				print "Added worker to help %s" % inst.id
		else:
			# There are too many auto_instances already
			if self.verbose:
				print "Could not add worker to help %s. Too many workers already." % inst.id

	# Parse data returned by listener to see if it responded with an 'ok' status
	# TODO: This method was created with the expectation that the listener will
	#		eventually send more complicated responses that include more details
	#		about why something went wrong with the message (e.g. invalid 
	# 		authentication, invalid command, something broke while executing a 
	# 		valid command, etc.)
	def confirm_receipt(self, data):
		if data[0] == '0':
			return True
		else:
			return False

	# Connect to a given AWS EC2 instance. Returns a socket object
	def connect_to_inst(self, inst):
		address = inst.public_dns_name
		port = self.SOCKET_PORT
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.settimeout(10.0)
		try:
			if self.verbose:
				print "Connecting to %s" % inst
			s.connect((address, port))
			return s
		except:
			if self.verbose:
				print "Connection to %s (%s:%s) failed." % (inst, address, port)
			return None

	# Force termination of a given AWS instance
	def force_terminate(self, inst):
		# Ensure given instance exists
		instance_found = False
		reservations = self.aws_conn.get_all_reservations()
		for res in reservations:
			for instance in res.instances:
				if instance.id == inst.id:
					instance_found = True
					break

		# Terminate instance
		if instance_found:
			self.aws_conn.terminate_instances(instance_ids=[inst.id])
			if self.verbose:
				print "Forcing termination of %s" % inst.id

	# Return list of children that have been created to help the given instance
	def get_children(self, inst):
		children = []
		for group in self.auto_instances:
			for i in self.auto_instances[group]:
				if inst.id in i.tags.values():
					children.append(i)

		return children

	# Shut down an auto instance for a given parent instance
	def remove_worker(self, parent_inst):
		children = self.get_children(parent_inst)
		
		# Select the first child in the list of children for the given parent
		# TODO: Use a more rigorous technique for selecting the worker to kill
		worker = None
		for child in children:
			if child in self.auto_instances['running']:
				worker = child
			break

		if worker:
			# Connect to worker
			conn = self.connect_to_inst(worker)
			if conn:
				conn.sendall(self.MESSAGE['kill'])
				response = conn.recv(2048).split()
				if self.confirm_receipt(response):
					if self.verbose:
						print "Killing worker %s for %s" % (worker.id, parent_inst.id)
				else:
					if self.verbose:
						print "Problem in message to %s: %s" % (worker.id, response)
				conn.close()
			else:
				# Could not connect to worker
				pass

			# Update lists
			self.auto_instances['running'].remove(worker)
			self.auto_instances['ending'].append(worker)

			# Record the time at which the worker was told to start shutting down. It should be
			# noted that worker is an AWS Instance object, and I'm adding a new attribute to it.
			worker.stop_time = time.time()
		else:
			# There must not be any running children to this parent_inst
			if self.verbose:
				print "Could not kill worker for %s. No workers exist." % parent_inst.id

	# Run scripts and perform other tasks with an auto instance that has just started running
	def start_up(self, inst):
		# Add a tag
		# It should be noted that 'inst' is an AWS Instance object, and 'parent' is an attribute that
		# this script should have added to the Instance when it was first created.
		try:
			tag = { "Name":"Auto Instance", "Type":"Child", "Parent":inst.parent.id }
			# Get processes for this instance to run
			tasks = inst.parent.tags['Tasks']
		except AttributeError:
			# TODO: I'm not actually sure about what should be done in this case. For now I'm going
			# to do nothing, but in the future it might make sense to terminate the instance because
			# it won't be possible to determine which tasks to run.
			return False
		
		inst.add_tags(tag)

		# Connect to instance and send it tasks to run
		# TODO: Actually do something here. Maybe make a method
		#       to parse the parent tasks and make them into real
		#       commands (this should probably be done in Listener)
		conn = self.connect_to_inst(inst)
		if conn:
			message = self.MESSAGE['run'] + tasks
			conn.sendall(message)
			# For now, the auto instance is just going to return the message it recieves
			data = conn.recv(2048)
			if self.confirm_receipt(data):
				if self.verbose:
					print "Message recieved from %s: %s" % (inst.id, data)
			else:
				if self.verbose:
					print "start_up(): Something went wrong in message to %s" % inst.id
				return False

			conn.close()
			return True
		else:
			# Could not connect to instance
			return False

	# Update list of known instances
	# TODO: The AWS Instance object also has a method called update(). Consider
	# 		refactoring to avoid confusion
	def update(self):
		reserves = self.aws_conn.get_all_reservations()
		for res in reserves:
			for inst in res.instances:
				try:
					# Case: Running parent instance
					if inst.tags['Type'] == "Parent" and not any(inst.id == i.id for i in self.parent_instances):
						if not inst.update() == "terminated":
							self.parent_instances.append(inst)
					# Case: Auto instance
					elif inst.tags['Type'] == "Child":
						state = inst.update()
						# Case: Starting auto instance
						if state == "pending" and not any(inst.id == i.id for i in self.auto_instances['starting']):
							self.auto_instances['starting'].append(inst)
						# Case: Running auto instance
						elif state == "running" and not any(inst.id == i.id for i in self.auto_instances['running']):
							self.auto_instances['running'].append(inst)
						# Case: Ending auto instance
						elif state == "shutting-down" and not any(inst.id == i.id for i in self.auto_instances['ending']):
							self.auto_instances['ending'].append(inst)
						# Case: Terminated or stopped auto_instance
						else:
							pass
					else:
						# The instance is not something that should be auto-scaled (like a web server)
						pass
				except KeyError:
					# The instance likely hasn't had tags assigned yet
					continue

# This is where everything happens. It basically calls methods in the Controller class
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
def monitor(controller):
	# Get all existing AWS instances
	controller.update()

	# Main loop
	while True:
		# Check on workers that were previously booting up
		if controller.verbose and controller.auto_instances['starting']:
			print "monitor(): Checking whether new workers have finished starting"
		for inst in controller.auto_instances['starting']:
			status = inst.update()
			if status == 'running':
				####################################################
				### Call function to start running tasks/scripts ###
				####################################################
				if controller.start_up(inst):
					if controller.verbose:
						print "%s finished booting" % inst
					controller.auto_instances['running'].append(inst)
					controller.auto_instances['starting'].remove(inst)
				else:
					# Could not connect to instance. It must not have started
					# listening yet
					continue

			elif status == 'pending':
				# The instance is still starting up
				pass
			else:
				# If this case occurs, I have no idea what's going on. Its
				# possible that the script couldn't connect to the instance.
				print "Strange InstanceState for %s" % inst

		# Check on parent instances
		if controller.verbose and controller.parent_instances:
			print "monitor(): Checking parent instances"
		for inst in controller.parent_instances:
			# Connect to instance
			conn = controller.connect_to_inst(inst)
			if conn:
				message = controller.MESSAGE["status"]
				conn.sendall(message)
			
				# Get the instance's response
				# TODO: Add check for 'OK' from Listener
				data = conn.recv(2048).split()

				if controller.confirm_receipt(data):
					CPU = float(data[1])
					disk = float(data[2])
					mem = float(data[3])

					if controller.verbose:
						print "%s: CPU=%s\tDisk Usage=%s\tMemory Usage=%s" % (inst.id, str(CPU) + "%", str(disk) + "%", str(mem) + "%")

					########################################
					### Condiditions for new worker here ###
					########################################
					'''
					TODO: Make conditions for creating/removing new workers

					It might be good to make some kind of data structure to keep
					track of past values for each primary worker. Basically
					something that either averages past values or something that
					keeps a tally of number of times a condition is > 85%.
					'''
					# Condition for making a new instance
					if CPU > 90:  
						controller.add_worker(inst)
					# Conditions for killing an existing instance
					elif CPU < 5 and controller.get_children(inst):
						controller.remove_worker(inst)
				else:
					# Something went wrong with message
					if controller.verbose:
						print "Something went wrong with message to %s" % inst.id

				conn.close()
			else:
				# Could not connect to instance
				continue

		# Check on running auto instances
		'''
		At this point, I don't have a good reason to check on the auto instance.
		Could create worker groups (i.e. a parent and its auto instances) and track
		average status across the group to determine whether more workers are
		needed. Probably won't be able to do this effectively until testing with
		the actual workers.
		'''
		if controller.verbose and controller.auto_instances['running']:
			print "monitor(): Checking auto instances"
		for inst in controller.auto_instances['running']:
			# Connect to instance
			conn = controller.connect_to_inst(inst)
			if conn:
				message = controller.MESSAGE["status"]
				conn.sendall(message)

				# Get the instance's response
				data = conn.recv(2048).split()
				if controller.confirm_receipt(data):
					CPU = float(data[1])
					disk = float(data[2])
					mem = float(data[3])
				else:
					# Something went wrong with the message
					if controller.verbose:
						print "Something went wrong with message to %s" % inst.id
				conn.close()
			else:
				# Could not connect to instance
				if controller.verbose:
					print "Could not connect to instance %s" % inst.id

		# Check on instances that are shutting down
		if controller.verbose and controller.auto_instances['ending']:
			print "monitor(): Checking on instances in the process of shutting down"
		for inst in controller.auto_instances['ending']:
			# Add a new attribute to the AWS Instance object inst to record the approximate time 
			# at which the instance was told to start shutting down. This attribute should have
			# been added when the instance was first added to the 'ending' list.
			if controller.verbose:
				print "Checking on %s" % inst.id

			try:
				inst.stop_time
			except AttributeError:
				inst.stop_time = time.time()

			# Get instance status
			status = inst.update()
			if status == "terminated":
				controller.auto_instances['ending'].remove(inst)
			else:
				# Check how long ago the instace was told to terminate
				current_time = time.time()
				time_difference_seconds = current_time - inst.stop_time
				time_difference_minutes = time_difference_seconds / 60

				# If longer than two minutes, force termination
				# TODO: Refine condition for force termination
				if time_difference_minutes > 2:
					controller.force_terminate(inst)
					controller.auto_instances['ending'].remove(inst)

# Manage command line input
def main(argv):
	# Set up parser
	parser = argparse.ArgumentParser()
	parser.add_argument('-i', '--ami', help='AWS AMI to use when making new workers', default='ami-100cd978')
	parser.add_argument('-k', '--key', help='name of the AWS key pair to use. You will need the corresponding .pem key to ssh into the new workers', default='blake')
	parser.add_argument('-s', '--security', help='group name of the AWS security group to use for the new instances', default='launch-wizard-1')
	parser.add_argument('-t', '--instance_type', help='The type of AWS instance to use (e.g. "t2.micro")', default='t2.micro')
	parser.add_argument('-v', '--verbose', help='output what is going on', action='store_true')

	'''
	TODO: Rather than using hard coded default options for the parser, it would be 
	better to make a function that will select the AMI and security groups 
	from AWS. This could be done by using a standard naming convention for 
	auto-scaling-related AMI/security groups and then checking which AMI/security
	groups are the newest or have been labeled as the desired choice for this program 
	to use.

	For example, name all auto-scaling AMI 'auto_scale_1', 'auto_scale_2', etc. Then, 
	use boto to get a list of all available AMI and use simple string parsing to 
	select the highest version.
	'''

	# Read command-line input
	args = parser.parse_args(argv)

	c = Controller(verbose=args.verbose, ami=args.ami, instance_type=args.instance_type, key_name=args.key, security_groups=[args.security])
	monitor(c)

if __name__=="__main__":
	main(sys.argv[1:])
