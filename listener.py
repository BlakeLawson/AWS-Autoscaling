'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
' Script to listen for messages on a specific port. Messages should
' take the format "password command additional_arguments". The Listener
' should always respond in the format 
' "request_confirmation additional_arguments" (NOTE: the listener
' response still needs to be properly implemented)
' 
' ctr+f for "TODO"
'
' Dependencies: boto, psutil
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
import argparse
import boto.ec2
import logging
import os
import psutil
import socket
import sys
import urllib2

# listener object used to manage connections
class Listener:
	# Confirmation code to send in response to valid messages
	CONFIRMATION = '1'
	# Code to send in response to messages with valid authentication but
	# invalid message
	REJECTION = '0'

	# Dictionary of terminal commands corresponding to messages sent from controller.py
	TERMINAL_COMMANDS = {
		'''
		Currently, these commands exist for to test interactions between the listener
		and the controller. Right now, I am thinking that the additional arguments for
		the 'run' command (i.e. the key values for this dictionary) should correspond to
		values listed in the 'tasks' tag on AWS (every 'parent' instance should have one
		of these.

		TODO: Whenever it is time to start using this script in the production environment, 
		      these commands will need to be updated.
		'''

		'process1' : 'echo hello world',
		'process2' : 'ls -a /',
		'process3' : 'touch test_file.txt',
	}

	def __init__(self, address=socket.gethostname(), port=9989, verbose=False):
		'''
		If you change the port, make sure you change the AWS security group to
		allow TCP access on that port.
		'''

		# Initialize member variables
		self.verbose = verbose

		# Set up socket
		self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.socket.bind((address, port))
		self.socket.listen(5)	

	# Check authentication key to sender
	# TODO: Make this legitimate
	def authenticate(self, key):
		if self.verbose:
			print "Authenticating connection"

		if key == "real":
			if self.verbose:
				print "Connection accepted"
			return True
		else:
			if self.verbose:
				print "Connection denied"
			return False

	# Listen for messages from controller script and execute commands
	# TODO: Consider making this method a method that stands on its own
	#       outside of the Listener class -- similar to the monitor 
	#       function in controller.py
	def listen(self):
		if self.verbose:
			print "Listening to %s on port %s." % self.socket.getsockname()

		# Main loop
		while True:
			# Wait for a connection
			conn, addr = self.socket.accept()
			if self.verbose:
				print "Recieved data from %s." % str(addr)

			# Recieve up to 2048 bytes of data (2048 chosen arbitrarily)
			data = conn.recv(2048).split()

			'''
			As explained in the header, data[0] is the authentication password,
			data[1] is the command for the listener, and data[2] (and all elements
			with index > 2) are additional arguments for the command.
			'''

			try:
				auth_key = data[0]
				command = data[1]
			except:
				# Invalid command format
				continue

			# Check authentication code
			if self.authenticate(auth_key):
				# Perform given command
				if command == 'status':
					# Return system information
					CPU, disk, mem = self.sys_check()

					# Send reply
					reply = " ".join((self.CONFIRMATION, str(CPU), str(disk), str(mem)))
					conn.sendall(reply)
					conn.close()
				
				elif command == 'run':
					# Run the given command/commands
					reply = self.CONFIRMATION
					for i in range(2, len(data) - 1):
						try:
							os.system(self.TERMINAL_COMMANDS[ data[i] ])
						except:
							reply = self.REJECTION
						
					conn.sendall(reply)
					conn.close()

				elif command == 'end':
					# Terminate this instance
					conn.sendall(self.CONFIRMATION)
					conn.close()
					self.shut_down()
					break

				else:
					# Invalid command
					conn.sendall(self.REJECTION)
					conn.close()
			else:
				# Message invalid. Do not respond
				pass



	########################################################################
	###################  DELETE THIS METHOD AFTER DEMO #####################
	########################################################################

	# Print any inbound messages and take no other actions. Used
	# for testing.
	def print_inbound(self):
		if self.verbose:
			print "Listening to %s on port %s." % self.socket.getsockname()
		
		listen = True
		while listen:
			# Wait for a connection
			conn, addr = self.socket.accept()
			if self.verbose:
				print "Recieved data from %s" % str(addr)

			# Recieve up to 2048 bytes of data (2048 chosen arbitrarily)
			data = conn.recv(2048)

			# Print inbound message
			print data
			# logging.info(str(data))

			if data == "end":
				conn.close()
				listen = False
				if self.verbose:
					print "Turning off listener"

	########################################################################
	########################################################################

	########################################################################
	###################  DELETE THIS METHOD AFTER DEMO #####################
	########################################################################

	# Return system information to sender
	def reply_to_inbound(self):
		if self.verbose:
			print "Listening to %s on port %s." % self.socket.getsockname()

		listen = True
		while listen:
			# Wait for a connection
			conn, addr = self.socket.accept()
			if self.verbose:
				print "Recieved data from %s" % str(addr)

			# Recieve up to 2048 bytes of data (2048 chosen arbitrarily)
			data = conn.recv(2048).split()

			# Take action if valid message
			if self.authenticate(data[0]):		
				if data[1] == "end":
					conn.close()
					listen = False
					if self.verbose:
						print "Turning off listener"
					break
				elif data[1] == "status":
					# Get information about current system
					CPU, disk, mem = self.sys_check()
					if self.verbose:
						print "Getting system status. CPU: %s Disk Usage: %s Memory Usage: %s" % (str(CPU) + "%", str(disk) + "%", str(mem) + "%")

					# Send reply
					reply = str(CPU) + " " + str(disk) + " " + str(mem)
					conn.sendall(reply)
					if self.verbose:
						print "Sent response"

					conn.close()
					if self.verbose:
						print "Closed connection"
				else:
					print "Recieved message: " + data

	########################################################################
	########################################################################

	# Function to turn off this AWS Instance
	# TODO: Add a few lines to kill existing Celery instances and other tasks
	def shut_down(self):
		# Look up this server's public ip address
		ip = urllib2.urlopen('http://ip.42.pl/raw').read()

		# Connect to this AWS instance
		conn = boto.ec2.connect_to_region("us-east-1")
		reserves = conn.get_all_reservations()
		for res in reserves:
			for instance in res.instances:
				if ip == instance.ip_address:
					inst = instance
					break

		# Kill this instance
		try:
			conn.terminate_instances(instance_ids=[inst.id])
		except:
			print "Could not find this instance on AWS."

	# Check system CPU, disk usage, and memory usage
	def sys_check(self):
		# Get CPU
		CPU = psutil.cpu_percent(interval=1)
		# Get disk usage
		disk = psutil.disk_usage('/').percent
		# Get memory usage
		mem = psutil.virtual_memory().percent

		return (CPU, disk, mem)

# Manage command line input
def main(argv):
	# Set up parser
	parser = argparse.ArgumentParser()
	parser.add_argument('-d', '--debug', help='run in debug mode', action='store_true')
	parser.add_argument('-v', '--verbose', help='output what is going on', action='store_true')

	# Read command line input
	args = parser.parse_args(argv)

	if args.debug:
		l = Listener(address='localhost', verbose=True)
	elif args.verbose:
		l = Listener(verbose=True)
	else:
		l = Listener()

	l.listen()

'''
########################################################################
###################  DELETE THIS METHOD AFTER DEMO #####################
########################################################################

# Old main
def main(argv):
	if argv:
		if len(argv) > 1:
			try:
				# Use verbose for now
				l = Listener(address=argv[1], port=int(argv[2]), verbose=True)
			except:
				print "Invalid arguments. Use format 'python listener.py mode address port'"
				return None
		else:
			# Verbose for now
			l = Listener(address="localhost", verbose=True)
			print "No address or port supplied. Using default settings: address=%s port=%s" % l.socket.getsockname()

		if argv[0] == "passive":
			l.print_inbound()
		elif argv[0] == "active":
			l.reply_to_inbound()
	else:
		print "Invalid arguments."

############################################################################
############################################################################
'''

if __name__=="__main__":
	main(sys.argv[1:])
