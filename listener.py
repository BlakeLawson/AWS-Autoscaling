'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
' Script to listen for messages on a specific port. Messages should
' take the format "password command additional_arguments". The Listener
' should always respond in the format 
' "request_confirmation additional_arguments" (NOTE: the listener
' response still needs to be properly implemented)
' 
' ctr+f for "TODO"
'
' Dependencies: boto, psutil, subprocess32
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
import argparse
import boto.ec2
import logging
import psutil
import socket
import subprocess32
import sys
import urllib2

# listener object used to manage connections
class Listener:
	# Confirmation code to send in response to valid messages
	CONFIRMATION = '0'
	# Code to send in response to messages with valid authentication but
	# invalid message
	REJECTION = '1'

	# Dictionary of terminal commands corresponding to messages sent from controller.py
	'''
	Currently, these commands exist for to test interactions between the listener
	and the controller. Right now, I am thinking that the additional arguments for
	the 'run' command (i.e. the key values for this dictionary) should correspond to
	values listed in the 'tasks' tag on AWS (every 'parent' instance should have one
	of these.

	TODO: Whenever it is time to start using this script in the production environment, 
	      these commands will need to be updated.
	'''
	TERMINAL_COMMANDS = {
		'process1' : 'df -h',
		'process2' : 'ls -a /',
		'process3' : 'touch /home/ubuntu/test_file.txt',
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
		# Main loop
		while True:
			# Wait for a connection
			if self.verbose:
				print "Listening to %s on port %s." % self.socket.getsockname()
			conn, addr = self.socket.accept()
			if self.verbose:
				print "Recieved connection from %s." % str(addr)

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
					error = ""
					for i in range(2, len(data)):
						try:
							if self.verbose:
								print "Executing command: %s" % self.TERMINAL_COMMANDS[ data[i] ]
							subprocess32.check_call(self.TERMINAL_COMMANDS[ data[i] ].split())
						except:
							error = error + " " + str(sys.exc_info()[0])
							reply = self.REJECTION
						
					conn.sendall(reply + error)
					conn.close()

				elif command == 'end':
					# Terminate this instance
					result = self.shut_down()
					if result == 0:
						conn.sendall(self.CONFIRMATION)
					else:
						conn.sendall(self.REJECTION + " " + result)
					conn.close()

				else:
					if self.verbose:
						print "Invalid command: %s" % command
					# Invalid command
					conn.sendall(self.REJECTION)
					conn.close()
			else:
				# Message invalid. Do not respond
				pass

	# Function to turn off this AWS Instance
	# TODO: Add a few lines to kill Celery and other tasks that may be running
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
			return 0
		except:
			return str(sys.exc_info()[0])

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

if __name__=="__main__":
	main(sys.argv[1:])
