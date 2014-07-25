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
import boto.ec2
import logging
import psutil
import socket
import sys

# listener object used to manage connections
class Listener:
	def __init__(self, address=socket.gethostname(), port=9989, verbose=False):
		self.verbose = verbose

		# Set up socket
		self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.socket.bind((address, port))
		self.socket.listen(5)	

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

	# Check system CPU, disk usage, and memory usage
	def sys_check(self):
		# Get CPU
		CPU = psutil.cpu_percent(interval=1)
		# Get disk usage
		disk = psutil.disk_usage('/').percent
		# Get memory usage
		mem = psutil.virtual_memory().percent

		return (CPU, disk, mem)


'''
NEED TO MAKE NEW main
'''

# Manage command line input
# TODO: set up argparse
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

if __name__=="__main__":
	main(sys.argv[1:])
