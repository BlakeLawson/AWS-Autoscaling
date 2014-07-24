'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
' Script to listen for messages on a specific port
' 
' Dependencies: psutil
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
import logging
import psutil
import socket
import sys

# listener object used to manage connections
class listener:
	def __init__(self, address="localhost", port=9989):
		# Set up socket
		self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.socket.bind((address, port))
		self.socket.listen(1)	

	# Print any inbound messages and take no other actions. Used
	# for testing.
	def print_inbound(self, verbose=False):
		if verbose:
			self.verbose = True
			print "Listening to %s on port %s." % self.socket.getsockname()
		
		listen = True
		while listen:
			# Wait for a connection
			conn, addr = self.socket.accept()
			if verbose:
				print "Recieved data from %s" % str(addr)

			# Recieve up to 2048 bytes of data (2048 chosen arbitrarily)
			data = conn.recv(2048)

			# Print inbound message
			print data
			# logging.info(str(data))

			if data == "end":
				conn.close()
				listen = False
				if verbose:
					print "Turning off listener"

	# Return system information to sender
	def reply_to_inbound(self, verbose=False):
		if verbose:
			self.verbose = True
			print "Listening to %s on port %s." % self.socket.getsockname()

		listen = True
		while listen:
			# Wait for a connection
			conn, addr = self.socket.accept()
			if verbose:
				print "Recieved data from %s" % str(addr)

			# Recieve up to 2048 bytes of data (2048 chosen arbitrarily)
			data = conn.recv(2048).split()

			# Take action if valid message
			if self.authenticate(data[0]):		
				if data[1] == "end":
					conn.close()
					listen = False
					if verbose:
						print "Turning off listener"
					break
				elif data[1] == "status":
					# Get information about current system
					CPU, disk, mem = self.sys_check()
					if verbose:
						print "Getting system status. CPU: %s Disk Usage: %s Memory Usage: %s" % (str(CPU) + "%", str(disk) + "%", str(mem) + "%")

					# Send reply
					reply = str(CPU) + str(disk) + str(mem)
					conn.sendall(reply)
					if verbose:
						print "Sent response"

					conn.close()
					if verbose:
						print "Closed connection"
				else:
					pass

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

# Manage command line input
def main(argv):
	if argv:
		if len(argv) > 1:
			try:
				l = listener(argv[1], int(argv[2]))
			except:
				print "Invalid arguments. Use format 'python listener.py mode address port'"
				return None
		else:
			l = listener()
			print "No address or port supplied. Using default settings: address=%s port=%s" % l.socket.getsockname()

		if argv[0] == "passive":
			# Use verbose setting for now
			l.print_inbound(verbose=True)
		elif argv[0] == "active":
			l.reply_to_inbound(verbose=True)
	else:
		print "Invalid arguments."

if __name__=="__main__":
	main(sys.argv[1:])
	