'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
' Script to listen for messages on a specific port
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
import socket

def listen():
	# Set up socket
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.bind(("localhost", 9989))
	s.listen(1)

	listen = True
	print "Listening on port 9989"
	while listen:
		# Wait for a connection
		conn, addr = s.accept()
		print "Recieved data from %s" % str(addr)

		# Recieve up to 2048 bytes of data
		data = conn.recv(2048)
		print "Data says '%s'" % data

		if data == "end":
			print "Turning off listener"
			conn.close()
			listen = False

if __name__=="__main__":
	listen()