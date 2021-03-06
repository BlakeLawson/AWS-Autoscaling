'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
' Script meant to test basic interactions with Amazon AWS using boto.
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
import argparse
import boto
import boto.ec2
import socket
import subprocess32
import sys
import time

# Spin up a new server
# TODO: Add pass variables to set image and number of instances, and
#       include a verbose option to suppress output.
def new_server():
	# Connect to AWS
	print "Making connection"
	conn = boto.ec2.connect_to_region("us-east-1")

	# Initialize the new server
	print "Making a new instance"
	reservation = conn.run_instances(
				image_id="ami-38b27a50", 
				instance_type="t2.micro",
				key_name="blake",
				security_groups=["launch-wizard-1"]
			)

	# Add a tag to the instance
	print "Adding a tag"
	for inst in reservation.instances:
		tag = { "Type":"Child", "Parent":"My Python Script!" }
		add_tag(inst, tag)

	print "Set up complete." 
	for inst in reservation.instances:
		print "Server id: %s" % inst

# Add a tag to an existing aws instance
def add_tag(instance, tag={ "Parent":"My Python Script!" }):
	# Make sure instanc is actually running
	status = instance.update()
	while status == "pending":
		time.sleep(100)
		status = instance.update()

	if status == "running":
		instance.add_tags(tag)
		print "Added tag to %s." % instance
		return instance
	else:
		print "Could not add tag. Status: %s" % status
		return None

# Terminate a server with given id
def kill_server(instance_id):
	# Connect to AWS
	conn = boto.ec2.connect_to_region("us-east-1")

	# Find given instance
	instance_found = False
	reserves = conn.get_all_reservations()
	for res in reserves:
		for inst in res.instances:
			if inst.id == instance_id:
				instance_found = True
				break

	# Terminate Instance
	if instance_found:
		conn.terminate_instances(instance_ids=[instance_id])
		print "Instance %s terminated" % instance_id
	else:
		print "Instance not found"

# Send message to listener
# TODO: Add ability to specify address to send to. Maybe make into a class
def send_message(message=""):
	# Configure connection
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect(("localhost", 9989))
	print "Connecting to localhost:9989"
	s.sendall(message)
	print "Sent message '%s'" % message
	s.close()

# Send request for status check an get response
def health_check():
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

# Test running terminal commands
def term_command(command):
	print subprocess32.check_output(command.split())

# Manage command line input
def main(argv):
	# # Set up parser
	# parser = argparse.ArgumentParser()
	# group = parser.add_mutually_exclusive_group()
	# group.add_argument('add', type=str, nargs=1,
	# 					help='Add a new server')
	# group.add_argument('del', type=str, nargs=1,
	# 					help='Delete a server')

	# # Read Arguments
	# args = parser.parse_args(argv)

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

		elif argv[0] == "status":
			health_check()

		elif argv[0] == "run":
			term_command(argv[1])

	else:
		print "Invalid arguments."

if __name__=="__main__":
	main(sys.argv[1:])
