import os, sys

def do_main(argv, address):	
	pids = []
	for arg in argv:
		command = 'bin/kvesb_adapter ' + str(arg) + ' ' + address
		print 'command: ' + command
		try:
			pid = os.fork()
			if (pid > 0):
				pids.append(pid)
			else:
				os.system(command)
				return
		except OSError, e:
			sys.stderr.write("Fork failed: %d (%s)\n" % (e.errno, e.strerror))
			sys.exit(1)
	# in the parent
	try:
		for pid in pids:
			os.waitpid(pid, 0);
	except OSError, e:
		sys.stderr.write("Wait failed: %d (%s)\n" % (e.errno, e.strerror))
		sys.exit(1)
#end do_main

if __name__ == "__main__":
	ports = []
	if len(sys.argv) != 4:
		sys.stderr.write('Usage %s <start port> <num_adapters> <server address>\n' \
			% sys.argv[0])
		sys.exit(1)
	
	start_port = int(sys.argv[1])
	num_adapters = int(sys.argv[2])
	ports = range(start_port, start_port + num_adapters)
	address = sys.argv[3]
	try:
		do_main(ports, address)
	except KeyboardInterrupt, e:
		sys.stdout.write("\n")

