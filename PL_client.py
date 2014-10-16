import os, sys

def do_main(ports, adapters):
	pids = []
	adapter_index = 0
	for services in adapters:
		for client in services:
			command = 'bin/test_client ' + client[0] + ' ' + client[1] + ' ' + str(ports[adapter_index])
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
		adapter_index += 1
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
	if len(sys.argv) != 5:
		sys.stderr.write('Usage %s <start port> <num adapters> <num services per adapter> <num clients per service>\n' \
			% sys.argv[0])
		sys.exit(1)
	
	start_port = int(sys.argv[1])
	num_adapters = int(sys.argv[2])
	num_services_per_adapter = int(sys.argv[3])
	num_clients_per_service = int(sys.argv[4])
	ports = range(start_port, start_port + num_adapters)

	adapters = []
	for adapter_num in range(0, num_adapters):
		service_args = []
		for service_num in range(0, num_services_per_adapter):
			for message_num in range(0, num_clients_per_service):
				service_args.append(['channel' + str(service_num), 'message' + str(message_num)])
		adapters.append(service_args)

	try:
		do_main(ports, adapters)
	except KeyboardInterrupt, e:
		sys.stdout.write("\n")

