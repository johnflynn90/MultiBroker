import os, sys

def do_main(argv):	
	pids = []
	for arg in argv:
		try:
			pid = os.fork()
			if (pid > 0):
				pids.append(pid)
			else:
				os.system(arg)
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
	args = []
	if len(sys.argv) < 2:
		sys.stderr.write('Usage %s "command 1" "command 2" ... "command n"\n' \
			% sys.argv[0])
		sys.exit(1)
	
	for cmd in sys.argv[1:]:
		cmd = cmd.strip()
		if cmd:
			args.append(cmd);
	try:
		do_main(args)
	except KeyboardInterrupt, e:
		sys.stdout.write("\n")

