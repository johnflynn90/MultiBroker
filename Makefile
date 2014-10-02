all : kvesb_broker orca orcb kf lr test_clientA test_clientB

kvesb_broker : kvesb_broker.c
	gcc kvesb_broker.c -lzmq -lczmq -luuid -o kvesb_broker -std=gnu99 -ljansson

orca : orch_serviceA.c
	gcc orch_serviceA.c -lzmq -lczmq -o orca -std=gnu99 -ljansson

orcb : orch_serviceB.c
	gcc orch_serviceB.c -lzmq -lczmq -o orcb -std=gnu99 -ljansson

kf : kf.c
	gcc kf.c -lzmq -lczmq -o kf -std=gnu99 -ljansson

lr : lr.c
	gcc lr.c -lzmq -lczmq -o lr -std=gnu99 -ljansson

test_clientA : test_client.c
	gcc test_client.c -lzmq -lczmq -o test_clientA -std=gnu99 -ljansson

test_clientB : test_clientB.c
	gcc test_clientB.c -lzmq -lczmq -o test_clientB -std=gnu99 -ljansson

clean : 
	rm kvesb_broker orca orcb kf lr test_clientA test_clientB


