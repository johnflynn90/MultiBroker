all : kvesb_broker kvesb_adapter test_client test_worker

kvesb_broker : src/kvesb_broker.c
	gcc src/kvesb_broker.c -lzmq -lczmq -luuid -o bin/kvesb_broker -std=gnu99 -ljansson

kvesb_adapter : src/kvesb_adapter.c
	gcc src/kvesb_adapter.c -pthread -ljansson -lzmq -lczmq -luuid -o bin/kvesb_adapter -std=gnu99

test_client : src/test_client.c
	gcc src/test_client.c -lzmq -lczmq -o bin/test_client -std=gnu99 -ljansson

test_worker : src/test_worker.c
	gcc src/test_worker.c -lzmq -lczmq -o bin/test_worker -std=gnu99 -ljansson

clean : 
	rm bin/kvesb_broker bin/kvesb_adapter bin/test_client bin/test_worker


