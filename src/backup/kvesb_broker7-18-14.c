/*  =========================================================================
    kvesb_broker.c 

    =========================================================================
*/


#include <stdio.h>
#include <unistd.h>
#include "../include/kvesb_common.h"
#include <uuid/uuid.h>

#define HEARTBEAT_LIVENESS  3       //  3-5 is reasonable
#define HEARTBEAT_INTERVAL  2500    //  msecs
#define HEARTBEAT_EXPIRY    HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS

//  The broker class defines a single broker instance

typedef struct {
    zctx_t *ctx;                //  Our context
    void *socket;               //  Router socket for clients & workers
    int verbose;                //  Print activity to stdout
    char *endpoint;             //  Broker binds to this endpoint
    zhash_t *services;          //  Hash of known services
    zhash_t *workers;           //  Hash of known workers
    zlist_t *waiting;           //  List of waiting workers
    uint64_t heartbeat_at;      //  When to send HEARTBEAT

    zhash_t *neighbor_brokers;  // Hash of dealer sockets for brokers
    zhash_t *registered_neighbors; //  Neighbors hashed by their identity
} broker_t;

static broker_t *
    s_broker_new (int verbose);
static void
    s_broker_destroy (broker_t **self_p);
static void
    s_broker_bind (broker_t *self, char *endpoint);
static void
s_broker_connect_to_broker (broker_t *self, char *endpoint);
static void
    s_broker_worker_msg (broker_t *self, zframe_t *sender, zmsg_t *msg);
static void
    s_broker_client_msg (broker_t *self, zframe_t *sender, zmsg_t *msg);
static void
    s_broker_broker_msg (broker_t *self, zframe_t *sender, zmsg_t *msg);

static char* 
    s_broker_generate_uuid();
static void
    s_broker_purge (broker_t *self);

typedef struct {
    broker_t *broker;           //  Broker instance.
    void *socket;               //  Dealer socket to a neighboring broker.
    char *endpoint;
    char *identity;
} neighbor_broker_t;

static neighbor_broker_t *
    s_neighbor_broker_require (broker_t *self, char *endpoint);
static void
    s_neighbor_broker_send (neighbor_broker_t *self, char *command, char *option, zmsg_t *msg);
static void
    s_neighbor_broker_delete (neighbor_broker_t *self);//, int disconnect);
static void
    s_neighbor_broker_destroy (void *argument);

//  The service class defines a single service instance

typedef struct {
    broker_t *broker;           //  Broker instance
    char *name;                 //  Service name
    zlist_t *requests;          //  List of client requests
    zlist_t *waiting;           //  List of waiting workers
    size_t workers;             //  How many workers we have
    zlist_t *blacklist;
} service_t;

static service_t *
    s_service_require (broker_t *self, zframe_t *service_frame);
static void
    s_service_destroy (void *argument);
static void
    s_service_dispatch (service_t *service);
static void
    s_service_enable_command (service_t *self, const char *command);
static void
    s_service_disable_command (service_t *self, const char *command);
static int
    s_service_is_command_enabled (service_t *self, const char *command);


//  The worker class defines a single worker, idle or active

typedef struct {
    broker_t *broker;           //  Broker instance
    char *identity;             //  Identity of worker
    zframe_t *address;          //  Address frame to route to
    service_t *service;         //  Owning service, if known
    int64_t expiry;             //  Expires at unless heartbeat
} worker_t;

static worker_t *
    s_worker_require (broker_t *self, zframe_t *address);
static void
    s_worker_delete (worker_t *self, int disconnect);
static void
    s_worker_destroy (void *argument);
static void
    s_worker_send (worker_t *self, char *command, char *option,
                   zmsg_t *msg);
static void
    s_worker_waiting (worker_t *self);

//  Here are the constructor and destructor for the broker

static broker_t *
s_broker_new (int verbose)
{
    broker_t *self = (broker_t *) zmalloc (sizeof (broker_t));

    //  Initialize broker state
    self->ctx = zctx_new ();
    self->socket = zsocket_new (self->ctx, ZMQ_ROUTER);
    self->verbose = verbose;
    self->services = zhash_new ();
    self->workers = zhash_new ();
    self->waiting = zlist_new ();
    self->neighbor_brokers = zhash_new ();
    self->registered_neighbors = zhash_new ();
    self->heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;
    return self;
}

static void
s_broker_destroy (broker_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        broker_t *self = *self_p;

        zctx_destroy (&self->ctx);
        zhash_destroy (&self->services);
        zhash_destroy (&self->workers);
        zlist_destroy (&self->waiting);
        zhash_destroy (&self->neighbor_brokers);
        zhash_destroy (&self->registered_neighbors);
        free(self->endpoint);
        free (self);
        *self_p = NULL;
    }
}

//  The bind method binds the broker instance to an endpoint. We can call
//  this multiple times. Note that KVESB uses a single socket for both clients 
//  and workers.

// static void?
void
s_broker_bind (broker_t *self, char *endpoint)
{
    zsocket_bind (self->socket, endpoint);
    self->endpoint = strdup(endpoint);
    zclock_log ("I: KVESB broker/0.2.0 is active at %s", endpoint);
}

static void
s_broker_connect_to_broker (broker_t *self, char *endpoint)
{
    assert(endpoint);
    // NOTE: endpoint is not the same as identity.
    neighbor_broker_t *neighbor_broker =
        (neighbor_broker_t *) zhash_lookup (self->neighbor_brokers, endpoint);
    zmsg_t *msg = zmsg_new();

    if(neighbor_broker)
        s_neighbor_broker_delete (neighbor_broker);
    neighbor_broker = s_neighbor_broker_require(self, endpoint);

    if (self->verbose)
        zclock_log ("I: connecting to neighbor broker at %s...", neighbor_broker->endpoint);

    // Q: Not sure why zmq_connect() is used over zsocket_connect().
    zmq_connect(neighbor_broker->socket, endpoint);

    // Register with a neighbor broker. The neighbor broker must send
    // back an ACK with the identity of this socket. This broker can
    // then treat that identity as the identity of the neighbor broker.
    // Send the broker's endpoint so that the neighbor can identify it.
    zmsg_pushstr (msg, self->endpoint);
    s_neighbor_broker_send (neighbor_broker, KVESBB_READY, NULL, msg);
}

static void
s_neighbor_broker_send (neighbor_broker_t *self, char *command, char *option, zmsg_t *msg)
{
    msg = msg? zmsg_dup (msg): zmsg_new ();

    //  Stack protocol envelope to start of message
    if (option) // service, if appropriate.
        zmsg_pushstr (msg, option);
    zmsg_pushstr (msg, command);
    zmsg_pushstr (msg, KVESBB_BROKER);
    zmsg_pushstr (msg, "");

    if (self->broker->verbose) {
        zclock_log ("I: sending %s to neighbor broker",
            kvesbb_commands [(int) *command]);
        zmsg_dump (msg);
    }
    zmsg_send (&msg, self->socket);
}

// NOTE: For now, we use endpoint instead of identity because
// identity isn't immediately available.
static neighbor_broker_t *
s_neighbor_broker_require (broker_t *self, char *endpoint)
{
    assert (endpoint);

    neighbor_broker_t *neighbor_broker =
        (neighbor_broker_t *) zhash_lookup (self->neighbor_brokers, endpoint);

    if (neighbor_broker == NULL) {
        neighbor_broker = (neighbor_broker_t *) zmalloc (sizeof (neighbor_broker_t));
        neighbor_broker->broker = self;
        neighbor_broker->socket = zsocket_new(self->ctx, ZMQ_DEALER);
        neighbor_broker->endpoint = strdup(endpoint);

        zhash_insert (self->neighbor_brokers, endpoint, neighbor_broker);
        zhash_freefn (self->neighbor_brokers, endpoint, s_neighbor_broker_destroy);
        if (self->verbose)
            zclock_log ("I: registering new neighbor broker: %s", endpoint);
    }

    return neighbor_broker;
}

//  The delete method deletes the current neighbor_broker.

static void
s_neighbor_broker_delete (neighbor_broker_t *self)//, int disconnect)
{
    assert (self);

    // TODO: Warn the neighbor_broker of the disconnection.
    //if (disconnect)
    //    s_worker_send (self, KVESBW_DISCONNECT, NULL, NULL);

    // Implicitly calls s_neighbor_broker_destroy.
    zhash_delete (self->broker->neighbor_brokers, self->endpoint);
}

//  neighbor_broker destructor is called automatically whenever the neighbor_broker is
//  removed from broker->neighbor_brokers.

static void
s_neighbor_broker_destroy (void *argument)
{
    neighbor_broker_t *self = (neighbor_broker_t *) argument;

    if(self->identity) {
        int neighbor_registered = (zhash_lookup (self->broker->registered_neighbors, self->identity) != NULL);

        // Delete neighbor_broker from other hash table.
        if(neighbor_registered) {
            zhash_delete(self->broker->registered_neighbors, self->identity);
        }
    }

    //zsocket_destroy (self->broker->ctx, self->socket);
    free(self->endpoint);
    free(self->identity);
    free(self);
}

static void
s_broker_broker_msg (broker_t *self, zframe_t *sender, zmsg_t *msg)
{
    assert(zmsg_size(msg) >= 1);

    zframe_t *command = zmsg_pop (msg);
    char *identity = zframe_strhex (sender);
    int neighbor_registered = (zhash_lookup (self->registered_neighbors, identity) != NULL);

    if(zframe_streq(command, KVESBB_READY)) {
        // TODO: Handle case of a neighbor broker registering twice.
        if(!neighbor_registered) {
            neighbor_broker_t *neighbor_broker;
            //int neighbor_connected = 0;
            char *endpoint = zmsg_popstr(msg);
            assert (endpoint != NULL);

            if (self->verbose)
                zclock_log ("I: received READY from neighbor broker at %s...", endpoint);
printf("Stage1\n");
           // neighbor_connected = (zhash_lookup (self->neighbor_brokers, endpoint) != NULL);
            neighbor_broker = (neighbor_broker_t *) zhash_lookup (self->neighbor_brokers, endpoint);
            free(endpoint);
printf("Stage2\n");
            if(neighbor_broker) {
                zmsg_t *ack_msg= zmsg_new();
printf("Stage3\n");
                if (self->verbose)
                  zclock_log ("I: sending ACK from neighbor broker at %s...", neighbor_broker->endpoint);

                zmsg_pushstr (ack_msg, self->endpoint);
                s_neighbor_broker_send (neighbor_broker, KVESBB_ACK, NULL, ack_msg);
            }
            else {
                // Invalid endpoint.
            }

        }
    }
    else
    if(zframe_streq(command, KVESBB_ACK)) {
        if(!neighbor_registered) {
            neighbor_broker_t *neighbor_broker;
            char *endpoint = zmsg_popstr(msg);
            assert (endpoint != NULL);

            if (self->verbose)
                zclock_log ("I: received ACK from neighbor broker at %s...", endpoint);

            neighbor_broker = (neighbor_broker_t *) zhash_lookup (self->neighbor_brokers, endpoint);
            free(endpoint);

            neighbor_broker->identity = strdup(identity);
            zhash_insert (self->registered_neighbors, identity, neighbor_broker);
        }
        else {
            // Second register?
        }
    }
    free(identity);
    zframe_destroy (&command);
    zmsg_destroy (&msg);
}

//  The worker_msg method processes one READY, REPORT, HEARTBEAT or
//  DISCONNECT message sent to the broker by a worker

static void
s_broker_worker_msg (broker_t *self, zframe_t *sender, zmsg_t *msg)
{
    assert (zmsg_size (msg) >= 1);     //  At least, command

    zframe_t *command = zmsg_pop (msg);
    char *identity = zframe_strhex (sender);
    int worker_ready = (zhash_lookup (self->workers, identity) != NULL);
    free (identity);
    worker_t *worker = s_worker_require (self, sender);

    if (zframe_streq (command, KVESBW_READY)) {
	printf("hello");
        if (worker_ready){               //  Not first command in session
            s_worker_delete (worker, 1);
	    zclock_log("worker deleted");
	}
        else
        if (zframe_size (sender) >= 4  //  Reserved service name
        &&  memcmp (zframe_data (sender), "mmi.", 4) == 0)
            s_worker_delete (worker, 1);
        else {
            //  Attach worker to service and mark as idle
	   
            zframe_t *service_frame = zmsg_pop (msg);
            worker->service = s_service_require (self, service_frame);
            zlist_append (self->waiting, worker);
            zlist_append (worker->service->waiting, worker);
            worker->service->workers++;
            worker->expiry = zclock_time () + HEARTBEAT_EXPIRY;
            s_service_dispatch (worker->service);
            zframe_destroy (&service_frame);
            zclock_log ("worker created");
        }
    }
    else
    if (zframe_streq (command, KVESBW_REPORT)) {
        if (worker_ready) {
            //  Remove & save client return envelope and insert the
            //  protocol header and service name, then rewrap envelope.
            zframe_t *client = zmsg_unwrap (msg);
            zmsg_pushstr (msg, worker->service->name);
            zmsg_pushstr (msg, KVESBC_REPORT);
            zmsg_pushstr (msg, KVESBC_CLIENT);
            zmsg_wrap (msg, client);
            zmsg_send (&msg, self->socket);
        }
        else
            s_worker_delete (worker, 1);
    }
    else
    if (zframe_streq (command, KVESBW_HEARTBEAT)) {
        if (worker_ready) {
            if (zlist_size (self->waiting) > 1) {
                // Move worker to the end of the waiting queue,
                // so s_broker_purge will only check old worker(s)
                zlist_remove (self->waiting, worker);
                zlist_append (self->waiting, worker);
            }
            worker->expiry = zclock_time () + HEARTBEAT_EXPIRY;
        }
        else
            s_worker_delete (worker, 1);
    }
    else
    if (zframe_streq (command, KVESBW_DISCONNECT))
        s_worker_delete (worker, 0);
    else {
        zclock_log ("E: invalid input message");
        zmsg_dump (msg);
    }
    zframe_destroy (&command);
    zmsg_destroy (&msg);
}

//  Process a request coming from a client. We implement MMI requests
//  directly here (at present, we implement only the mmi.service request)

static void
s_broker_client_msg (broker_t *self, zframe_t *sender, zmsg_t *msg)
{
    zmsg_dump(msg);
    assert (zmsg_size (msg) >= 2);     //  Service name + body

    zframe_t *service_frame = zmsg_pop (msg);
    service_t *service = s_service_require (self, service_frame);

    //  If we got a MMI service request, process that internally
    if (zframe_size (service_frame) >= 4
    &&  memcmp (zframe_data (service_frame), "mmi.", 4) == 0) {
        char *return_code;
        if (zframe_streq (service_frame, "mmi.service")) {
            char *name = zframe_strdup (zmsg_last (msg));
            service_t *service =
                (service_t *) zhash_lookup (self->services, name);
            return_code = service && service->workers? "200": "404";
            free (name);
        }
        else
        // The filter service that can be used to manipulate
        // the command filter table.
        if (zframe_streq (service_frame, "mmi.filter")
        && zmsg_size (msg) == 3) {
            zframe_t *operation = zmsg_pop (msg);
            zframe_t *service_frame = zmsg_pop (msg);
            zframe_t *command_frame = zmsg_pop (msg);
            char *command_str = zframe_strdup (command_frame);

            if (zframe_streq (operation, "enable")) {
                service_t *service = s_service_require (self, service_frame);
                s_service_enable_command (service, command_str);
                return_code = "200";
            }
            else
            if (zframe_streq (operation, "disable")) {
                service_t *service = s_service_require (self, service_frame);
                s_service_disable_command (service, command_str);
                return_code = "200";
            }
            else
                return_code = "400";

            zframe_destroy (&operation);
            zframe_destroy (&service_frame);
            zframe_destroy (&command_frame);
            free (command_str);
            //  Add an empty frame; it will be replaced by the return code.
            zmsg_pushstr (msg, "");
        }
        else
            return_code = "501";

        zframe_reset (zmsg_last (msg), return_code, strlen (return_code));

        //  Insert the protocol header and service name, then rewrap envelope.
        zmsg_push (msg, zframe_dup (service_frame));
        zmsg_pushstr (msg, KVESBC_REPORT);
        zmsg_pushstr (msg, KVESBC_CLIENT);
        zmsg_wrap (msg, zframe_dup (sender));
        zmsg_send (&msg, self->socket);
    }
    else {
        int enabled = 1;
        if (zmsg_size (msg) >= 1) {
            zframe_t *cmd_frame = zmsg_first (msg);
            char *cmd = zframe_strdup (cmd_frame);
            enabled = s_service_is_command_enabled (service, cmd);
            free (cmd);
        }

        //  Forward the message to the worker.
        if (enabled) {
	    zmsg_t *uuid_msg = zmsg_new();
            zmsg_pushstr(uuid_msg, s_broker_generate_uuid());
            zmsg_pushstr(uuid_msg, KVESBC_UUID);
	    zmsg_pushstr(uuid_msg, KVESBC_CLIENT);
            zmsg_wrap(uuid_msg, zframe_dup (sender) );
            zmsg_send( &uuid_msg, self->socket);

            zmsg_wrap (msg, zframe_dup (sender));
            zlist_append (service->requests, msg);
            s_service_dispatch (service);
        }
        //  Send a NAK message back to the client.
        else {
            zmsg_push (msg, zframe_dup (service_frame));
            zmsg_pushstr (msg, KVESBC_NAK);
            zmsg_pushstr (msg, KVESBC_CLIENT);
            zmsg_wrap (msg, zframe_dup (sender));
            zmsg_send (&msg, self->socket);
        }
    }

    zframe_destroy (&service_frame);
}

static char *
s_broker_generate_uuid (void)
{
    char hex_char [] = "0123456789ABCDEF";
    char *uuidstr = zmalloc (sizeof (uuid_t) * 2 + 1);
    uuid_t uuid;
    uuid_generate (uuid);
    int byte_nbr;
    for (byte_nbr = 0; byte_nbr < sizeof (uuid_t); byte_nbr++) {
        uuidstr [byte_nbr * 2 + 0] = hex_char [uuid [byte_nbr] >> 4];
        uuidstr [byte_nbr * 2 + 1] = hex_char [uuid [byte_nbr] & 15];
    }
    return uuidstr;
}

//  The purge method deletes any idle workers that haven't pinged us in a
//  while. We hold workers from oldest to most recent, so we can stop
//  scanning whenever we find a live worker. This means we'll mainly stop
//  at the first worker, which is essential when we have large numbers of
//  workers (since we call this method in our critical path)
static void
s_broker_purge (broker_t *self)
{
    worker_t *worker = (worker_t *) zlist_first (self->waiting);
    while (worker) {
        if (zclock_time () < worker->expiry)
            break;                  //  Worker is alive, we're done here
        if (self->verbose)
            zclock_log ("I: deleting expired worker: %s",
                        worker->identity);

        s_worker_delete (worker, 0);
        worker = (worker_t *) zlist_first (self->waiting);
    }
}

//  Here is the implementation of the methods that work on a service.
//  Lazy constructor that locates a service by name, or creates a new
//  service if there is no service already with that name.

static service_t *
s_service_require (broker_t *self, zframe_t *service_frame)
{
    assert (service_frame);
    char *name = zframe_strdup (service_frame);

    service_t *service =
        (service_t *) zhash_lookup (self->services, name);
    if (service == NULL) {
        service = (service_t *) zmalloc (sizeof (service_t));
        service->broker = self;
        service->name = name;
        service->requests = zlist_new ();
        service->waiting = zlist_new ();
        service->blacklist = zlist_new ();
        zhash_insert (self->services, name, service);
        zhash_freefn (self->services, name, s_service_destroy);
        if (self->verbose)
            zclock_log ("I: added service: %s", name);
    }
    else
        free (name);

    return service;
}

//  Service destructor is called automatically whenever the service is
//  removed from broker->services.

static void
s_service_destroy (void *argument)
{
    service_t *service = (service_t *) argument;
    while (zlist_size (service->requests)) {
        zmsg_t *msg = (zmsg_t*)zlist_pop (service->requests);
        zmsg_destroy (&msg);
    }
    //  Free memory keeping  blacklisted commands.
    char *command = (char *) zlist_first (service->blacklist);
    while (command) {
        zlist_remove (service->blacklist, command);
        free (command);
    }
    zlist_destroy (&service->requests);
    zlist_destroy (&service->waiting);
    zlist_destroy (&service->blacklist);
    free (service->name);
    free (service);
}

//  The dispatch method sends request to the worker.
static void
s_service_dispatch (service_t *self)
{
    assert (self);

    s_broker_purge (self->broker);
    if (zlist_size (self->waiting) == 0)
        return;

    while (zlist_size (self->requests) > 0) {
	//printf("worker being dispatched a request");
        worker_t *worker = (worker_t*)zlist_pop (self->waiting);
        zlist_remove (self->waiting, worker);
        zmsg_t *msg = (zmsg_t*)zlist_pop (self->requests);
        s_worker_send (worker, KVESBW_REQUEST, NULL, msg);
        //  Workers are scheduled in the round-robin fashion
        zlist_append (self->waiting, worker);
        zmsg_destroy (&msg);
    }
}

static void
s_service_enable_command (service_t *self, const char *command)
{
    char *item = (char *) zlist_first (self->blacklist);
    while (item && !streq (item, command))
        item = (char *) zlist_next (self->blacklist);
    if (item) {
        zlist_remove (self->blacklist, item);
        free (item);
    }
}

static void
s_service_disable_command (service_t *self, const char *command)
{
    char *item = (char *) zlist_first (self->blacklist);
    while (item && !streq (item, command))
        item = (char *) zlist_next (self->blacklist);
    if (!item)
        zlist_push (self->blacklist, strdup (command));
}

static int
s_service_is_command_enabled (service_t *self, const char *command)
{
    char *item = (char *) zlist_first (self->blacklist);
    while (item && !streq (item, command))
        item = (char *) zlist_next (self->blacklist);
    return item? 0: 1;
}

//  Here is the implementation of the methods that work on a worker.
//  Lazy constructor that locates a worker by identity, or creates a new
//  worker if there is no worker already with that identity.

static worker_t *
s_worker_require (broker_t *self, zframe_t *address)
{
    assert (address);

    //  self->workers is keyed off worker identity
    char *identity = zframe_strhex (address);
    worker_t *worker =
        (worker_t *) zhash_lookup (self->workers, identity);

    if (worker == NULL) {
        worker = (worker_t *) zmalloc (sizeof (worker_t));
        worker->broker = self;
        worker->identity = identity;
        worker->address = zframe_dup (address);
        zhash_insert (self->workers, identity, worker);
        zhash_freefn (self->workers, identity, s_worker_destroy);
        if (self->verbose)
            zclock_log ("I: registering new worker: %s", identity);
    }
    else
        free (identity);
    return worker;
}

//  The delete method deletes the current worker.

static void
s_worker_delete (worker_t *self, int disconnect)
{
    assert (self);
    if (disconnect)
        s_worker_send (self, KVESBW_DISCONNECT, NULL, NULL);

    if (self->service) {
        zlist_remove (self->service->waiting, self);
        self->service->workers--;
    }
    zlist_remove (self->broker->waiting, self);
    //  This implicitly calls s_worker_destroy
    zhash_delete (self->broker->workers, self->identity);
}

//  Worker destructor is called automatically whenever the worker is
//  removed from broker->workers.

static void
s_worker_destroy (void *argument)
{
    worker_t *self = (worker_t *) argument;
    zframe_destroy (&self->address);
    free (self->identity);
    free (self);
}

//  The send method formats and sends a command to a worker. The caller may
//  also provide a command option, and a message payload.

static void
s_worker_send (worker_t *self, char *command, char *option, zmsg_t *msg)
{
    msg = msg? zmsg_dup (msg): zmsg_new ();

    //  Stack protocol envelope to start of message
    if (option)
        zmsg_pushstr (msg, option);
    zmsg_pushstr (msg, command);
    zmsg_pushstr (msg, KVESBW_WORKER);

    //  Stack routing envelope to start of message
    zmsg_wrap (msg, zframe_dup (self->address));

    if (self->broker->verbose) {
        zclock_log ("I: sending %s to worker",
            kvesbw_commands [(int) *command]);
        zmsg_dump (msg);
    }
    zmsg_send (&msg, self->broker->socket);
}


//  Finally here is the main task. We create a new broker instance and
//  then processes messages on the broker socket.

int main (int argc, char *argv [])
{
    int verbose = 0;
    int daemonize = 0;

    int char_num = 0;
    bool bounded = false;
    char temp_char;
    char endpoint[50];

    for (int i = 1; i < argc; i++)
    {
        if (streq(argv[i], "-v")) verbose = 1;
        else if (streq(argv[i], "-d")) daemonize = 1;
        else if (streq(argv[i], "-h"))
        {
            printf("%s [-h] | [-d] [-v] [broker url]\n\t-h This help message\n\t-d Daemon mode.\n\t-v Verbose output\n\tbroker url defaults to tcp://*:5555\n", argv[0]);
            return -1;
        }
    }

    if (daemonize != 0)
    {
        daemon(0, 0);
    }

    broker_t *self = s_broker_new (verbose);

    // Accept input from stdin.
    temp_char = getchar();
    while(EOF != temp_char) {
        if('\n' == temp_char && char_num > 0) {
            endpoint[char_num] = '\0';
            if(!bounded) {
                bounded = true;
                s_broker_bind(self, endpoint);
                printf("Bound to %s\n", endpoint);
            }
            else {
                s_broker_connect_to_broker(self, endpoint);
                printf("Connected to %s\n", endpoint);
            }
            char_num = 0;
        }
        else {
            endpoint[char_num] = temp_char;
            char_num++;
        }
        temp_char = getchar();
    }

    /* did the user specify a bind address? */
    if(!bounded) {
        if (argc > 1)
        {
            s_broker_bind (self, argv[argc-1]);
            printf("Bound to %s\n", argv[argc-1]);
        }
        else
        {
            /* default */
            s_broker_bind (self, "tcp://*:5555");
            printf("Bound to tcp://*:5555\n");
        }
    }

    //  Get and process messages forever or until interrupted
    while (true) {
        zmq_pollitem_t items [] = {
            { self->socket,  0, ZMQ_POLLIN, 0 } };
        int rc = zmq_poll (items, 1, HEARTBEAT_INTERVAL * ZMQ_POLL_MSEC);
        if (rc == -1)
            break;              //  Interrupted

        //  Process next input message, if any
        if (items [0].revents & ZMQ_POLLIN) {
            zmsg_t *msg = zmsg_recv (self->socket);
            if (!msg)
                break;          //  Interrupted
            if (self->verbose) {
                zclock_log ("I: received message:");
                zmsg_dump (msg);
            }
            zframe_t *sender = zmsg_pop (msg);
            zframe_t *empty  = zmsg_pop (msg);
            zframe_t *header = zmsg_pop (msg);

            if (zframe_streq (header, KVESBC_CLIENT))
                s_broker_client_msg (self, sender, msg);
            else
            if (zframe_streq (header, KVESBW_WORKER))
                s_broker_worker_msg (self, sender, msg);
            else
            if (zframe_streq (header, KVESBB_BROKER))
                s_broker_broker_msg (self, sender, msg);
            else {
                zclock_log ("E: invalid message:");
                zmsg_dump (msg);
                zmsg_destroy (&msg);
            }
            zframe_destroy (&sender);
            zframe_destroy (&empty);
            zframe_destroy (&header);
        }
        //  Disconnect and delete any expired workers
        //  Send heartbeats to idle workers if needed
        if (zclock_time () > self->heartbeat_at) {
            s_broker_purge (self);
            worker_t *worker = (worker_t *) zlist_first (self->waiting);
            while (worker) {
                s_worker_send (worker, KVESBW_HEARTBEAT, NULL, NULL);
                worker = (worker_t *) zlist_next (self->waiting);
            }
            self->heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;
        }
    }
    if (zctx_interrupted)
        printf ("W: interrupt received, shutting down...\n");

    s_broker_destroy (&self);
    return 0;
}
