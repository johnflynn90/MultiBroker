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
    void *socket;               //  Router socket for clients, workers & brokers
    int verbose;                //  Print activity to stdout
    int benchmark;              //  Record and print various timestamps
    char *endpoint;             //  Broker binds to this endpoint
    zhash_t *services;          //  Hash of known services
    zhash_t *workers;           //  Hash of known workers
    zlist_t *waiting;           //  List of waiting workers
    uint64_t heartbeat_at;      //  When to send HEARTBEAT

    // neighbors is the full list of neighbors. connections is
    // simply a list of neighbors whose two-way connection has been
    // established. Use connections for regular message passing. Use
    // neighbors for everything else including heartbeating and deletion.
    zlist_t *neighbors_list;    //  Temporary list until I update CZMQ.
    zhash_t *neighbors;         //  Hash of known neighbors
    zhash_t *connections;       //  Hash of known connected neighbors.
} broker_t;

// broker_t definitions

static broker_t *
    s_broker_new (int verbose, int benchmark);
static void
    s_broker_destroy (broker_t **self_p);
static void
    s_broker_bind (broker_t *self,/* char *endpoint,*/ char *self_endpoint);
static void
    s_broker_connect_to_broker (broker_t *self, char *endpoint);
static void
    s_broker_worker_msg (broker_t *self, zframe_t *sender, zmsg_t *msg);
static void
    s_broker_client_msg (broker_t *self, zframe_t *sender, zmsg_t *msg);
static void
    s_broker_broker_msg (broker_t *self, zframe_t *sender, zmsg_t *msg);
static void
    s_broker_forward_msg (broker_t *self, char *service, zmsg_t *msg);
static void
    s_broker_return_msg (broker_t *self, zmsg_t *msg);

static char* 
    s_broker_generate_uuid();
static void
    s_broker_purge_workers (broker_t *self);
static void
    s_broker_purge_neighbors(broker_t *self);


typedef struct {
    broker_t *broker;
    void *socket;
    char *endpoint;
    char *identity;

    // Heartbeat management
    int liveness;
    int has_token;              //  Our turn to send this broker a heartbeat.
    uint64_t heartbeat_at;      //  Time at which this neighbor's connection will expire.
    // TODO: Consider adding zframe_t *address?
} neighbor_t;

// neighbor_t definitions

static neighbor_t *
    s_neighbor_new (broker_t *self, char *endpoint);
static neighbor_t *
    s_neighbor_require (broker_t *self, char *endpoint);
static void
    s_neighbor_send (neighbor_t *self, char *command,
                                 char *option, zmsg_t *msg);
static void
    s_neighbor_delete (neighbor_t *self, int disconnect);
static void
    s_neighbor_destroy (void *argument);


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
s_broker_new (int verbose, int benchmark)
{
    broker_t *self = (broker_t *) zmalloc (sizeof (broker_t));

    //  Initialize broker state
    self->ctx = zctx_new ();
    self->socket = zsocket_new (self->ctx, ZMQ_ROUTER);
    self->verbose = verbose;
    self->benchmark = benchmark;
    self->services = zhash_new ();
    self->workers = zhash_new ();
    self->waiting = zlist_new ();
    self->neighbors_list = zlist_new();
    self->neighbors = zhash_new ();
    self->connections = zhash_new ();
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
        zlist_destroy (&self->neighbors_list);
        zhash_destroy (&self->neighbors);
        zhash_destroy (&self->connections);
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
s_broker_bind (broker_t *self,/* char *endpoint,*/ char *self_endpoint)
{
    zsocket_bind (self->socket, self_endpoint); // This used to be just endpoint. Why so?
    self->endpoint = strdup(self_endpoint);
    zclock_log("I: am %s", self_endpoint);
    zclock_log ("I: KVESB broker/0.2.0 is active at %s", self->endpoint);
}

static void
s_broker_connect_to_broker (broker_t *self, char *endpoint)
{
    assert(endpoint);

    s_neighbor_new(self, endpoint);
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
    // TODO: Shouldn't REPORT update the heartbeating expiry for the worker?
    else
    if (zframe_streq (command, KVESBW_REPORT)) {
        if (worker_ready) {
            s_broker_return_msg (self, msg);
        }
        else {
            s_worker_delete (worker, 1);
        }
    }
    else
    if (zframe_streq (command, KVESBW_HEARTBEAT)) {
        if (worker_ready) {
            if (zlist_size (self->waiting) > 1) {
                // Move worker to the end of the waiting queue,
                // so s_broker_purge_workers will only check old worker(s)
                zlist_remove (self->waiting, worker);
                zlist_append (self->waiting, worker);
            }
            worker->expiry = zclock_time () + HEARTBEAT_EXPIRY;
        }
        else {
            s_worker_delete (worker, 1);
        }
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
    //zmsg_dump(msg);
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

static void
s_broker_broker_msg (broker_t *self, zframe_t *sender, zmsg_t *msg)
{
    //zmsg_dump(msg);
    assert(zmsg_size(msg) >= 1);

    zframe_t *command = zmsg_pop (msg);
    char *identity = zframe_strhex (sender);
    neighbor_t *neighbor = zhash_lookup (self->connections, identity); // TODO: Handle case where this doesn't exist.
    int broker_ready = (neighbor != NULL);

    // WARNING: Are there issues with out of order message recv's? 
    // Say brokers A wants to connect to broker B. A creates a neighbor object
    // and adds it to A.neighbors. It then sends a READY message to B. B will
    // then create a neighbor object and add it to B.neighbors if needed. Then
    // it will use the identity of A to register its new neighbor object in
    // B.connections. B will then send a Ready message to A if it hasen't already.
    // A will use B's identity to add its new neighbor object to A.connections.
    if(zframe_streq(command, KVESBB_READY)) {
        // Handles case where broker tries to register a second time.
        // Disconnect the broker and force it to re-register.
        if(broker_ready) {
            s_neighbor_delete(neighbor, 1);
            zclock_log ("broker connection destroyed");
        }
        else {
            // Establish a connection if one doesn't already exist.
            char *endpoint = zmsg_popstr(msg);

            neighbor = s_neighbor_require(self, endpoint);
            neighbor->identity = strdup(identity);
            zhash_insert (self->connections, identity, neighbor);

            // Set up heartbeat data.
            neighbor->has_token = 1;
            neighbor->heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;
            neighbor->liveness = HEARTBEAT_LIVENESS;
            
            if(self->verbose)
                zclock_log ("broker connection created with identity: %s", identity);

            free(endpoint);
        }
        zmsg_destroy (&msg);
    }
    else
    if(broker_ready) {
        if(zframe_streq(command, KVESBB_DISCONNECT)) {
            // A neighboring broker disconnected from this one.
            // Attempt to reconnect.
            char *endpoint = strdup(neighbor->endpoint);

            zclock_log ("broker connection destroyed.. reconnecting");
            s_broker_connect_to_broker(self, endpoint);

            free(endpoint);
            zmsg_destroy (&msg);
        }
        else
        if(zframe_streq(command, KVESBB_REQUEST)) {
            zframe_t *temp = zframe_dup(sender); // TODO: Find a way to do this without copying.
            zframe_t *service_frame = zmsg_pop (msg);
            service_t *service = s_service_require (self, service_frame);

            // TODO: Check if service is enabled.
            //zmsg_wrap (msg, zframe_dup (sender));
            zmsg_prepend(msg, &temp);
            zlist_append (service->requests, msg);
            s_service_dispatch (service);

            zframe_destroy (&service_frame);
        }
        else
        if(zframe_streq(command, KVESBB_REPORT)) {
            s_broker_return_msg (self, msg);
        }
        else
        if(zframe_streq(command, KVESBB_HEARTBEAT)) {
            if(self->verbose) {
                zclock_log ("received heartbeat from broker at %s", neighbor->endpoint);
            }
            zmsg_destroy (&msg);
        }

        // Set up heartbeat data.
        neighbor->has_token = 1;
        neighbor->heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;
        neighbor->liveness = HEARTBEAT_LIVENESS;
    }
    else {
        zclock_log ("invalid broker-broker message");
    }

    free(identity);
    zframe_destroy (&command);
}

//  Send a REQUEST message to a neighbor broker. Unlike s_broker_return_msg()
//  this function is only called when it has been decided that the destination
//  will be a broker. s_broker_return_msg() decides to send the message to a
//  broker or client once it is called. This is because s_broker_forward_msg()
//  can only be appropriately called by s_service_dispatch().

static void
s_broker_forward_msg (broker_t *self, char *service, zmsg_t *msg) {
    neighbor_t *neighbor = (neighbor_t *) zlist_head (self->neighbors_list);

    msg = msg? zmsg_dup (msg): zmsg_new ();
    s_neighbor_send (neighbor, KVESBB_REQUEST, service, msg);
}

// Send a REPORT message to a neighbor or the owning client.

static void
s_broker_return_msg (broker_t *self, zmsg_t *msg) {
    msg = msg? zmsg_dup (msg): zmsg_new ();

    if(zmsg_size(msg) > 4) { // 4 means [clientaddr][""][service][msg]
        zframe_t *next_address = zmsg_pop(msg);
        char *next_identity = zframe_strhex(next_address);
        // TODO: Use s_neighbor_require()?
        neighbor_t *neighbor = (neighbor_t*)zhash_lookup(self->connections, next_identity);
        assert(neighbor != NULL);

        s_neighbor_send (neighbor, KVESBB_REPORT, NULL, msg);

        free(next_identity);
        zframe_destroy(&next_address);
    }
    else {
        zframe_t *client = zmsg_unwrap (msg);
        zmsg_pushstr (msg, KVESBC_REPORT);
        zmsg_pushstr (msg, KVESBC_CLIENT);
        zmsg_wrap (msg, client);
        //zmsg_dump(msg);
        zmsg_send (&msg, self->socket);
    }
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
s_broker_purge_workers (broker_t *self)
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

// Same as purge_workers except that items are not in order by oldest
// to most recent so we much iterate through all of them.
/*static void
s_broker_purge_neighbors (broker_t *self)
{
    neighbor_t *neighbor = (neighbor_t *) zlist_first (self->neighbors_list);
    while (neighbor) {
        if (neighbor->liveness <= 0) {
            if (self->verbose)
                zclock_log ("I: deleting expired worker: %s",
                            neighbor->endpoint);

            s_neighbor_delete (neighbor, 1);
        }
        worker = (worker_t *) zlist_next (self->neighbors_list);
    }
}*/

// Creates a new neighbor and destroys the old if it exists.

static neighbor_t *
s_neighbor_new (broker_t *self, char *endpoint)
{
    assert(endpoint);

    neighbor_t *neighbor;
    zmsg_t *msg = zmsg_new();
    int neighbor_exists = (zhash_lookup (self->neighbors, endpoint) != NULL);

    if(neighbor_exists) {
        s_neighbor_delete(neighbor, 1);
    }

    // Initialize neighbor.
    neighbor = (neighbor_t *) zmalloc (sizeof (neighbor_t));
    neighbor->socket = zsocket_new (self->ctx, ZMQ_DEALER);
    neighbor->broker = self;
    neighbor->endpoint = strdup(endpoint);
    neighbor->identity = NULL;

    // Set up heartbeat data.
    neighbor->has_token = 1; // Let all connections initially think they have the token.
    neighbor->heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;
    neighbor->liveness = HEARTBEAT_LIVENESS;

    zlist_append (self->neighbors_list, neighbor); // Temporary.
    zhash_insert (self->neighbors, endpoint, neighbor);
    zhash_freefn (self->neighbors, endpoint, s_neighbor_destroy);

    //zlist_append (self->neighbors, neighbor);
    //zlist_freefn (self->neighbors, neighbor, s_neighbor_destroy, 0);

    // Q: Not sure why zmq_connect() is used over zsocket_connect().
    zmq_connect(neighbor->socket, endpoint);

    if (self->verbose)
        zclock_log ("I: connecting to neighbor broker at %s...", neighbor->endpoint);

    // Send endpoint of this broker in ready message.
    zmsg_pushstr (msg, self->endpoint);
    s_neighbor_send(neighbor, KVESBB_READY, NULL, msg);

    zmsg_destroy(&msg);

    return neighbor;
}

static neighbor_t *
s_neighbor_require (broker_t *self, char *endpoint)
{
    assert (endpoint);

    neighbor_t *neighbor =
        (neighbor_t *) zhash_lookup (self->neighbors, endpoint);

    if (neighbor == NULL) {
        neighbor = s_neighbor_new (self, endpoint);

        if (self->verbose)
            zclock_log ("I: registering new broker connection for %s", endpoint);
    }

    return neighbor;
}

static void
s_neighbor_send (neighbor_t *self, char *command, char *option, zmsg_t *msg)
{
    msg = msg? zmsg_dup (msg): zmsg_new ();

    //  Stack protocol envelope to start of message
    if (option) // service, if appropriate.
        zmsg_pushstr (msg, option);
    zmsg_pushstr (msg, command);
    zmsg_pushstr (msg, KVESBB_BROKER);
    zmsg_pushstr (msg, "");

    // Heartbeat management.
    self->has_token = 0;

    if (self->broker->verbose) {
        zclock_log ("I: sending %s to neighbor broker",
            kvesbb_commands [(int) *command]);
        zmsg_dump (msg);
    }
    // Send to dealer socket.
    zmsg_send (&msg, self->socket);
}

//  The delete method deletes the current neighbor.

static void
s_neighbor_delete (neighbor_t *self, int disconnect)
{
    assert (self);

    // Tell the neighbor to disconenct. This might make the neighbor reconnect.
    if (disconnect)
        s_neighbor_send (self, KVESBW_DISCONNECT, NULL, NULL);

    // Putting this into s_neighbor_destroy runs the risk of
    // being called after the context is destroyed.
    zsocket_destroy (self->broker->ctx, self->socket);

    // Implicitly calls s_neighbor_destroy.
    zlist_remove (self->broker->neighbors_list, self); // Temporary
    zhash_delete (self->broker->connections, self->identity);
    zhash_delete (self->broker->neighbors, self->endpoint);
}

//  neighbor destructor is called automatically whenever the neighbor is
//  removed from broker->neighbors.

static void
s_neighbor_destroy (void *argument)
{
    neighbor_t *self = (neighbor_t *) argument;

    if(self->identity != NULL) {
    //    zframe_destroy (&self->address);
        free (self->identity);
    }
    free(self->endpoint);
    free(self);
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

    s_broker_purge_workers (self->broker);
    if (zlist_size (self->waiting) > 0) {
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
    else {
        // There are no available workers. Logic should be added
        // here to determine which (if any) neighboring broker should
        // process the request.
        while (zlist_size (self->requests) > 0) {
            zmsg_t *msg = (zmsg_t*)zlist_pop (self->requests);
            s_broker_forward_msg(self->broker, self->name, msg);

            zmsg_destroy(&msg);
        }
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
    int benchmark = 0;

    int char_num = 0;
    int lines_read = 0;
    char temp_char;
    char endpoint[50];
    char self_endpoint[50];

    /*zlist_t *test = zlist_new();

    for(int i = 0; i < 10; i++) {
        int *k = (int *)malloc(sizeof(int));
        *k = i;
        zlist_append(test, k);
        printf("Num 1:%d\n", *k);
    }

    int *num = (int *)zlist_first(test);
    while (num != 0) {
        if(2 == *num){
            zlist_remove(test, num);   
        }

        printf("Num 2:%d\n", *num);
        num = (int *)zlist_next(test);
    }*/

    for (int i = 1; i < argc; i++)
    {
        if (streq(argv[i], "-v")) verbose = 1;
        else if (streq(argv[i], "-d")) daemonize = 1;
        else if (streq(argv[i], "-b")) benchmark = 1;
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

    broker_t *self = s_broker_new (verbose, benchmark);

    // Accept input from stdin.
    temp_char = getchar();
    while(EOF != temp_char) {
        if('\n' == temp_char && char_num > 0) {
            if(lines_read == 0) {
                self_endpoint[char_num] = '\0';
                s_broker_bind(self, self_endpoint);
                printf("Bound to %s\n", self_endpoint);
            }
            else {
                endpoint[char_num] = '\0';
                /*if(lines_read == 1) {
                    s_broker_bind(self, endpoint, self_endpoint);
                    printf("Bound to %s\n", self_endpoint);
                }*/
                //else {
                    s_broker_connect_to_broker(self, endpoint);
                    printf("Connected to %s\n", endpoint);
                //}
            }
            char_num = 0;
            lines_read++;
        }
        else {
            if(lines_read == 0) {
                self_endpoint[char_num] = temp_char;
            }
            else {
                endpoint[char_num] = temp_char;
            }
            char_num++;
        }
        temp_char = getchar();
    }

    /* did the user specify a bind address? */
    /*if(lines_read == 0) {
        if (argc > 1)
        {
            s_broker_bind (self, argv[argc-1], "");
            printf("Bound to %s\n", argv[argc-1]);
        }
        else
        {
            /* default *
            s_broker_bind (self, "tcp://*:5555", "");
            printf("Bound to tcp://*:5555\n");
        }
    }*/

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
        uint64_t current_time = zclock_time();
        if (current_time > self->heartbeat_at) {
            s_broker_purge_workers (self);//_workers(self);
            worker_t *worker = (worker_t *) zlist_first (self->waiting);
            while (worker) {
                s_worker_send (worker, KVESBW_HEARTBEAT, NULL, NULL);
                worker = (worker_t *) zlist_next (self->waiting);
            }
            self->heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;
        }
        //s_broker_purge_neighbors(self);
        // TODO: Potentially replace this with a list.
        // TODO: Q: Should this heartbeat self->neighbors or just self->connections?
        neighbor_t *neighbor = (neighbor_t *)zlist_first(self->neighbors_list);
        while (neighbor) {
            if(current_time > neighbor->heartbeat_at) {
                if(neighbor->has_token) {
                    if(self->verbose) {
                        zclock_log("Sent token for %s", neighbor->endpoint);
                    }
                    s_neighbor_send (neighbor, KVESBB_HEARTBEAT, NULL, NULL);
                }
                else if(--neighbor->liveness == 0) {
                    zclock_log("Disconnected from broker at %s", neighbor->endpoint);
                    // zclock_sleep (self->reconnect);
                    // TODO: Disconnect and maybe reconnect.
                    // TODO: Q: Should this continue to send reconnect requests to the neighbor broker?
                    // What if it is dead permanently? And what if it comes back after a long time?

                }
                else {
                    //zclock_log("No token for %s", neighbor->endpoint);
                }
                neighbor->heartbeat_at = current_time + HEARTBEAT_INTERVAL;
            }
            neighbor = (neighbor_t *) zlist_next (self->neighbors_list);
        }
    }
    if (zctx_interrupted)
        printf ("W: interrupt received, shutting down...\n");

    s_broker_destroy (&self);
    return 0;
}
