

#include "../include/kvesb_common.h"
#include "../include/kvesb_worker.h"

//  Reliability parameters
#define HEARTBEAT_LIVENESS  3       //  3-5 is reasonable


//  Structure of our class
//  We access these properties only via class methods

struct _kvesb_worker_t {
    zctx_t *ctx;                //  Our context
    char *broker;
    char *service;
    void *worker;               //  Socket to broker
    int verbose;                //  Print activity to stdout

    //  Heartbeat management
    uint64_t heartbeat_at;      //  When to send HEARTBEAT
    size_t liveness;            //  How many attempts left
    int heartbeat;              //  Heartbeat delay, msecs
    int reconnect;              //  Reconnect delay, msecs
};

// \/ These two functions are temporary until I figure out a better way. \/

// TODO: Verify that this is a valid implementation. Eg. no memory leaks.
// TODO: Add assertions.
static zmsg_t *
unwrapSenderChain (zmsg_t *msg) {
    zmsg_t *address = zmsg_new();
    zframe_t *current_frame = zmsg_pop(msg);

    while(current_frame != NULL) {
        if(zframe_streq(current_frame, "")) {
            zmsg_prepend(address, &current_frame);
                break;
            }
        zmsg_prepend(address, &current_frame);
        current_frame = zmsg_pop(msg);
    }

    return address;
}

// TODO: Verify that this is a valid implementation. Eg. no memory leaks.
// TODO: Add assertions.
static void
wrapSenderChain (zmsg_t *msg, zmsg_t *address) {
    // TODO: Find a more efficient way to do this.
    // Prepend all of the broker forwarding data.
    zframe_t *current_frame = zmsg_pop(address);
    while(current_frame != NULL) {
        zmsg_prepend(msg, &current_frame);
        current_frame = zmsg_pop(address);
    }
}


//  We have two utility functions; to send a message to the broker and
//  to (re-)connect to the broker.

//  ---------------------------------------------------------------------
//  Send message to broker
//  If no msg is provided, creates one internally

static void
s_kvesb_worker_send_to_broker (kvesb_worker_t *self, char *command, char *option,
                        zmsg_t *msg)
{
    msg = msg? zmsg_dup (msg): zmsg_new ();

    //  Stack protocol envelope to start of message
    if (option)
        zmsg_pushstr (msg, option);
    zmsg_pushstr (msg, command);
    zmsg_pushstr (msg, KVESBW_WORKER);
    zmsg_pushstr (msg, "");

    if (self->verbose) {
        zclock_log ("I: sending %s to broker",
            kvesbw_commands [(int) *command]);
        zmsg_dump (msg);
    }
    zmsg_send (&msg, self->worker);
}


//  ---------------------------------------------------------------------
//  Connect or reconnect to broker

void s_kvesb_worker_connect_to_broker (kvesb_worker_t *self)
{
    // Q: I can't figure out why it is nesessary to destroy and recreate the socket here.
    // It's obvious that if the broker crashes we need to resend a ready message, but
    // why do we have to manually close the socket? This seems like it will only affect
    // the identity of the worker on the broker side but does that even matter?
    if (self->worker)
        zsocket_destroy (self->ctx, self->worker);
    self->worker = zsocket_new (self->ctx, ZMQ_DEALER);
    zmq_connect (self->worker, self->broker);
    if (self->verbose)
        zclock_log ("I: connecting to broker at %s...", self->broker);

    //  Register service with broker
    printf("sending ready\n");	
    s_kvesb_worker_send_to_broker (self, KVESBW_READY, self->service, NULL);

    //  If liveness hits zero, worker is considered disconnected
    self->liveness = HEARTBEAT_LIVENESS;
    self->heartbeat_at = zclock_time () + self->heartbeat;
}


//  Here we have the constructor and destructor for our kvesb_worker class

//  ---------------------------------------------------------------------
//  Constructor

kvesb_worker_t *
kvesb_worker_new (char *broker, const char *service, int verbose)
{
    assert (broker);
    assert (service);

    kvesb_worker_t *self = (kvesb_worker_t *) zmalloc (sizeof (kvesb_worker_t));
    self->ctx = zctx_new ();
    self->broker = strdup (broker);
    self->service = strdup (service);
    self->verbose = verbose;
    self->heartbeat = 2500;     //  msecs
    self->reconnect = 2500;     //  msecs

    s_kvesb_worker_connect_to_broker (self);
    return self;
}


//  ---------------------------------------------------------------------
//  Destructor

void
kvesb_worker_destroy (kvesb_worker_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        kvesb_worker_t *self = *self_p;
        zctx_destroy (&self->ctx);
        free (self->broker);
        free (self->service);
        free (self);
        *self_p = NULL;
    }
}


//  We provide two methods to configure the worker API. You can set the
//  heartbeat interval and retries to match the expected network performance.

//  ---------------------------------------------------------------------
//  Set heartbeat delay

void
kvesb_worker_set_heartbeat (kvesb_worker_t *self, int heartbeat)
{
    self->heartbeat = heartbeat;
}


//  ---------------------------------------------------------------------
//  Set reconnect delay

void
kvesb_worker_set_reconnect (kvesb_worker_t *self, int reconnect)
{
    self->reconnect = reconnect;
}


//  ---------------------------------------------------------------------
//  Set worker socket option

int
kvesb_worker_setsockopt (kvesb_worker_t *self, int option, const void *optval, size_t optvallen)
{
    assert (self);
    assert (self->worker);
    return zmq_setsockopt (self->worker, option, optval, optvallen);
}


//  ---------------------------------------------------------------------
//  Get worker socket option

int
kvesb_worker_getsockopt (kvesb_worker_t *self, 	int option, void *optval, size_t *optvallen)
{
    assert (self);
    assert (self->worker);
    return zmq_getsockopt (self->worker, option, optval, optvallen);
}


//  This is the recv method; it receives a new request from a client.
//  If reply_to_p is not NULL, a pointer to client's address is filled in.

//  ---------------------------------------------------------------------
//  Wait for a new request.

zmsg_t *
kvesb_worker_recv (kvesb_worker_t *self, zmsg_t **reply_to_p)
{
    while (true) {
        zmq_pollitem_t items [] = {
            { self->worker,  0, ZMQ_POLLIN, 0 } };
        int rc = zmq_poll (items, 1, self->heartbeat * ZMQ_POLL_MSEC);
        if (rc == -1)
            break;              //  Interrupted

        if (items [0].revents & ZMQ_POLLIN) {
            zmsg_t *msg = zmsg_recv (self->worker);
            if (!msg)
                break;          //  Interrupted
            if (self->verbose) {
                zclock_log ("I: received message from broker:");
                zmsg_dump (msg);
            }
            self->liveness = HEARTBEAT_LIVENESS;

            //  Don't try to handle errors, just assert noisily
            assert (zmsg_size (msg) >= 3);

            zframe_t *empty = zmsg_pop (msg);
            assert (zframe_streq (empty, ""));
            zframe_destroy (&empty);

            zframe_t *header = zmsg_pop (msg);
            assert (zframe_streq (header, KVESBW_WORKER));
            zframe_destroy (&header);

            zframe_t *command = zmsg_pop (msg);
            if (zframe_streq (command, KVESBW_REQUEST)) {
                //  Here is where we actually have a message to process; we
                //  return it to the caller application
                zmsg_t *reply_to = unwrapSenderChain(msg); 
                if (reply_to_p)
                    *reply_to_p = reply_to;
                else
                    zmsg_destroy (&reply_to);

                zframe_destroy (&command);
                //  Here is where we actually have a message to process; we
                //  return it to the caller application
                return msg;     //  We have a request to process
            }
            else
            if (zframe_streq (command, KVESBW_HEARTBEAT))
                ;               //  Do nothing for heartbeats
            else
            if (zframe_streq (command, KVESBW_DISCONNECT))
                s_kvesb_worker_connect_to_broker (self);
            else {
                zclock_log ("E: invalid input message");
                zmsg_dump (msg);
            }
            zframe_destroy (&command);
            zmsg_destroy (&msg);
        }
        else
        if (--self->liveness == 0) {
            if (self->verbose)
                zclock_log ("W: disconnected from broker - retrying...");
            zclock_sleep (self->reconnect);
            s_kvesb_worker_connect_to_broker (self);
        }
        //  Send HEARTBEAT if it's time
        if (zclock_time () > self->heartbeat_at) {
            s_kvesb_worker_send_to_broker (self, KVESBW_HEARTBEAT, NULL, NULL);
            self->heartbeat_at = zclock_time () + self->heartbeat;
        }
    }
    if (zctx_interrupted)
        printf ("W: interrupt received, killing worker...\n");
    return NULL;
}


//  ---------------------------------------------------------------------
//  Send a report to the client.

void
kvesb_worker_send (kvesb_worker_t *self, zmsg_t **report_p, zmsg_t *reply_to)
{
    assert (report_p);
    zmsg_t *report = *report_p;
    assert (report);
    assert (reply_to);
    // Add client address

    // Add the service name. TODO: This should be done in the broker class.
    zmsg_pushstr(report, self->service);

    // TODO: Find a more efficient way to do this.
    wrapSenderChain(report, reply_to);

    s_kvesb_worker_send_to_broker (self, KVESBW_REPORT, NULL, report);
    zmsg_destroy (report_p);
}
