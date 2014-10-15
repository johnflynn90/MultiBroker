
#include "../include/kvesb.h"
#include "kvesb_client.c"

int main (int argc, char *argv [])
{
    int verbose = (argc > 1 && streq (argv [1], "-v"));
    kvesb_client_t *session = kvesb_client_new ("tcp://localhost:5556", verbose);

    int count;
    for (count = 0; count <1 ; count++) {
        zmsg_t *request = zmsg_new ();
        zmsg_pushstr (request, "Hello world");
        kvesb_client_send (session, "channelA", &request);
        zmsg_t *reply = kvesb_client_recv (session, NULL, NULL);
  		
        reply = kvesb_client_recv (session, NULL, NULL);
        if (reply)
            zmsg_destroy (&reply);
        else
            break;              //  Interrupted by Ctrl-C
    }
    printf ("%d replies received\n", count);
    kvesb_client_destroy (&session);
    return 0;
}
