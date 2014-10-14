
#include "../include/kvesb.h"
#include "kvesb_client.c"
//#include <ctime>

int main (int argc, char *argv [])
{
    int verbose = (argc > 1 && streq (argv [1], "-v"));
    kvesb_client_t *session;

    int num_requests = 20000;

    int char_num = 0;
    int lines_read = 0;
    char temp_char;
    char endpoint[50];
    char service[50];
    char message[50]; // TODO: Generalize

    // Get the number of requests from the user.
    //scanf("%d", &num_requests);

    // Accept input from stdin.
    temp_char = getchar();
    while(EOF != temp_char) {
        if('\n' == temp_char && char_num > 0) {
            switch(lines_read) {
            case 0:
                endpoint[char_num] = '\0';
                break;
            case 1:
                service[char_num] = '\0';
                break;
            case 2:
                message[char_num] = '\0';
                break;
            }
            char_num = 0;
            lines_read++;
        }
        else {
            switch(lines_read) {
            case 0:
                endpoint[char_num] = temp_char;
                break;
            case 1:
                service[char_num] = temp_char;
                break;
            case 2:
                message[char_num] = temp_char;
                break;
            }
            char_num++;
        }
        temp_char = getchar();
    }

    session = kvesb_client_new (endpoint, verbose);

    int count;
    int64_t duration = zclock_time();
    for (count = 0; count <num_requests ; count++) {
        zmsg_t *request = zmsg_new ();
        zmsg_pushstr (request, message);
        kvesb_client_send (session, service, &request);
        zmsg_t *reply = kvesb_client_recv (session, NULL, NULL);
  		
        reply = kvesb_client_recv (session, NULL, NULL);
        if (reply)
            zmsg_destroy (&reply);
        else
            break;              //  Interrupted by Ctrl-C
    }
    duration = zclock_time() - duration;
    printf ("%d replies sent\n", num_requests);
    printf ("%d replies received\n", count);
    printf("Took %" PRId64 " milliseconds\n", duration);
    kvesb_client_destroy (&session);
    return 0;
}
