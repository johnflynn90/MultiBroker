

#include "../include/kvesb.h"
#include "kvesb_worker.c"
int main (int argc, char *argv [])
{
    int verbose = (argc > 1 && streq (argv [1], "-v"));
    kvesb_worker_t *session;

    int char_num = 0;
    int lines_read = 0;
    char temp_char;
    char endpoint[50];
    char service[50];

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
            }
            char_num++;
        }
        temp_char = getchar();
    }

    session = kvesb_worker_new(endpoint, service, verbose);

    while (1) {
        zmsg_t *reply_to;
        zmsg_t *request = kvesb_worker_recv (session, &reply_to);
        if (request == NULL)
            break;              //  Worker was interrupted
        //  Echo message
        kvesb_worker_send (session, &request, reply_to);
        zmsg_destroy (&reply_to);
    }
    kvesb_worker_destroy (&session);
    return 0;
}
