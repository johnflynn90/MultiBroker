// test client for adapter
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h> //inet_addr
#include <jansson.h> // for JSON
#include <signal.h> // ctrl+c
//#include <time.h>

#define MESSAGE_SIZE        1000
#define ADAPTER_IP_ADDRESS  "127.0.0.1"
//#define ADAPTER_PORT        8888

#define JSON_MESSAGE    "message"
#define JSON_SERVICE    "service"

static int Terminated = 0;

void ctrlcHandler(int);

void ctrlcHandler(int dummy)
{
    Terminated = 1;
}

int main(int argc , char *argv[])
{
    struct sockaddr_in server;
    int socket_desc;
    int adapter_port = 0;
    char service[100], message[100];
    unsigned long long num_requests = 0;
    unsigned long long num_replies = 0;
    //clock_t time_elapsed = 0;

    signal(SIGINT, ctrlcHandler); // Handle ctrl+c

    strncpy(service, argv[1], 99);
    service[99] = '\0';
    strncpy(message, argv[2], 99);
    message[99] = '\0';
    sscanf(argv[3], "%d", &adapter_port);

    socket_desc = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_desc == -1){
        printf("Could not create socket\n");
        return 0;
    }
    printf("Created socket!\n");

    server.sin_addr.s_addr = inet_addr(ADAPTER_IP_ADDRESS);
    server.sin_family = AF_INET;
    server.sin_port = htons(adapter_port);

    //Connect to remote server
    if (connect(socket_desc, (struct sockaddr *)&server, sizeof(server)) < 0){
        puts("connect error");
        return 1;
    }
    puts("Connected");

    //time_elapsed = clock();
    int i = 0;
    while(!Terminated && i < 1000){
        char *request_message, reply_message[MESSAGE_SIZE];
        int request_message_length = 0;
        //char service[100], message[100];
        char user_command;
        int read_size;
        json_t *root;

        /*printf("Send a message? (y/n): ");
        scanf(" %c", &user_command);
        if(user_command == 'n'){
            break;
        }

        printf("Enter service: ");
        scanf("%s", service);

        printf("Enter message: ");
        scanf("%s", message);*/

        root = json_object();
        json_object_set_new(root, JSON_SERVICE, json_string(service));
        json_object_set_new(root, JSON_MESSAGE, json_string(message));

        request_message = json_dumps(root, 0);
        request_message_length = strlen(request_message);
        /*printf("Sending Message: %s\n", request_message);*/

        // TODO: Find a way around this.
        if(request_message_length >= MESSAGE_SIZE){
            puts("Message too large");
            json_decref(root);
            return 1;
        }

        if(send(socket_desc, request_message, request_message_length + 1, 0) < 0){
            puts("Send failed");
            json_decref(root);
            return 1;
        }
        num_requests++;
        free(request_message);

        if((read_size = recv(socket_desc, reply_message, MESSAGE_SIZE, 0)) < 0){
            // TODO: What should we do here?
            puts("Receive failed");
            json_decref(root);
            return 1;
        }
        if(read_size == 0){
            puts("Adapter disconnected");
            json_decref(root);
            return 0;
        }
        num_replies++;

        /*printf("Received Message: %s\n", reply_message);*/

        i++;
        json_decref(root);
    }

    //time_elapsed = clock() - time_elapsed;
    //float TE = ((float)time_elapsed) / CLOCKS_PER_SEC;
    //printf("time_elapsed: %d, TE: %f", (int)time_elapsed, TE);
    printf("Service: %s Message: %s Requests: %llu Replies: %llu\n", service, message, num_requests, num_replies);
    return 0;
}
