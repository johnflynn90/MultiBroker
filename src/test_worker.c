// test worker for adapter
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h> //inet_addr
#include <jansson.h> // for JSON
 
#define MESSAGE_SIZE        1000
#define ADAPTER_IP_ADDRESS  "127.0.0.1"
//#define ADAPTER_PORT        8888

#define JSON_MESSAGE    "message"
#define JSON_SERVICE    "service"

int main(int argc , char *argv[])
{
    struct sockaddr_in server;
    int socket_desc;
    int read_size;
    char *request_message;
    int request_message_length = 0;
    int adapter_port = 0;
    char service[100], message[100];
    json_t *root;

    // Input the service name and the port of the local adapter.
    if(argc != 3){
        fprintf(stderr, "error: Invalid input parameters. Use <service name> <adapter port>");
    }
    strncpy(service, argv[1], 99);
    service[99] = '\0';
    sscanf(argv[2], "%d", &adapter_port);

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
    if (connect(socket_desc , (struct sockaddr *)&server , sizeof(server)) < 0){
        puts("connect error");
        return 1;
    }
    puts("Connected");

    /*printf("Enter service: ");
    scanf("%s", service);*/

    root = json_object();
    json_object_set_new(root, JSON_SERVICE, json_string(service));

    request_message = json_dumps(root, 0);
    request_message_length = strlen(request_message);
    printf("Service Assignment: %s\n", request_message);

    // TODO: Find a way around this.
    if(request_message_length >= MESSAGE_SIZE){
        puts("Message too large");
        return 1;
    }

    if(send(socket_desc, request_message, request_message_length, 0) < 0){
        puts("Send failed");
        return 1;
    }
    free(request_message);
    json_decref(root);

    while(1){
        char reply_message[MESSAGE_SIZE];

        if((read_size = recv(socket_desc, reply_message, MESSAGE_SIZE, 0)) < 0){
            puts("Receive failed");
            return 1;
        }
        if(read_size == 0){
            puts("Adapter disconnected");
            break;
        }

            /*printf("Received/Sending Message: %s\n", reply_message);*/

        // Echo message back.
        if(send(socket_desc, reply_message, MESSAGE_SIZE, 0) < 0){
            puts("Send failed");
            return 1;
        }
    }

    return 0;
}
