#include <stdio.h>
#include <string.h>    //strlen
#include <stdlib.h>    //strlen
#include <sys/socket.h>
#include <arpa/inet.h> //inet_addr
#include <unistd.h>    //write
#include <pthread.h> //for threading , link with lpthread
#include <jansson.h> // for JSON

//#include "../include/kvesb_common.h"

// TODO: Should these not be referenced?
#include "kvesb_worker.c"
#include "kvesb_client.c"

#define PORT_NUMBER     8888
#define MESSAGE_SIZE    1000

#define JSON_MESSAGE    "message"
#define JSON_SERVICE    "service"

typedef struct {
    int verbose;        // Debug mode.
    int socket;         // Incoming connection to adapter.
    char *endpoint;     // Endpoint to broker.

} connection_data_t;

void *connectionHandler(void *);
int executeWorker(connection_data_t *connection_data, const char *service);
int executeClient();

json_t *createJSONObject(const char *json_string, json_error_t *error);
int extractJSONValue(json_t *root, const char *key, const char **value_text);

int main(int argc , char *argv[])
{
    int verbose = (argc > 1 && streq (argv [1], "-v"));
    struct sockaddr_in server, client;
    connection_data_t *connection_data;
    int socket_desc , new_socket, c;
    char endpoint[50]; // TODO: Dynamically allocate?

    // Accept input from stdin.
    scanf("%s", endpoint);

    //Create socket
    socket_desc = socket(AF_INET , SOCK_STREAM , 0);
    if (socket_desc == -1){
        printf("Could not create socket");
    }
     
    //Prepare the sockaddr_in structure
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = INADDR_ANY;
    server.sin_port = htons(PORT_NUMBER);
     
    //Bind
    if( bind(socket_desc,(struct sockaddr *)&server , sizeof(server)) < 0){
        puts("bind failed");
        return 1;
    }
    puts("bind done");
     
    //Listen
    listen(socket_desc , 3);
     
    //Accept and incoming connection
    puts("Waiting for incoming connections...");
    c = sizeof(struct sockaddr_in);
    while( (new_socket = accept(socket_desc, (struct sockaddr *)&client, (socklen_t*)&c)) >= 0 ){ // TODO: is >=0 necessary?
        pthread_t sniffer_thread;
        connection_data = (connection_data_t *)malloc(sizeof(connection_data_t));
        connection_data->verbose = verbose;
        connection_data->socket = new_socket;
        connection_data->endpoint = strdup(endpoint);

        puts("Connection accepted");

        printf("new_socket: %d\n", new_socket);
         
        if( pthread_create(&sniffer_thread, NULL,  connectionHandler, (void*)connection_data) < 0){
            perror("could not create thread");
            return 1;
        }
         
        //Now join the thread , so that we dont terminate before the thread
        //pthread_join( sniffer_thread , NULL);
        puts("Handler assigned");
    }
     
    if (new_socket < 0){
        perror("accept failed");
        return 1;
    }
     
    return 0;
}

/*
 * This will handle connection for each client
 * */
void *connectionHandler(void *arg)
{
    connection_data_t *connection_data = (connection_data_t *)arg;
    json_t *root;
    json_error_t error;
    char client_message[MESSAGE_SIZE]; // TODO: Accept variable length messages.
    const char *service, *message;
    int read_size;

    if(read_size = recv(connection_data->socket, client_message, MESSAGE_SIZE, 0) < 0){
        perror("recv failed");
        free(connection_data->endpoint);
        free(connection_data);
        return 0; // TODO: Should maybe be 1 or -1?
    }

    root = createJSONObject(client_message, &error);
    if(!root){
        free(connection_data->endpoint);
        free(connection_data);
        return 0;
    }

    extractJSONValue(root, JSON_MESSAGE, &message);

    if(extractJSONValue(root, JSON_SERVICE, &service)){
        fprintf(stderr, "error: service field does not exist\n");
        free(connection_data->endpoint);
        free(connection_data);
        json_decref(root);
        return 0; // No service field.
    }

    // If there isn't a message then this is a worker. Otherwise it is a client.
    if(!message){
        // New worker
        printf("Received worker message\n");
        executeWorker(connection_data, service);
    }
    else {
        // new client
        printf("Received client message\n");
        printf("First Mssage Received from Client: %s\n", client_message);
        executeClient(connection_data, service, message);

    }

    free(connection_data->endpoint);
    free(connection_data);
    json_decref(root);
    return 0;
}

int executeClient(connection_data_t *connection_data, const char *service, const char *message)
{
    int read_size;
    json_t *root;
    json_error_t error;
    kvesb_client_t *session = kvesb_client_new (connection_data->endpoint, connection_data->verbose);

    while(1){
        char *reply_message, request_message[MESSAGE_SIZE];
        int reply_message_length = 0;
        zmsg_t *request = zmsg_new();
        zmsg_t *reply;

        zmsg_pushstr(request, message);
        kvesb_client_send (session, service, &request);

        // Why must this be done twice?
        reply = kvesb_client_recv (session, NULL, NULL);
        reply = kvesb_client_recv (session, NULL, NULL);
        //zmsg_dump(reply);
        if (reply){
            reply_message = zmsg_popstr(reply);
            zmsg_destroy (&reply);
        }
        else{
            break;             //  Interrupted by Ctrl-C
        }

        root = json_object();
        json_object_set_new(root, JSON_MESSAGE, json_string(reply_message));
        free(reply_message);
        reply_message = json_dumps(root, 0);
        reply_message_length = strlen(reply_message);

        if(reply_message_length >= MESSAGE_SIZE){
            puts("Message too large");
            json_decref(root);
            free(reply_message);
            break;
        }

        printf("Sending to Client: %s\n", reply_message);
        write(connection_data->socket, reply_message, reply_message_length + 1);
        json_decref(root);
        free(reply_message);

        if((read_size = recv(connection_data->socket, request_message, MESSAGE_SIZE, 0)) <= 0){
            break;
        }
        printf("C: Received from Client: %s\n", request_message);

        root = createJSONObject(request_message, &error);
        if(!root){
            break;
        }
        extractJSONValue(root, JSON_MESSAGE, &message);
        extractJSONValue(root, JSON_SERVICE, &service);
    }

    kvesb_client_destroy (&session);
    return 0;
}

int executeWorker(connection_data_t *connection_data, const char *service)
{
    int read_size;
    kvesb_worker_t *session = kvesb_worker_new(connection_data->endpoint, service, connection_data->verbose);

    while(1){
        char *request_message, reply_message[MESSAGE_SIZE]; // TODO: Why is it 2*?
        int request_message_length = 0;
        json_t *root;
        json_error_t error;
        const char *message;
        zmsg_t *reply;
        zmsg_t *reply_to;
        zmsg_t *request = kvesb_worker_recv (session, &reply_to);

        if (request == NULL){
            break;              //  Worker was interrupted
        }

        request_message = zmsg_popstr(request);
        root = json_object();
        json_object_set_new(root, JSON_MESSAGE, json_string(request_message));
        free(request_message);
        request_message = json_dumps(root, 0);
        request_message_length = strlen(request_message);

        if(request_message_length > MESSAGE_SIZE){
            puts("Message too large");
            json_decref(root);
            free(request_message);
            break;
        }

        printf("Sending to Worker: %s\n", request_message);
        write(connection_data->socket, request_message, request_message_length + 1);
        json_decref(root);
        free(request_message);

        if((read_size = recv(connection_data->socket, reply_message, MESSAGE_SIZE, 0)) <= 0){
            zmsg_destroy (&reply_to);
            break;
        }
        printf("Received from Worker: %s\n", reply_message);

        root = createJSONObject(reply_message, &error);
        if(!root) {
            break;
        }
        extractJSONValue(root, JSON_MESSAGE, &message);

        reply = zmsg_new();
        zmsg_pushstr(reply, message);
        kvesb_worker_send (session, &reply, reply_to);
        zmsg_destroy (&reply_to);
        json_decref(root);
    }

    if(read_size == 0){
        puts("Client disconnected");
        fflush(stdout);
    }
    else if(read_size == -1){
        perror("recv failed!");
    }

    kvesb_worker_destroy (&session);
    return 0;
}

json_t* createJSONObject(const char *json_string, json_error_t *error)
{
    json_t *root;

    root = json_loads(json_string, 0, error);
    if(!root){
        // TODO: Send a reply indicating an error?
        fprintf(stderr, "error: on line %d: %s\n", error->line, error->text);
        return NULL;
    }

    if(!json_is_object(root)){
        fprintf(stderr, "error: JSON message is of improper format\n");
        json_decref(root);
        return NULL;
    }

    return root;
}

int extractJSONValue(json_t *root, const char *key, const char **value_text)
{
    json_t *value = json_object_get(root, key);
    if(!json_is_string(value)){
        *value_text = NULL;
        return 1;
    }
    *value_text = json_string_value(value);

    return 0;
}
