// test client for adapter
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h> //inet_addr
#include <jansson.h> // for JSON
 
int main(int argc , char *argv[])
{
    struct sockaddr_in server;
    int socket_desc;
    const int message_size = 1000;
    char *req_message;//[message_size], rep_message[2*message_size]; // TODO: Generalize lengths.
    char service[100], message[100];
    json_t *root, *data;

    socket_desc = socket(AF_INET , SOCK_STREAM , 0);
     
    if (socket_desc == -1)
    {
        printf("Could not create socket\n");
        return 0;
    }
    printf("Created socket!\n");

    server.sin_addr.s_addr = inet_addr("127.0.0.1");
    server.sin_family = AF_INET;
    server.sin_port = htons(8888);

    //Connect to remote server
    if (connect(socket_desc , (struct sockaddr *)&server , sizeof(server)) < 0)
    {
        puts("connect error");
        return 1;
    }
     
    puts("Connected");

	root = json_object();

    printf("Enter service and message: ");
    scanf("%99s %99s", service, message);
	
	//root = json_object(); // TODO: Why is there a segmentation fault if this is done here?

    data = json_string(service);
    json_object_set(root, "service", data);

    if(message[0] != 'x'){
        data = json_string(message);
        json_object_set(root, "message", data);
    }

    req_message = json_dumps(root, 0);
    printf("reg_msg: %s\n", req_message);

    // TODO: Find a way around this.
    if(strlen(req_message) > message_size)
    {
    	puts("Message too large");
    	return 1;
    }

    if(send(socket_desc, req_message, strlen(req_message), 0) < 0)
	{
		puts("Send failed");
		return 1;
	}

    json_decref(root);
	free(req_message);
    return 0;
}