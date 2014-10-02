/*  =========================================================================
    kvesb_client.h - client API

    =========================================================================
*/

#ifndef __KVESBCLIAPI_H_INCLUDED__
#define __KVESBCLIAPI_H_INCLUDED__

#ifdef __cplusplus
extern "C" {
#endif

//  Opaque class structure
typedef struct _kvesb_client_t kvesb_client_t;

//  @interface
CZMQ_EXPORT kvesb_client_t *
    kvesb_client_new (char *broker, int verbose);
CZMQ_EXPORT void
    kvesb_client_destroy (kvesb_client_t **self_p);
CZMQ_EXPORT void
    kvesb_client_set_timeout (kvesb_client_t *self, int timeout);
CZMQ_EXPORT int
    kvesb_client_setsockopt (kvesb_client_t *self, int option, const void *optval,
    size_t optvallen);
CZMQ_EXPORT int
    kvesb_client_getsockopt (kvesb_client_t *self, int option, void *optval,
    size_t *optvallen);
CZMQ_EXPORT void
    kvesb_client_send (kvesb_client_t *self, const char *service, zmsg_t **request_p);
CZMQ_EXPORT zmsg_t *
    kvesb_client_recv (kvesb_client_t *self, char **command_p, char **service_p);
//  @end

#ifdef __cplusplus
}
#endif

#endif
