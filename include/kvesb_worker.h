/*  =========================================================================
    kvesb_worker.h - worker API
    =========================================================================
*/

#ifndef __KVESBWRKAPI_H_INCLUDED__
#define __KVESBWRKAPI_H_INCLUDED__

#ifdef __cplusplus
extern "C" {
#endif

//  Opaque class structure
typedef struct _kvesb_worker_t kvesb_worker_t;

//  @interface
CZMQ_EXPORT kvesb_worker_t *
    kvesb_worker_new (char *broker, const char *service, int verbose);
CZMQ_EXPORT void
    kvesb_worker_destroy (kvesb_worker_t **self_p);
CZMQ_EXPORT void
    kvesb_worker_set_heartbeat (kvesb_worker_t *self, int heartbeat);
CZMQ_EXPORT void
    kvesb_worker_set_reconnect (kvesb_worker_t *self, int reconnect);
CZMQ_EXPORT int
    kvesb_worker_setsockopt (kvesb_worker_t *self, int option, const void *optval,
    size_t optvallen);
CZMQ_EXPORT int
    kvesb_worker_getsockopt (kvesb_worker_t *self, int option, void *optval,
    size_t *optvallen);
CZMQ_EXPORT zmsg_t *
    kvesb_worker_recv (kvesb_worker_t *self, zmsg_t **reply_p);
CZMQ_EXPORT void
    kvesb_worker_send (kvesb_worker_t *self, zmsg_t **progress_p,
                     zmsg_t *reply_to);
//  @end

#ifdef __cplusplus
}
#endif

#endif
