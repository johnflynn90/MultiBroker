/*  =========================================================================
    kvesb_common.h - KVESB common definitions
    =========================================================================
*/

#ifndef __KVESB_COMMON_H_INCLUDED__
#define __KVESB_COMMON_H_INCLUDED__

#include "czmq.h"

//  This is the version of KVESB/Client we implement
#define KVESBC_CLIENT         "KVESBC01"

//  KVESB/Client commands, as strings
#define KVESBC_REQUEST        "\001"
#define KVESBC_REPORT         "\002"
#define KVESBC_NAK            "\003"
#define KVESBC_UUID           "\004"
static char *kvesbc_commands [] = {
    NULL, "REQUEST", "REPORT", "NAK", "UUID",
};

//  This is the version of KVESB/Worker we implement
#define KVESBW_WORKER         "KVESBW01"

//  KVESB/Worker commands, as strings
#define KVESBW_READY          "\001"
#define KVESBW_REQUEST        "\002"
#define KVESBW_REPORT         "\003"
#define KVESBW_HEARTBEAT      "\004"
#define KVESBW_DISCONNECT     "\005"

static char *kvesbw_commands [] = {
    NULL, "READY", "REQUEST", "REPORT", "HEARTBEAT", "DISCONNECT"
};

//  This is the version of KVESB/Broker we implement
#define KVESBB_BROKER         "KVESBB01"

//  KVESB/Broker commands, as strings
#define KVESBB_READY          "\001"
#define KVESBB_REQUEST        "\002"
#define KVESBB_REPORT         "\003"
#define KVESBB_HEARTBEAT	  "\004"
#define KVESBB_DISCONNECT     "\005"
static char *kvesbb_commands [] = {
    NULL, "READY", "REQUEST", "REPORT", "HEARTBEAT", "DISCONNECT"
};

#endif

