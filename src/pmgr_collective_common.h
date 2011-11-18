/*
 * Copyright (c) 2009, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-411040.
 * All rights reserved.
 * This file is part of the PMGR_COLLECTIVE library.
 * For details, see https://sourceforge.net/projects/pmgrcollective.
 * Please also read this file: LICENSE.TXT.
*/

/*
 * PMGR_COLLECTIVE ============================================================
 * This protocol enables MPI to bootstrap itself through a series of collective
 * operations.  The collective operations are modeled after MPI collectives --
 * all tasks must call them in the same order and with consistent parameters.
 *
 * MPI may invoke any number of collectives, in any order, passing an arbitrary
 * amount of data.  All message sizes are specified in bytes.
 * PMGR_COLLECTIVE ============================================================
 *
 * This file provides common definitions for
 *   pmgr_collective_mpirun - the interface used by mpirun
 *   pmgr_collective_client - the interface used by the MPI tasks
*/

#ifndef _PMGR_COLLECTIVE_COMMON_H
#define _PMGR_COLLECTIVE_COMMON_H

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#if defined(_IA64_)
#undef htons
#undef ntohs
#define htons(__bsx) ((((__bsx) >> 8) & 0xff) | (((__bsx) & 0xff) << 8))
#define ntohs(__bsx) ((((__bsx) >> 8) & 0xff) | (((__bsx) & 0xff) << 8))
#endif

/* PMGR_VERSION for pmgr_collective is PMGR_COLLECTIVE (== 8) */
#define PMGR_COLLECTIVE 8

#define PMGR_SUCCESS 0
#define PMGR_FAILURE (!PMGR_SUCCESS)

#define PMGR_OPEN      0
#define PMGR_CLOSE     1
#define PMGR_ABORT     2
#define PMGR_BARRIER   3
#define PMGR_BCAST     4
#define PMGR_GATHER    5
#define PMGR_SCATTER   6
#define PMGR_ALLGATHER 7
#define PMGR_ALLTOALL  8

/*
   my rank
   -3     ==> unitialized task (may be mpirun or MPI task)
   -2     ==> mpirun
   -1     ==> MPI task before rank is assigned
   0..N-1 ==> MPI task
*/
extern int pmgr_me;

extern int pmgr_echo_debug;

/* Return the number of secs as a double between two timeval structs (tv2-tv1) */
double pmgr_getsecs(struct timeval* tv2, struct timeval* tv1);

/* Fills in timeval via gettimeofday */
void pmgr_gettimeofday(struct timeval* tv);

/* Reads environment variable, bails if not set */
#define ENV_REQUIRED 0
#define ENV_OPTIONAL 1
char* pmgr_getenv(char* envvar, int type);

/* malloc n bytes, and bail out with error msg if fails */
void* pmgr_malloc(size_t n, char* msg);

/* macro to free the pointer if set, then set it to NULL */
#define pmgr_free(p) { if(p) { free((void*)p); p=NULL; } }

/* print message to stderr */
void pmgr_error(char *fmt, ...);

/* print message to stderr */
void pmgr_debug(int leve, char *fmt, ...);

/* write size bytes from buf into fd, retry if necessary */
int pmgr_write_fd_suppress(int fd, const void* buf, int size, int suppress);

/* write size bytes from buf into fd, retry if necessary */
int pmgr_write_fd(int fd, const void* buf, int size);

/* read size bytes into buf from fd, retry if necessary */
int pmgr_read_fd_timeout(int fd, void* buf, int size, int usecs);

/* read size bytes into buf from fd, retry if necessary */
int pmgr_read_fd(int fd, void* buf, int size);

/* read size bytes into buf from fd, retry if necessary */
int pmgr_read_fd (int fd, void* buf, int size);

/* Open a connection on socket FD to peer at ADDR (which LEN bytes long).
 * This function uses a non-blocking filedescriptor for the connect(),
 * and then does a bounded poll() for the connection to complete.  This
 * allows us to timeout the connect() earlier than TCP might do it on
 * its own.  We have seen timeouts that failed after several minutes,
 * where we would really prefer to time out earlier and retry the connect.
 *
 * Return 0 on success, -1 for errors.
 */
int pmgr_connect_timeout_suppress(int fd, struct sockaddr_in* addr, int millisec, int suppress);

/* Connect to given IP:port.  Upon successful connection, pmgr_connect
 * shall return the connected socket file descriptor.  Otherwise, -1 shall be
 * returned.
 */
int pmgr_connect(struct in_addr ip, int port);

/* open a listening socket and return the descriptor, the ip address, and the port */
int pmgr_open_listening_socket(int* out_fd, struct in_addr* out_ip, short* out_port);

int pmgr_authenticate_accept(int fd, const char* connect_text, size_t connect_len, const char* accept_text, size_t accept_len, int reply_timeout);

/* issues a handshake across connection to verify we really connected to the right socket */
int pmgr_authenticate_connect(int fd, int rank, const char* hostname, int port, const char* connect_text, size_t connect_len, const char* accept_text, size_t accept_len, int reply_timeout);

/* Attempts to connect to a given hostname using a port list and timeouts */
int pmgr_connect_hostname(int rank, const char* hostname, const char* ports, const char* connect_text, size_t connect_len, const char* accept_text, size_t accept_len);

#endif /* _PMGR_COLLECTIVE_COMMON_H */
