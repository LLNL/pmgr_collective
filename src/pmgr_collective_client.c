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
 * This file implements the interface used by the MPI tasks (clients).
 *
 * An MPI task should make calls in the following sequenece:
 *
 *   pmgr_init
 *   pmgr_open
 *   [collectives]
 *   pmgr_close
 *   pmgr_finalize
 *
 * MPI may invoke any number of collectives, in any order, passing an arbitrary
 * amount of data.  All message sizes are specified in bytes.
 *
 * All functions return PMGR_SUCCESS on successful completion.
 *
 * Copyright (C) 2007 The Regents of the University of California.
 * Produced at Lawrence Livermore National Laboratory.
 * Author: Adam Moody <moody20@llnl.gov>
 */

/*
 * Copyright (C) 1999-2001 The Regents of the University of California
 * (through E.O. Lawrence Berkeley National Laboratory), subject to
 * approval by the U.S. Department of Energy.
 *
 * Use of this software is under license. The license agreement is included
 * in the file MVICH_LICENSE.TXT.
 *
 * Developed at Berkeley Lab as part of MVICH.
 *
 * Authors: Bill Saphir      <wcsaphir@lbl.gov>
 *          Michael Welcome  <mlwelcome@lbl.gov>
 */

/* Copyright (c) 2002-2007, The Ohio State University. All rights
 * reserved.
 *
 * This file is part of the MVAPICH software package developed by the
 * team members of The Ohio State University's Network-Based Computing
 * Laboratory (NBCL), headed by Professor Dhabaleswar K. (DK) Panda.
 *
 * For detailed copyright and licensing information, please refer to the
 * copyright file COPYRIGHT_MVAPICH in the top level MPICH directory.
 */

#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <netdb.h>
#include <errno.h>
#include <stdarg.h>
#include "pmgr_collective_client.h"

/* set env variable to select which trees to use, if any -- all enabled by default */
#ifndef MPIRUN_USE_TREES
#define MPIRUN_USE_TREES (1)
#endif
#ifndef MPIRUN_USE_GATHER_TREE
#define MPIRUN_USE_GATHER_TREE (1)
#endif
#ifndef MPIRUN_USE_BCAST_TREE
#define MPIRUN_USE_BCAST_TREE (1)
#endif
int mpirun_use_trees       = MPIRUN_USE_TREES;       /* set envvar MPIRUN_USE_TREES={0,1} to disable/enable tree algorithms */
int mpirun_use_gather_tree = MPIRUN_USE_GATHER_TREE; /* set envvar MPIRUN_USE_GATHER_TREE={0,1} to disable/enable gather tree */
int mpirun_use_bcast_tree  = MPIRUN_USE_BCAST_TREE;  /* set envvar MPIRUN_USE_BCAST_TREE={0,1} to disable/enable bcast tree */

char* mpirun_hostname;
int   mpirun_port;
int   mpirun_socket;
int   pmgr_nprocs = -1;
/*int   pmgr_me     = -1; */ /* moved to pmgr_collective_client */
int   pmgr_id     = -1;

/* tree data structures */
int  pmgr_parent;         /* MPI rank of parent */
int  pmgr_parent_s;       /* socket fd to parent */
int* pmgr_child;          /* MPI ranks of children */
int* pmgr_child_s;        /* socket fds to children */
int  pmgr_num_child;      /* number of children */
int* pmgr_child_incl;     /* number of children each child is responsible for (includes itself) */
int  pmgr_num_child_incl; /* total number of children this node is responsible for */

/*
 * =============================
 * Utility functions for use by other functions in this file
 * =============================
 */

/* read size bytes into buf from mpirun_socket */
int pmgr_read(void* buf, int size)
{
    return pmgr_read_fd(mpirun_socket, buf, size);
}

/* write size bytes into mpirun_socket from buf */
int pmgr_write(void* buf, int size)
{
    return pmgr_write_fd(mpirun_socket, buf, size);
}

/* write integer into mpirun_socket */
int pmgr_write_int(int value)
{
    return pmgr_write(&value, sizeof(value));
}

/* 
 * =============================
 * The mpirun_* functions implement PMGR_COLLECTIVE operations through
 * the mpirun process.  Typically, this amounts to a flat tree with the
 * mpirun process at the root.  These functions implement the client side
 * of the protocol specified in pmgr_collective_mpirun.c.
 * =============================
 */

/*
 * Perform barrier, each task writes an int then waits for an int
 */
int mpirun_barrier()
{
    /* send BARRIER op code, then wait on integer reply */
    int buf;

    pmgr_write_int(PMGR_BARRIER);
    pmgr_read(&buf, sizeof(int));

    return PMGR_SUCCESS;
}

/*
 * Perform MPI-like Broadcast, root writes sendcount bytes from buf,
 * into mpirun_socket, all receive sendcount bytes into buf
 */
int mpirun_bcast(void* buf, int sendcount, int root)
{
    /* send BCAST op code, then root, then size of data */
    pmgr_write_int(PMGR_BCAST);
    pmgr_write_int(root);
    pmgr_write_int(sendcount);

    /* if i am root, send data */
    if (pmgr_me == root) {
      pmgr_write(buf, sendcount);
    }

    /* read in data */
    pmgr_read(buf, sendcount);

    return PMGR_SUCCESS;
}

/*
 * Perform MPI-like Gather, each task writes sendcount bytes from sendbuf
 * into mpirun_socket, then root receives N*sendcount bytes into recvbuf
 */
int mpirun_gather(void* sendbuf, int sendcount, void* recvbuf, int root)
{
    /* send GATHER op code, then root, then size of data, then data itself */
    pmgr_write_int(PMGR_GATHER);
    pmgr_write_int(root);
    pmgr_write_int(sendcount);
    pmgr_write(sendbuf, sendcount);

    /* only the root receives data */
    if (pmgr_me == root) {
       pmgr_read(recvbuf, sendcount * pmgr_nprocs);
    }

    return PMGR_SUCCESS;
}

/*
 * Perform MPI-like Scatter, root writes N*sendcount bytes from sendbuf
 * into mpirun_socket, then each task receives sendcount bytes into recvbuf
 */
int mpirun_scatter(void* sendbuf, int sendcount, void* recvbuf, int root)
{
    /* send SCATTER op code, then root, then size of data, then data itself */
    pmgr_write_int(PMGR_SCATTER);
    pmgr_write_int(root);
    pmgr_write_int(sendcount);

    /* if i am root, send all chunks to mpirun */
    if (pmgr_me == root) {
      pmgr_write(sendbuf, sendcount * pmgr_nprocs);
    }

    /* receive my chunk */
    pmgr_read(recvbuf, sendcount);

    return PMGR_SUCCESS;
}

/*
 * Perform MPI-like Allgather, each task writes sendcount bytes from sendbuf
 * into mpirun_socket, then receives N*sendcount bytes into recvbuf
 */
int mpirun_allgather(void* sendbuf, int sendcount, void* recvbuf)
{
    /* send ALLGATHER op code, then size of data, then data itself */
    pmgr_write_int(PMGR_ALLGATHER);
    pmgr_write_int(sendcount);
    pmgr_write(sendbuf, sendcount);
    pmgr_read (recvbuf, sendcount * pmgr_nprocs);

    return PMGR_SUCCESS;
}

/*
 * Perform MPI-like Alltoall, each task writes N*sendcount bytes from sendbuf
 * into mpirun_socket, then recieves N*sendcount bytes into recvbuf
 */
int mpirun_alltoall(void* sendbuf, int sendcount, void* recvbuf)
{
    /* send ALLTOALL op code, then size of data, then data itself */
    pmgr_write_int(PMGR_ALLTOALL);
    pmgr_write_int(sendcount);
    pmgr_write(sendbuf, sendcount * pmgr_nprocs);
    pmgr_read (recvbuf, sendcount * pmgr_nprocs);

    return PMGR_SUCCESS;
}

/* 
 * =============================
 * Functions to open/close/gather/bcast the TCP/socket tree.
 * =============================
*/

/* connect to given IP:port and return opened socket file descriptor */
int pmgr_connect(struct in_addr ip, int port)
{
    /* set up address to connect to */
    struct sockaddr_in sockaddr;
    sockaddr.sin_family = AF_INET;
    sockaddr.sin_addr = ip;
    sockaddr.sin_port = port;

    /* create a socket */
    int sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sockfd < 0) {
        pmgr_error("%d: Creating socket: (socket() errno %d): file %s line %d",
            pmgr_me, errno, __FILE__, __LINE__);
        exit(1);
    }

    /* connect socket to address */
    if (connect(sockfd, (struct sockaddr *) &sockaddr, sizeof(sockaddr)) < 0) {
        pmgr_error("%d: Connecting socket (connect() errno %d): file %s line %d",
            pmgr_me, errno, __FILE__, __LINE__);
        exit(1);
    }

    return sockfd;
}

/* open socket tree across MPI tasks */
int pmgr_open_tree()
{
    /* currently implements a binomial tree */

    /* initialize parent and children based on pmgr_me and pmgr_nprocs */
    int n = 1;
    int max_children = 0;
    while (n < pmgr_nprocs) {
      n <<= 1;
      max_children++;
    }

    pmgr_parent = 0;
    pmgr_num_child = 0;
    pmgr_num_child_incl = 0;
    pmgr_child      = (int*) pmgr_malloc(max_children * sizeof(int), "Child MPI rank array");
    pmgr_child_s    = (int*) pmgr_malloc(max_children * sizeof(int), "Child socket fd array");
    pmgr_child_incl = (int*) pmgr_malloc(max_children * sizeof(int), "Child children count array");

    /* find our parent and list of children */
    int low  = 0;
    int high = pmgr_nprocs - 1;
    while (high - low > 0) {
        int mid = (high - low) / 2 + (high - low) % 2 + low;
        if (low == pmgr_me) {
            pmgr_child[pmgr_num_child] = mid;
            pmgr_child_incl[pmgr_num_child] = high - mid + 1;
            pmgr_num_child++;
            pmgr_num_child_incl += (high - mid + 1);
        }
        if (mid == pmgr_me) { pmgr_parent = low; }
        if (mid <= pmgr_me) { low  = mid; }
        else                { high = mid-1; }
    }

    /* create a socket to accept connection from parent */
    int sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sockfd < 0) {
        pmgr_error("%d: Creating parent socket (socket() errno %d): file %s line %d",
            pmgr_me, errno, __FILE__, __LINE__);
        exit(1);
    }

    struct sockaddr_in sin;
    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = htonl(INADDR_ANY);
    sin.sin_port = htons(0); /* bind ephemeral port - OS will assign us a free port */

    /* bind socket */
    if (bind(sockfd, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
        pmgr_error("%d: Binding parent socket (bind() errno %d): file %s line %d",
            pmgr_me, errno, __FILE__, __LINE__);
        exit(1);
    }

    /* set the socket to listen for connections */
    if (listen(sockfd, 1) < 0) {
        pmgr_error("%d: Setting parent socket to listen (listen() errno %d): file %s line %d",
            pmgr_me, errno, __FILE__, __LINE__);
        exit(1);
    }

    /* ask which port the OS assigned our socket to */
    socklen_t len = sizeof(sin);
    if (getsockname(sockfd, (struct sockaddr *) &sin, &len) < 0) {
        pmgr_error("%d: Reading parent socket port number (getsockname() errno %d): file %s line %d",
            pmgr_me, errno, __FILE__, __LINE__);
        exit(1);
    }

    /* extract our ip and port number to send to mpirun */
    char hn[256];
    gethostname(hn, 256);
    struct hostent* he = gethostbyname(hn);
    struct in_addr ip = * (struct in_addr *) *(he->h_addr_list);
    short port = sin.sin_port;

    /* allocate buffers to receive ip:port table for all tasks */
    int sendcount = sizeof(ip) + sizeof(port);
    void* sendbuf = (void*) pmgr_malloc(sendcount, "Send buffer for socket data");
    void* recvbuf = (void*) pmgr_malloc(sendcount * pmgr_nprocs, "Receive buffer for socket table");

    /* fill in send buffer with our ip:port */
    memcpy(sendbuf, &ip, sizeof(ip));
    memcpy((char*)sendbuf + sizeof(ip), &port, sizeof(port));

    /* gather ip:port info to rank 0 -- explicitly call mpirun_gather since tcp tree is not setup */
    mpirun_gather(sendbuf, sendcount, recvbuf, 0);

    /* if i'm not rank 0, accept a connection (from parent) and receive socket table */
    if (pmgr_me != 0) {
        socklen_t parent_len;
        struct sockaddr parent_addr;
	parent_len = sizeof(parent_addr);
        pmgr_parent_s = accept(sockfd, (struct sockaddr *) &parent_addr, &parent_len);
        pmgr_read_fd(pmgr_parent_s, recvbuf, sendcount * pmgr_nprocs);
    }

    /* for each child, open socket connection and forward socket table */
    int i;
    for(i=0; i<pmgr_num_child; i++) {
        int c = pmgr_child[i];
        struct in_addr child_ip = * (struct in_addr *)  ((char*)recvbuf + sendcount*c);
        short child_port = * (short*) ((char*)recvbuf + sendcount*c + sizeof(ip));
        pmgr_child_s[i] = pmgr_connect(child_ip, child_port);
        pmgr_write_fd(pmgr_child_s[i], recvbuf, sendcount * pmgr_nprocs);
    }

    pmgr_free(sendbuf);
    pmgr_free(recvbuf);

    return PMGR_SUCCESS;
}

/*
 * close down socket connections for tree (parent and any children), free
 * related memory
 */
int pmgr_close_tree()
{
    /* if i'm not rank 0, close socket connection with parent */
    if (pmgr_me != 0) {
        close(pmgr_parent_s);
    }

    /* and all children */
    int i;
    for(i=0; i<pmgr_num_child; i++) {
        close(pmgr_child_s[i]);
    }

    /* free data structures */
    pmgr_free(pmgr_child);
    pmgr_free(pmgr_child_s);
    pmgr_free(pmgr_child_incl);

    return PMGR_SUCCESS;
}

/* broadcast size bytes from buf on rank 0 using socket tree */
int pmgr_bcast_tree(void* buf, int size)
{
    int i;

    /* if i'm not rank 0, receive data from parent */
    if (pmgr_me != 0) {
        pmgr_read_fd(pmgr_parent_s, buf, size);
    }

    /* for each child, forward data */
    for(i=0; i<pmgr_num_child; i++) {
        pmgr_write_fd(pmgr_child_s[i], buf, size);
    }

    return PMGR_SUCCESS;
}

/* gather sendcount bytes from sendbuf on each task into recvbuf on rank 0 */
int pmgr_gather_tree(void* sendbuf, int sendcount, void* recvbuf)
{
    int bigcount = (pmgr_num_child_incl+1) * sendcount;
    void* bigbuf = recvbuf;

    /* if i'm not rank 0, create a temporary buffer to gather child data */
    if (pmgr_me != 0) {
        bigbuf = (void*) pmgr_malloc(bigcount, "Temporary gather buffer in pmgr_gather_tree");
    }

    /* copy my own data into buffer */
    memcpy(bigbuf, sendbuf, sendcount);

    /* if i have any children, receive their data */
    int i;
    int offset = sendcount;
    for(i=pmgr_num_child-1; i>=0; i--) {
        pmgr_read_fd(pmgr_child_s[i], (char*)bigbuf + offset, sendcount * pmgr_child_incl[i]);
        offset += sendcount * pmgr_child_incl[i];
    }

    /* if i'm not rank 0, send to parent and free temporary buffer */
    if (pmgr_me != 0) {
        pmgr_write_fd(pmgr_parent_s, bigbuf, bigcount);
        pmgr_free(bigbuf);
    }

    return PMGR_SUCCESS;
}

/*
 * =============================
 * The pmgr_* collectives are the user interface (what the MPI tasks call).
 * =============================
 */

/* Perform barrier, each task writes an int then waits for an int */
int pmgr_barrier()
{
    char c;
    void* recvbuf = NULL;

    if (mpirun_use_trees) {
        /* allocate temporary gather buffer on rank 0 */
        if (pmgr_me == 0) {
            recvbuf = (void*) pmgr_malloc(sizeof(c) * pmgr_nprocs, "Temporary gather buffer in pmgr_barrier");
        }

        /* gather a character to rank 0 */
        if (mpirun_use_gather_tree) {
            pmgr_gather_tree(&c, sizeof(c), recvbuf);
	} else {
            mpirun_gather(&c, sizeof(c), recvbuf, 0);
	}

        /* broadcast a character from rank 0 */
        if (mpirun_use_bcast_tree) {
            pmgr_bcast_tree(&c, sizeof(c));
	} else {
            mpirun_bcast(&c, sizeof(c), 0);
	}

        /* free temporary gather buffer on rank 0 */
        if (pmgr_me == 0) {
            pmgr_free(recvbuf);
        }
    } else {
        /* trees aren't enabled, use mpirun to do barrier */
        mpirun_barrier();
    }

    return PMGR_SUCCESS;
}

/*
 * Perform MPI-like Broadcast, root writes sendcount bytes from buf,
 * into mpirun_socket, all receive sendcount bytes into buf
 */
int pmgr_bcast(void* buf, int sendcount, int root)
{
    /* if root is rank 0 and bcast tree is enabled, use it */
    /* (this is a common case) */
    if (root == 0 && mpirun_use_trees && mpirun_use_bcast_tree) {
        return pmgr_bcast_tree(buf, sendcount);
    }

    return mpirun_bcast(buf, sendcount, root);
}

/*
 * Perform MPI-like Gather, each task writes sendcount bytes from sendbuf
 * into mpirun_socket, then root receives N*sendcount bytes into recvbuf
 */
int pmgr_gather(void* sendbuf, int sendcount, void* recvbuf, int root)
{
    /* if root is rank 0 and gather tree is enabled, use it */
    /* (this is a common case) */
    if (root == 0 && mpirun_use_trees && mpirun_use_gather_tree) {
        return pmgr_gather_tree(sendbuf, sendcount, recvbuf);
    }

    return mpirun_gather(sendbuf, sendcount, recvbuf, root);
}

/*
 * Perform MPI-like Scatter, root writes N*sendcount bytes from sendbuf
 * into mpirun_socket, then each task receives sendcount bytes into recvbuf
 */
int pmgr_scatter(void* sendbuf, int sendcount, void* recvbuf, int root)
{
    return mpirun_scatter(sendbuf, sendcount, recvbuf, root);
}

/*
 * Perform MPI-like Allgather, each task writes sendcount bytes from sendbuf
 * into mpirun_socket, then receives N*sendcount bytes into recvbuf
 */
int pmgr_allgather(void* sendbuf, int sendcount, void* recvbuf)
{
    if (mpirun_use_trees) {
        /* gather data to rank 0 */
        if (mpirun_use_gather_tree) {
            pmgr_gather_tree(sendbuf, sendcount, recvbuf);
	} else {
            mpirun_gather(sendbuf, sendcount, recvbuf, 0);
	}

        /* broadcast data from rank 0 */
        if (mpirun_use_bcast_tree) {
            pmgr_bcast_tree(recvbuf, sendcount * pmgr_nprocs);
	} else {
            mpirun_bcast(recvbuf, sendcount * pmgr_nprocs, 0);
	}
    } else {
        /* trees aren't enabled, use mpirun to do allgather */
        mpirun_allgather(sendbuf, sendcount, recvbuf);
    }

    return PMGR_SUCCESS;
}

/*
 * Perform MPI-like Alltoall, each task writes N*sendcount bytes from sendbuf
 * into mpirun_socket, then recieves N*sendcount bytes into recvbuf
 */
int pmgr_alltoall(void* sendbuf, int sendcount, void* recvbuf)
{
    return mpirun_alltoall(sendbuf, sendcount, recvbuf);
}

/*
 * Open socket to mpirun launch process, then send protocol version and rank
 * number
 */
int pmgr_open()
{
    struct sockaddr_in sockaddr;

    mpirun_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (mpirun_socket < 0) {
        pmgr_error("%d: Creating mpirun socket (socket() errno %d): file %s line %d",
            pmgr_me, errno, __FILE__, __LINE__);
        exit(1);
    }

    struct hostent* mpirun_hostent = gethostbyname(mpirun_hostname);
    if (!mpirun_hostent) {
        pmgr_error("%d: gethostbyname(%s) failed: %s (h_errno %d): file %s line %d",
            pmgr_me, mpirun_hostname, hstrerror(h_errno), h_errno, __FILE__, __LINE__);
        exit(1);
    }

    sockaddr.sin_family = AF_INET;
    sockaddr.sin_addr = *(struct in_addr *) (*mpirun_hostent->h_addr_list);
    sockaddr.sin_port = htons(mpirun_port);

    if (connect(mpirun_socket, (struct sockaddr *) &sockaddr, sizeof(sockaddr)) < 0) {
        pmgr_error("%d: Connecting mpirun socket (conenct() errno %d): file %s line %d",
            pmgr_me, errno, __FILE__, __LINE__);
        exit(1);
    }

    /* we are now connected to the mpirun process */

    /* 
     * Exchange information with mpirun.  If you make any changes
     * to this protocol, be sure to increment the version number
     * in the header file.  This is to permit compatibility with older
     * executables.
     */

    /* send version number, then rank */
    pmgr_write_int(PMGR_COLLECTIVE);
    pmgr_write(&pmgr_me, sizeof(pmgr_me));

    /* open up socket tree, if enabled */
    if (mpirun_use_trees) {
        pmgr_open_tree();
    }

    return PMGR_SUCCESS;
}

/*
 * Closes the mpirun socket
 */
int pmgr_close()
{
    /* shut down the tree, if enabled */
    if (mpirun_use_trees) {
        pmgr_close_tree();
    }

    /* send CLOSE op code, then close socket */
    pmgr_write_int(PMGR_CLOSE);
    close(mpirun_socket);

    return PMGR_SUCCESS;
}

/*
 * =============================
 * Handle init and finalize
 * =============================
 */

int pmgr_init(int *argc_p, char ***argv_p, int *np_p, int *me_p,
                     int *id_p, char ***processes_p)
{
    char *str;
    char *str_token;
    char **pmgr_processes = NULL;
    int i;
    setvbuf(stdout, NULL, _IONBF, 0);
    char *value;

    pmgr_me = -1;
    pmgr_echo_debug = 0;

    /* Get information from environment, not from the argument list */

    /* MPIRUN_DEBUG={0,1} disables/enables debug statements */
    if ((value = pmgr_getenv("MPIRUN_DEBUG", ENV_OPTIONAL)) != NULL) {
        pmgr_echo_debug = atoi(value);
    }

    /* MPI rank of current process */
    pmgr_me = atoi(pmgr_getenv("MPIRUN_RANK", ENV_REQUIRED));

    /* number of MPI processes in job */
    pmgr_nprocs = atoi(pmgr_getenv("MPIRUN_NPROCS", ENV_REQUIRED));

    /* unique jobid of current application */
    pmgr_id = atoi(pmgr_getenv("MPIRUN_ID", ENV_REQUIRED));

    /* mpirun host IP string in dotted decimal notation */
    mpirun_hostname = strdup(pmgr_getenv("MPIRUN_HOST", ENV_REQUIRED));

    /* mpirun port number */
    mpirun_port = atoi(pmgr_getenv("MPIRUN_PORT", ENV_REQUIRED));

    /* check that we have a valid number of processes */
    if (pmgr_nprocs <= 0) {
        pmgr_error("%d: Invalid MPIRUN_NPROCS %s: file %s line %d",
            pmgr_me, pmgr_getenv("MPIRUN_NPROCS", ENV_REQUIRED), __FILE__, __LINE__);
        exit(1);
    }

    /* check that our rank is valid */
    if (pmgr_me < 0 || pmgr_me >= pmgr_nprocs) {
        pmgr_error("%d: Invalid MPIRUN_RANK %s: file %s line %d",
            pmgr_me, pmgr_getenv("MPIRUN_RANK", ENV_REQUIRED), __FILE__, __LINE__);
        exit(1);
    }

    /* check that we have a valid jobid */
    if (pmgr_id == 0) {
        pmgr_error("%d: Invalid MPIRUN_ID %s: file %s line %d",
            pmgr_me, pmgr_getenv("MPIRUN_ID", ENV_REQUIRED), __FILE__, __LINE__);
        exit(1);
    }

    /* check that we have a valid mpirun port */
    if (mpirun_port <= 0) {
        pmgr_error("%d: Invalid MPIRUN_PORT %s: file %s line %d",
            pmgr_me, pmgr_getenv("MPIRUN_PORT", ENV_REQUIRED), __FILE__, __LINE__);
        exit(1);
    }

    /* list of hostnames running processes in job */
    if ((value = pmgr_getenv("NOT_USE_TOTALVIEW", ENV_OPTIONAL)) == NULL) {
        pmgr_processes = (char **) pmgr_malloc((size_t)pmgr_nprocs * sizeof(char*), "Process list");

        str = strdup(pmgr_getenv("MPIRUN_PROCESSES", ENV_REQUIRED));
        if (str == NULL) {
            pmgr_error("%d: Can't allocate process list (strdup() errno %d): file %s line %d",
                pmgr_me, errno, __FILE__, __LINE__);
            exit(1);
        }

        for (i = 0; i < pmgr_nprocs; i++) {
            if (!str) {
                pmgr_error("%d: Invalid MPIRUN process list: '%s': file %s line %d",
                    pmgr_me, pmgr_getenv("MPIRUN_PROCESSES", ENV_REQUIRED), __FILE__, __LINE__);
                exit(1);
            }

            str_token = strchr(str, ':');
            if(str_token) *str_token = ' ';

            pmgr_processes[i] = str;
            str = strchr(str, ' ');
            if (str) {
                *str = '\0';
                str++;
            }
        }
    }
 
    /* MPIRUN_USE_TREES={0,1} disables/enables tree algorithms */
    if ((value = pmgr_getenv("MPIRUN_USE_TREES", ENV_OPTIONAL))) {
        mpirun_use_trees = atoi(value);
    }

    /* MPIRUN_USE_GATHER_TREE={0,1} disables/enables gather tree */
    if ((value = pmgr_getenv("MPIRUN_USE_GATHER_TREE", ENV_OPTIONAL))) {
        mpirun_use_gather_tree = atoi(value);
    }

    /* MPIRUN_USE_BCAST_TREE={0,1} disables/enables bcast tree */
    if ((value = pmgr_getenv("MPIRUN_USE_BCAST_TREE", ENV_OPTIONAL))) {
        mpirun_use_bcast_tree = atoi(value);
    }

    *np_p = pmgr_nprocs;
    *me_p = pmgr_me;
    *id_p = pmgr_id;
    *processes_p = pmgr_processes;

    return PMGR_SUCCESS;
}

/*
 * No cleanup necessary here.
 */
int pmgr_finalize()
{
    return PMGR_SUCCESS;
}

/*
 * =============================
 * Handle aborts
 * =============================
 */

int vprint_msg(char *buf, size_t len, const char *fmt, va_list ap)
{
    int n;

    n = vsnprintf(buf, len, fmt, ap);

    if ((n >= len) || (n < 0)) {
        /* Add trailing '+' to indicate truncation */
        buf[len - 2] = '+';
        buf[len - 1] = '\0';
    }

    return (0);
}

/*
 * Call into the process spawner, using the same port we were given
 * at startup time, to tell it to abort the entire job.
 */
int pmgr_abort(int code, const char *fmt, ...)
{
    int s;
    struct sockaddr_in sin;
    struct hostent* he;
    va_list ap;
    char buf [256];
    int len;

    he = gethostbyname(mpirun_hostname);
    if (!he) {
        pmgr_error("%d: pmgr_abort: gethostbyname(%s) failed: %s (h_errno %d): file %s line %d",
            pmgr_me, mpirun_hostname, hstrerror(h_errno), h_errno, __FILE__, __LINE__);
        return -1;
    }

    s = socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0) {
        pmgr_error("%d: pmgr_abort: Failed to create socket: (socket() errno %d): file %s line %d",
            pmgr_me, errno, __FILE__, __LINE__);
        return -1;
    }

    memset(&sin, 0, sizeof(sin));
    sin.sin_family = he->h_addrtype;
    memcpy(&sin.sin_addr, he->h_addr_list[0], sizeof(sin.sin_addr));
    sin.sin_port = htons(mpirun_port);
    if (connect(s, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
        pmgr_error("%d: pmgr_abort: Connect to mpirun failed (connect() errno %d): file %s line %d",
            pmgr_me, errno, __FILE__, __LINE__);
        return -1;
    }

    va_start(ap, fmt);
    vprint_msg(buf, sizeof(buf), fmt, ap);
    va_end(ap);

    /* write an abort code (may be destination rank), our rank to mpirun */
    pmgr_write_fd(s, &code, sizeof(code));
    pmgr_write_fd(s, &pmgr_me, sizeof(pmgr_me));

    /* now length of error string, and error string itself to mpirun */
    len = strlen(buf) + 1;
    pmgr_write_fd(s, &len, sizeof(len));
    pmgr_write_fd(s, buf, len);

    close(s);

    return PMGR_SUCCESS;
}
