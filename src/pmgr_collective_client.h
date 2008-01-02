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
 * This file defines the interface used by the MPI tasks (clients).
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

#ifndef _PMGR_COLLECTIVE_CLIENT_H
#define _PMGR_COLLECTIVE_CLIENT_H

#include "pmgr_collective_common.h"

int pmgr_open ();
int pmgr_close();

/* sync point, no task makes it past until all have reached */
int pmgr_barrier  ();

/* root sends sendcount bytes from buf, each task recevies sendcount bytes into buf */
int pmgr_bcast    (void* buf, int sendcount, int root);

/* each task sends sendcount bytes from buf, root receives N*sendcount bytes into recvbuf */
int pmgr_gather   (void* sendbuf, int sendcount, void* recvbuf, int root);

/* root sends blocks of sendcount bytes to each task indexed from sendbuf */
int pmgr_scatter  (void* sendbuf, int sendcount, void* recvbuf, int root);

/* each task sends sendcount bytes from sendbuf and receives N*sendcount bytes into recvbuf */
int pmgr_allgather(void* sendbuf, int sendcount, void* recvbuf);

/* each task sends N*sendcount bytes from sendbuf and receives N*sendcount bytes into recvbuf */
int pmgr_alltoall (void* sendbuf, int sendcount, void* recvbuf);

/*
 * This function is called by each process in the job during
 * initialization.  Pointers to argc and argv are passes
 * in the event that the process manager passed args on
 * the command line.
 * The following values are filled in:
 *    *np_p     = total number of processes in the job
 *    *me_p     = the rank of this process (zero based)
 *    *id_p     = the global ID associated with this job.
 */
int pmgr_init(int *argc_p, char ***argv_p,
	int *np_p, int *me_p, int *id_p,
	char ***processes_p);

int pmgr_finalize(void);

int pmgr_abort(int code, const char *fmt, ...);

#endif /* _PMGR_COLLECTIVE_CLIENT_H */
