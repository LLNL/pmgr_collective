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
 * This file implements the interface used by mpirun.  The mpirun process should call
 * pmgr_processops after accepting connections from the MPI tasks and negotiating
 * the protocol version number (PMGR_COLLECTIVE uses protocol 8).
 *
 * It should provide an array of open socket file descriptors indexed by MPI rank
 * (fds) along with the number of MPI tasks (nprocs) as arguments.
 *
 * pmgr_processops will handle all PMGR_COLLECTIVE operations and return control
 * upon an error or after receiving PMGR_CLOSE from the MPI tasks.  If no errors
 * are encountered, it will close all socket file descriptors before returning.
 *
 * Copyright (C) 2007 The Regents of the University of California.
 * Produced at Lawrence Livermore National Laboratory.
 * Author: Adam Moody <moody20@llnl.gov>
*/

#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/time.h>
#include "pmgr_collective_mpirun.h"

int* fd_by_rank;
int  N;

double pmgr_getsecs(struct timeval* tv2, struct timeval* tv1)
{
	struct timeval result;
	timersub(tv2, tv1, &result);
	return (double) result.tv_sec + (double) result.tv_usec / 1000000.0;
}

void pmgr_gettimeofday(struct timeval* tv)
{
	if (gettimeofday(tv, NULL) < 0) {
		pmgr_error("getting time (gettimeofday() %m errno %d)",
			errno);
	}
}

/* Write size bytes from buf into socket for rank */
void pmgr_send(void* buf, int size, int rank)
{
	int fd = fd_by_rank[rank];
	if (pmgr_write_fd(fd, buf, size) < 0) {
		pmgr_error("writing to rank %d (write() %m errno %d) @ file %s:%d",
			rank, errno, __FILE__, __LINE__);
	}
}

/* Read size bytes from socket for rank into buf */
void pmgr_recv(void* buf, int size, int rank)
{
	int fd = fd_by_rank[rank];
	if (pmgr_read_fd(fd, buf, size) <= 0) {
		pmgr_error("reading from rank %d (read() %m errno %d) @ file %s:%d",
			rank, errno, __FILE__, __LINE__);
	}
}

/* Read an integer from socket for rank */
int pmgr_recv_int(int rank)
{
	int buf;
	pmgr_recv(&buf, sizeof(buf), rank);
	return buf;
}

/* Scatter data in buf to ranks using chunks of size bytes */
void pmgr_scatterbcast(void* buf, int size)
{
	int i;
	for (i = 0; i < N; i++) {
		pmgr_send(buf + i*size, size, i);
	}
}

/* Broadcast buf, which is size bytes big, to each rank */
void pmgr_allgatherbcast(void* buf, int size)
{
	int i;
	for (i = 0; i < N; i++) {
		pmgr_send(buf, size, i);
	}
}

/* Perform alltoall using data in buf with elements of size bytes */
void pmgr_alltoallbcast(void* buf, int size)
{
	int pbufsize = size * N;
	void* pbuf = (void*) pmgr_malloc(pbufsize, "Temporary buffer for alltoall");	

	int i, src;
	for (i = 0; i < N; i++) {
		for (src = 0; src < N; src++) {
			memcpy( pbuf + size*src,
				buf  + size*(src*N + i),
				size );
		}
		pmgr_send(pbuf, pbufsize, i);
	}
	
	pmgr_free(pbuf);
}

/* Check that new == curr value if curr has been initialized (-1 == uninitialized) */
int set_current(int curr, int new)
{
	if (curr == -1) { curr = new; }
	if (new != curr) {
		pmgr_error("unexpected value: received %d, expecting %d @ file %s:%d",
			   new, curr, __FILE__, __LINE__);
	}
	return curr;
}

/*
 * pmgr_processops
 * This function carries out pmgr_collective operations to bootstrap MPI.
 * These collective operations are modeled after MPI collectives -- all tasks
 * must call them in the same order and with consistent parameters.
 *
 * fds - integer array of open sockets (file descriptors)
 *       indexed by MPI rank
 * nprocs - number of MPI tasks in job
 *
 * returns PMGR_SUCCESS on success
 * If no errors are encountered, all sockets are closed before returning.
 *
 * Until a 'CLOSE' or 'ABORT' message is seen, we continuously loop processing ops
 *   For each op, we read one packet from each rank (socket)
 *     A packet consists of an integer OP CODE, followed by variable length data
 *     depending on the operation
 *   After reading a packet from each rank, mpirun completes the operation by broadcasting
 *   data back to any destinations, depending on the operation being performed
 *
 * Note: Although there are op codes available for PMGR_OPEN and PMGR_ABORT, neither
 * is fully implemented and should not be used.
 *
 * This function assumes there is a set of open sockets (file descriptors) to each MPI
 * task which can be indexed by MPI rank.
 *
 * Packet structures:
 *   N    ==> Number of MPI tasks
 *   From ==> data mpirun receives from each MPI task
 *   To   ==> data mpirun sends to each MPI task
 *   NULL ==> no data sent sent / recevied
 * 
 *   Message sizes are always in bytes and give number
 *   of bytes in vector for each process, not necessarily
 *   the total number of bytes included in the packet.
 *   The message size and collective operation together
 *   imply the total number of bytes in the packet.
 *
 * PMGR_OPEN: (not used)
 *   From: <int opcode == 0, int rank>
 *   To:   NULL
 * PMGR_CLOSE:
 *   From: <int opcode == 1>
 *   To:   NULL
 * PMGR_ABORT: (not used)
 *   From: <int opcode == 2, int errcode>
 *   To:   NULL
 * PMGR_BARRIER:
 *   From: <int opcode == 3>
 *   To:   <int opcode == 3>
 * PMGR_BCAST:
 *   From (root):     <int opcode == 4, int root, int msg_size, msg_size msg>
 *   From (non-root): <int opcode == 4, int root, int msg_size>
 *   To:   <msg_size msg>
 * PMGR_GATHER:
 *   From: <int opcode == 5, int root, int msg_size, msg_size msg>
 *   To (root):     <msg_size*N msg>
 *   To (non-root): NULL
 * PMGR_SCATTER:
 *   From (root):     <int opcode == 6, int root, int msg_size, msg_size*N msg>
 *   From (non-root): <int opcode == 6, int root, int msg_size>
 *   To:   <msg_size msg>
 * PMGR_ALLGATHER:
 *   From: <int opcode == 7, int msg_size, msg_size msg>
 *   To:   <msg_size*N msg>
 * PMGR_ALLTOALL:
 *   From: <int opcode == 8, int msg_size, msg_size*N msg>
 *   To:   <msg_size*N msg>
*/
int pmgr_processops(int* fds, int nprocs)
{
  pmgr_me = -2;
  pmgr_echo_debug = 0;
  char* value;
  fd_by_rank = fds;
  N = nprocs;

  struct timeval time_start, time_end, time_startop, time_endop;
  pmgr_gettimeofday(&time_start);

  if ((value = pmgr_getenv("MPIRUN_DEBUG", ENV_OPTIONAL)) != NULL) {
    pmgr_echo_debug = atoi(value);
  }

  pmgr_debug(1, "Processing PMGR opcodes");

  /* Until a 'CLOSE' or 'ABORT' message is seen, we continuously loop processing ops */
  int exit = 0;
  while (!exit) {
	int opcode = -1;
	int root   = -1;
	int size   = -1;
	void* buf = NULL;
        pmgr_gettimeofday(&time_startop);

	/* for each process, read in one packet (opcode and its associated data) */
	int i;
	for (i = 0; i < N; i++) {
		/* read in opcode */
		opcode = set_current(opcode, pmgr_recv_int(i));

		/* read in additional data depending on current opcode */
		int rank, code;
		switch(opcode) {
			case PMGR_OPEN: /* followed by rank */
				if (i==0) { pmgr_debug(1, "Receiving data for PMGR_OPEN"); }
				rank = pmgr_recv_int(i);
				break;
			case PMGR_CLOSE: /* no data, close the socket */
				if (i==0) { pmgr_debug(1, "Receiving data for PMGR_CLOSE"); }
				close(fd_by_rank[i]);
				break;
			case PMGR_ABORT: /* followed by exit code */
				if (i==0) { pmgr_debug(1, "Receiving data for PMGR_ABORT"); }
				code = pmgr_recv_int(i);
				pmgr_error("received abort code %d from rank %d @ file %s:%d", code, i, __FILE__, __LINE__);
				break;
			case PMGR_BARRIER: /* no data */
				if (i==0) { pmgr_debug(1, "Receiving data for PMGR_BARRIER"); }
				break;
			case PMGR_BCAST: /* root, size of message, then message data (from root only) */
				if (i==0) { pmgr_debug(1, "Receiving data for PMGR_BCAST"); }
				root = set_current(root, pmgr_recv_int(i));
				size = set_current(size, pmgr_recv_int(i));
				if (!buf) { buf = (void*) pmgr_malloc(size, "Bcast buffer"); }
				if (i == root) { pmgr_recv(buf, size, i); }
				break;
			case PMGR_GATHER: /* root, size of message, then message data */
				if (i==0) { pmgr_debug(1, "Receiving data for PMGR_GATHER"); }
				root = set_current(root, pmgr_recv_int(i));
				size = set_current(size, pmgr_recv_int(i));
				if (!buf) { buf = (void*) pmgr_malloc(size * N, "Gather buffer"); }
				pmgr_recv(buf + size*i, size, i);
				break;
			case PMGR_SCATTER: /* root, size of message, then message data */
				if (i==0) { pmgr_debug(1, "Receiving data for PMGR_SCATTER"); }
				root = set_current(root, pmgr_recv_int(i));
				size = set_current(size, pmgr_recv_int(i));
				if (!buf) { buf = (void*) pmgr_malloc(size * N, "Scatter buffer"); }
				if (i == root) { pmgr_recv(buf, size * N, i); }
				break;
			case PMGR_ALLGATHER: /* size of message, then message data */
				if (i==0) { pmgr_debug(1, "Receiving data for PMGR_ALLGATHER"); }
				size = set_current(size, pmgr_recv_int(i));
				if (!buf) { buf = (void*) pmgr_malloc(size * N, "Allgather buffer"); }
				pmgr_recv(buf + size*i, size, i);
				break;
			case PMGR_ALLTOALL: /* size of message, then message data */
				if (i==0) { pmgr_debug(1, "Receiving data for PMGR_ALLTOALL"); }
				size = set_current(size, pmgr_recv_int(i));
				if (!buf) { buf = (void*) pmgr_malloc(size * N * N, "Alltoall buffer"); }
				pmgr_recv(buf + (size*N)*i, size * N, i);
				break;
			default:
				pmgr_error("unrecognized PMGR_COLLECTIVE opcode: %d @ file %s:%d", opcode, __FILE__, __LINE__);
		}
	} /* end for each process, read in one packet (opcode and its associated data) */

	/* Complete operation */
	switch(opcode) {
		case PMGR_OPEN:
			pmgr_debug(1, "Sending data for PMGR_OPEN");
			pmgr_debug(1, "Completed PMGR_OPEN");
			break;
		case PMGR_CLOSE:
			pmgr_debug(1, "Sending data for PMGR_CLOSE");
			pmgr_debug(1, "Completed PMGR_CLOSE");
			exit = 1;
			break;
		case PMGR_ABORT:
			pmgr_debug(1, "Sending data for PMGR_ABORT");
			pmgr_debug(1, "Completed PMGR_ABORT");
			exit = 1;
			break;
		case PMGR_BARRIER: /* (just echo the opcode back) */
			pmgr_debug(1, "Sending data for PMGR_BARRIER");
			pmgr_allgatherbcast(&opcode, sizeof(opcode));
			pmgr_debug(1, "Completed PMGR_BARRIER");
			break;
		case PMGR_BCAST:
			pmgr_debug(1, "Sending data for PMGR_BCAST");
			pmgr_allgatherbcast(buf, size);
			pmgr_debug(1, "Completed PMGR_BCAST");
			break;
		case PMGR_GATHER:
			pmgr_debug(1, "Sending data for PMGR_GATHER");
			pmgr_send(buf, size * N, root);
			pmgr_debug(1, "Completed PMGR_GATHER");
			break;
		case PMGR_SCATTER:
			pmgr_debug(1, "Sending data for PMGR_SCATTER");
			pmgr_scatterbcast(buf, size);
			pmgr_debug(1, "Completed PMGR_SCATTER");
			break;
		case PMGR_ALLGATHER:
			pmgr_debug(1, "Sending data for PMGR_ALLGATHER");
			pmgr_allgatherbcast(buf, size * N);
			pmgr_debug(1, "Completed PMGR_ALLGATHER");
			break;
		case PMGR_ALLTOALL:
			pmgr_debug(1, "Sending data for PMGR_ALLTOALL");
			pmgr_alltoallbcast(buf, size);
			pmgr_debug(1, "Completed PMGR_ALLTOALL");
			break;
		default:
			pmgr_error("unrecognized PMGR_COLLECTIVE opcode: %d @ file %s:%d", opcode, __FILE__, __LINE__);
	} /* end switch(opcode) for Completing operations */

	pmgr_free(buf);
        pmgr_gettimeofday(&time_endop);
        pmgr_debug(1, "Operation took %f seconds for %d procs", pmgr_getsecs(&time_endop, &time_startop), nprocs);
  } /* while(!exit) must be more opcodes to process */

  pmgr_gettimeofday(&time_end);
  pmgr_debug(1, "Completed processing PMGR opcodes; took %f seconds for %d procs", pmgr_getsecs(&time_end, &time_start), nprocs);

  return PMGR_SUCCESS;
}
