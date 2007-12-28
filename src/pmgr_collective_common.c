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
 * This file provides common implementations for
 *   pmgr_collective_mpirun - the interface used by mpirun
 *   pmgr_collective_client - the interface used by the MPI tasks
 *
 * Copyright (C) 2007 The Regents of the University of California.
 * Produced at Lawrence Livermore National Laboratory.
 * Author: Adam Moody <moody20@llnl.gov>
*/

#include <stdarg.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include "pmgr_collective_common.h"

/* print message to stderr */
void pmgr_error(char *fmt, ...)
{
        va_list argp;
        fprintf(stderr, "PMGR_COLLECTIVE ERROR: ");
        va_start(argp, fmt);
        vfprintf(stderr, fmt, argp);
        va_end(argp);
        fprintf(stderr, "\n");
}

/* write size bytes from buf into fd, retry if necessary */
int pmgr_write_fd(int fd, void* buf, int size)
{
    int rc;
    int n = 0;
    char* offset = (char*) buf;

    while (n < size) {
	rc = write(fd, offset, size - n);

	if(rc < 0) {
	    if(errno == EINTR || errno == EAGAIN) continue;
	    return rc;
	}

	else if(rc == 0) {
	    return n;
	}

	offset += rc;
	n += rc;
    }

    return n;
}

/* read size bytes into buf from fd, retry if necessary */
int pmgr_read_fd(int fd, void* buf, int size)
{
    int rc;
    int n = 0;
    char* offset = (char*) buf;

    while (n < size) {
	rc = read(fd, offset, size - n);

	if(rc < 0) {
	    if(errno == EINTR || errno == EAGAIN) continue;
	    return rc;
	}

	else if(rc == 0) {
	    return n;
	}

	offset += rc;
	n += rc;
    }

    return n;
}
