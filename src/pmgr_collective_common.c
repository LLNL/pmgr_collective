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
 * This file provides common implementations for
 *   pmgr_collective_mpirun - the interface used by mpirun
 *   pmgr_collective_client - the interface used by the MPI tasks
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
#include <poll.h>
#include <fcntl.h>
#include <sys/time.h>
#include <arpa/inet.h>

#include "pmgr_collective_common.h"
#include "pmgr_collective_ranges.h"

/* parameters for connection attempts */
extern int mpirun_connect_tries;
extern int mpirun_connect_timeout;
extern int mpirun_connect_backoff;
extern int mpirun_connect_random;
extern unsigned pmgr_backoff_rand_seed;

/*
   my rank
   -3     ==> unitialized task (may be mpirun or MPI task)
   -2     ==> mpirun
   -1     ==> MPI task before rank is assigned
   0..N-1 ==> MPI task
*/
int pmgr_me = -3;

int pmgr_echo_debug = 0;

static unsigned int pmgr_serviceid = 2238503211;

/* Return the number of secs as a double between two timeval structs (tv2-tv1) */
double pmgr_getsecs(struct timeval* tv2, struct timeval* tv1)
{
    struct timeval result;
    timersub(tv2, tv1, &result);
    double secs = (double) result.tv_sec + (double) result.tv_usec / 1000000.0;
    return secs;
}

/* Fills in timeval via gettimeofday */
void pmgr_gettimeofday(struct timeval* tv)
{
    if (gettimeofday(tv, NULL) < 0) {
        pmgr_error("Getting time (gettimeofday() %m errno=%d) @ %s:%d",
            errno, __FILE__, __LINE__
        );
    }
}

/* Reads environment variable, bails if not set */
char* pmgr_getenv(char* envvar, int type)
{
    char* str = getenv(envvar);
    if (str == NULL && type == ENV_REQUIRED) {
        pmgr_error("Missing required environment variable: %s @ %s:%d",
            envvar, __FILE__, __LINE__
        );
        exit(1);
    }
    return str;
}

/* malloc n bytes, and bail out with error msg if fails */
void* pmgr_malloc(size_t n, char* msg)
{
    void* p = malloc(n);
    if (!p) {
        pmgr_error("Call to malloc(%lu) failed: %s (%m errno %d) @ %s:%d",
            n, msg, errno, __FILE__, __LINE__
        );
        exit(1);
    }
    return p;
}

/* free memory and set pointer to NULL */
/*
void pmgr_free(void** mem)
{
    if (mem == NULL) {
        return PMGR_FAILURE;
    }
    if (*mem != NULL) {
        free(*mem);
        *mem = NULL;
    }
    return PMGR_SUCCESS;
}
*/

/* print message to stderr */
void pmgr_error(char *fmt, ...)
{
    va_list argp;
    char hostname[256];
    if (gethostname(hostname, 256) < 0) {
        strcpy(hostname, "NULLHOST");
    }
    fprintf(stderr, "PMGR_COLLECTIVE ERROR: ");
    if (pmgr_me >= 0) {
        fprintf(stderr, "rank %d on %s: ", pmgr_me, hostname);
    } else if (pmgr_me == -2) {
        fprintf(stderr, "mpirun on %s: ", hostname);
    } else if (pmgr_me == -1) {
        fprintf(stderr, "unitialized MPI task on %s: ", hostname);
    } else {
        fprintf(stderr, "unitialized task (mpirun or MPI) on %s: ", hostname);
    }
    va_start(argp, fmt);
    vfprintf(stderr, fmt, argp);
    va_end(argp);
    fprintf(stderr, "\n");
}

/* print message to stderr */
void pmgr_debug(int level, char *fmt, ...)
{
    va_list argp;
    char hostname[256];
    if (gethostname(hostname, 256) < 0) {
        strcpy(hostname, "NULLHOST");
    }
    if (pmgr_echo_debug > 0 && pmgr_echo_debug >= level) {
        fprintf(stderr, "PMGR_COLLECTIVE DEBUG: ");
        if (pmgr_me >= 0) {
            fprintf(stderr, "rank %d on %s: ", pmgr_me, hostname);
        } else if (pmgr_me == -2) {
            fprintf(stderr, "mpirun on %s: ", hostname);
        } else if (pmgr_me == -1) {
            fprintf(stderr, "unitialized MPI task on %s: ", hostname);
        } else {
            fprintf(stderr, "unitialized task (mpirun or MPI) on %s: ", hostname);
        }
        va_start(argp, fmt);
        vfprintf(stderr, fmt, argp);
        va_end(argp);
        fprintf(stderr, "\n");
    }
}

/* write size bytes from buf into fd, retry if necessary */
int pmgr_write_fd_suppress(int fd, const void* buf, int size, int suppress)
{
    int rc;
    int n = 0;
    const char* offset = (const char*) buf;

    while (n < size) {
	rc = write(fd, offset, size - n);

	if (rc < 0) {
	    if(errno == EINTR || errno == EAGAIN) { continue; }
            pmgr_debug(suppress, "Writing to file descriptor (write(fd=%d,offset=%x,size=%d) %m errno=%d) @ file %s:%d",
                fd, offset, size-n, errno, __FILE__, __LINE__
            );
	    return rc;
	} else if(rc == 0) {
            pmgr_debug(suppress, "Unexpected return code of 0 from write to file descriptor (write(fd=%d,offset=%x,size=%d)) @ file %s:%d",
                fd, offset, size-n, __FILE__, __LINE__
            );
	    return -1;
	}

	offset += rc;
	n += rc;
    }

    return n;
}

/* write size bytes from buf into fd, retry if necessary */
int pmgr_write_fd(int fd, const void* buf, int size)
{
    return pmgr_write_fd_suppress(fd, buf, size, 0);
}

/* read size bytes into buf from fd, retry if necessary */
int pmgr_read_fd_timeout(int fd, void* buf, int size, int usecs)
{
    int rc;
    int n = 0;
    char* offset = (char*) buf;

    struct pollfd fds;
    fds.fd      = fd;
    fds.events  = POLLIN;
    fds.revents = 0x0;

    while (n < size) {
        /* poll the connection with a timeout value */
        int poll_rc = poll(&fds, 1, usecs);
        if (poll_rc < 0) {
            pmgr_error("Polling file descriptor for read (read(fd=%d,offset=%x,size=%d) %m errno=%d) @ file %s:%d",
                       fd, offset, size-n, errno, __FILE__, __LINE__
            );
            return -1;
        } else if (poll_rc == 0) {
            return -1;
        }

        /* check the revents field for errors */
        if (fds.revents & POLLHUP) {
            pmgr_debug(1, "Hang up error on poll for read(fd=%d,offset=%x,size=%d) @ file %s:%d",
                       fd, offset, size-n, __FILE__, __LINE__
            );
            return -1;
        }

        if (fds.revents & POLLERR) {
            pmgr_debug(1, "Error on poll for read(fd=%d,offset=%x,size=%d) @ file %s:%d",
                       fd, offset, size-n, __FILE__, __LINE__
            );
            return -1;
        }

        if (fds.revents & POLLNVAL) {
            pmgr_error("Invalid request on poll for read(fd=%d,offset=%x,size=%d) @ file %s:%d",
                       fd, offset, size-n, __FILE__, __LINE__
            );
            return -1;
        }

        if (!(fds.revents & POLLIN)) {
            pmgr_error("No errors found, but POLLIN is not set for read(fd=%d,offset=%x,size=%d) @ file %s:%d",
                       fd, offset, size-n, __FILE__, __LINE__
            );
            return -1;
        }

        /* poll returned that fd is ready for reading */
	rc = read(fd, offset, size - n);

	if (rc < 0) {
	    if(errno == EINTR || errno == EAGAIN) { continue; }
            pmgr_error("Reading from file descriptor (read(fd=%d,offset=%x,size=%d) %m errno=%d) @ file %s:%d",
                       fd, offset, size-n, errno, __FILE__, __LINE__
            );
	    return rc;
	} else if(rc == 0) {
            pmgr_error("Unexpected return code of 0 from read from file descriptor (read(fd=%d,offset=%x,size=%d) revents=%x) @ file %s:%d",
                       fd, offset, size-n, fds.revents, __FILE__, __LINE__
            );
	    return -1;
	}

	offset += rc;
	n += rc;
    }

    return n;
}

/* read size bytes into buf from fd, retry if necessary */
int pmgr_read_fd(int fd, void* buf, int size)
{
    /* use in infinite timeout */
    int rc = pmgr_read_fd_timeout(fd, buf, size, -1);
    return rc;
}

/* Open a connection on socket FD to peer at ADDR (which LEN bytes long).
 * This function uses a non-blocking filedescriptor for the connect(),
 * and then does a bounded poll() for the connection to complete.  This
 * allows us to timeout the connect() earlier than TCP might do it on
 * its own.  We have seen timeouts that failed after several minutes,
 * where we would really prefer to time out earlier and retry the connect.
 *
 * Return 0 on success, -1 for errors.
 */
int pmgr_connect_timeout_suppress(int fd, struct sockaddr_in* addr, int millisec, int suppress)
{
    int flags = fcntl(fd, F_GETFL);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    int err = 0;
    int rc = connect(fd, (struct sockaddr *) addr, sizeof(struct sockaddr_in));
    if (rc < 0 && errno != EINPROGRESS) {
        pmgr_debug(suppress, "Nonblocking connect failed immediately connecting to %s:%d (connect() %m errno=%d) @ file %s:%d",
            inet_ntoa(addr->sin_addr), htons(addr->sin_port), errno, __FILE__, __LINE__
        );
        return -1;
    }
    if (rc == 0) {
        goto done;  /* connect completed immediately */
    }

    struct pollfd ufds;
    ufds.fd = fd;
    ufds.events = POLLIN | POLLOUT;
    ufds.revents = 0;

again:	rc = poll(&ufds, 1, millisec);
    if (rc == -1) {
        /* poll failed */
        if (errno == EINTR) {
            /* NOTE: connect() is non-interruptible in Linux */
            goto again;
        } else {
            pmgr_debug(suppress, "Failed to poll connection connecting to %s:%d (poll() %m errno=%d) @ file %s:%d",
                inet_ntoa(addr->sin_addr), htons(addr->sin_port), errno, __FILE__, __LINE__
            );
        }
        return -1;
    } else if (rc == 0) {
        /* poll timed out before any socket events */
        /* perror("pmgr_connect_w_timeout poll timeout"); */
        return -1;
    } else {
        /* poll saw some event on the socket
         * We need to check if the connection succeeded by
         * using getsockopt.  The revent is not necessarily
         * POLLERR when the connection fails! */
        socklen_t err_len = (socklen_t) sizeof(err);
        if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &err_len) < 0) {
            pmgr_debug(suppress, "Failed to read event on socket connecting to %s:%d (getsockopt() %m errno=%d) @ file %s:%d",
                inet_ntoa(addr->sin_addr), htons(addr->sin_port), errno, __FILE__, __LINE__
            );
            return -1; /* solaris pending error */
        }
    }

done:
    fcntl(fd, F_SETFL, flags);

    /* NOTE: Connection refused is typically reported for
     * non-responsive nodes plus attempts to communicate
     * with terminated launcher. */
    if (err) {
        pmgr_debug(suppress, "Error on socket in pmgr_connect_w_timeout() connecting to %s:%d (getsockopt() set err=%d) @ file %s:%d",
            inet_ntoa(addr->sin_addr), htons(addr->sin_port), err, __FILE__, __LINE__
        );
        return -1;
    }
 
    return 0;
}

/* Connect to given IP:port.  Upon successful connection, pmgr_connect
 * shall return the connected socket file descriptor.  Otherwise, -1 shall be
 * returned.
 */
int pmgr_connect(struct in_addr ip, int port)
{
    struct sockaddr_in sockaddr;
    int sockfd;
    int i;

    /* set up address to connect to */
    sockaddr.sin_family = AF_INET;
    sockaddr.sin_addr = ip;
    sockaddr.sin_port = port;

    /* Try making the connection several times, with a random backoff
       between tries. */
    for (i = 0; ; i++) {
        /* create a socket */
        sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (sockfd < 0) {
            pmgr_error("Creating socket (socket() %m errno=%d) @ file %s:%d",
                errno, __FILE__, __LINE__
            );
            return -1;
        }

        /* connect socket to address */
        if (pmgr_connect_timeout_suppress(sockfd, &sockaddr, mpirun_connect_timeout * 1000, 1) < 0) {
            if (i >= mpirun_connect_tries) {
                pmgr_error("Failed to connect to %s:%d @ file %s:%d",
                    inet_ntoa(ip), htons(port), __FILE__, __LINE__
                );
                close(sockfd);
                return -1;
            } else {
                close(sockfd);
                if (mpirun_connect_random) {
                    usleep(((rand_r(&pmgr_backoff_rand_seed) % (mpirun_connect_backoff * 1000)) + 1) * 1000);
                } else {
                    usleep(mpirun_connect_backoff * 1000 * 1000);
                }
            }
        } else {
            break;
        }
    }

    return sockfd;
}

/* open a listening socket and return the descriptor, the ip address, and the port */
int pmgr_open_listening_socket(int* out_fd, struct in_addr* out_ip, short* out_port)
{
    /* create a socket to accept connection from parent */
    int sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sockfd < 0) {
        pmgr_error("Creating parent socket (socket() %m errno=%d) @ file %s:%d",
            errno, __FILE__, __LINE__
        );
        return PMGR_FAILURE;
    }

    /* prepare socket to be bound to ephemeral port - OS will assign us a free port */
    struct sockaddr_in sin;
    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = htonl(INADDR_ANY);
    sin.sin_port = htons(0); /* bind ephemeral port - OS will assign us a free port */

    /* bind socket */
    if (bind(sockfd, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
        pmgr_error("Binding parent socket (bind() %m errno=%d) @ file %s:%d",
            errno, __FILE__, __LINE__
        );
        return PMGR_FAILURE;
    }

    /* set the socket to listen for connections */
    if (listen(sockfd, 1) < 0) {
        pmgr_error("Setting parent socket to listen (listen() %m errno=%d) @ file %s:%d",
            errno, __FILE__, __LINE__
        );
        return PMGR_FAILURE;
    }

    /* ask which port the OS assigned our socket to */
    socklen_t len = sizeof(sin);
    if (getsockname(sockfd, (struct sockaddr *) &sin, &len) < 0) {
        pmgr_error("Reading parent socket port number (getsockname() %m errno=%d) @ file %s:%d",
            errno, __FILE__, __LINE__
        );
        return PMGR_FAILURE;
    }

    /* extract our ip and port number */
    char hn[256];
    if (gethostname(hn, sizeof(hn)) < 0) {
        pmgr_error("Error calling gethostname() @ file %s:%d",
            __FILE__, __LINE__
        );
        return PMGR_FAILURE;
    }
    struct hostent* he = gethostbyname(hn);
    struct in_addr ip = * (struct in_addr *) *(he->h_addr_list);
    short port = sin.sin_port;

    /* set output parameters */
    *out_fd   = sockfd;
    *out_ip   = ip;
    *out_port = port;

    return PMGR_SUCCESS;
}

int pmgr_authenticate_accept(int fd, const char* connect_text, size_t connect_len, const char* accept_text, size_t accept_len, int reply_timeout)
{
    int test_failed = 0;

    /* read the service id */
    unsigned int received_serviceid = 0;
    if (!test_failed && pmgr_read_fd_timeout(fd, &received_serviceid, sizeof(received_serviceid), reply_timeout) < 0) {
        pmgr_debug(1, "Receiving service id from new connection failed @ file %s:%d",
            __FILE__, __LINE__
        );
        test_failed = 1;
    }

    /* check that we got the expected service id */
    /* TODO: reply with some sort of error message if no match? */
    if (!test_failed && received_serviceid != pmgr_serviceid) {
        test_failed = 1;
    }

    /* read the connect text */
    char* received_connect_text = NULL;
    if (!test_failed && connect_text > 0) {
        received_connect_text = (char*) malloc(connect_len);
        if (received_connect_text == NULL) {
            pmgr_debug(1, "Failed to allocate memory to receive connect text from new connection @ file %s:%d",
                __FILE__, __LINE__
            );
            test_failed = 1;
        }

        if (!test_failed && pmgr_read_fd_timeout(fd, received_connect_text, connect_len, reply_timeout) < 0) {
            pmgr_debug(1, "Receiving connect text from new connection failed @ file %s:%d",
                __FILE__, __LINE__
            );
            test_failed = 1;
        }
    }

    /* check that we got the expected connect text */
    if (!test_failed && received_connect_text != NULL) {
        size_t i;
        for (i = 0; i < connect_len; i++) {
            if (received_connect_text[i] != connect_text[i]) {
                test_failed = 1;
                break;
            }
        }
    }

    /* write a nack back immediately so connecting proc can tear down faster */
    if (test_failed) {
        unsigned int nack = 0;
        pmgr_write_fd(fd, &nack, sizeof(nack));
        fsync(fd);
    }

    /* write our service id back as a reply */
    if (!test_failed && pmgr_write_fd_suppress(fd, &pmgr_serviceid, sizeof(pmgr_serviceid), 1) < 0) {
        pmgr_debug(1, "Writing service id to new connection failed @ file %s:%d",
                   __FILE__, __LINE__
        );
        test_failed = 1;
    }

    /* write our accept text back as a reply */
    if (!test_failed && pmgr_write_fd_suppress(fd, accept_text, accept_len, 1) < 0) {
        pmgr_debug(1, "Writing accept id to new connection failed @ file %s:%d",
                   __FILE__, __LINE__
        );
        test_failed = 1;
    }

    /* force our writes to be sent */
    if (!test_failed) {
        fsync(fd);
    }

    /* the other end may have dropped us if it was too impatient waiting for our reply,
     * read its ack to know that it completed the connection */
    unsigned int ack = 0;
    if (!test_failed && pmgr_read_fd_timeout(fd, &ack, sizeof(ack), reply_timeout) < 0) {
        pmgr_debug(1, "Receiving ack to finalize connection @ file %s:%d",
                   __FILE__, __LINE__
        );
        test_failed = 1;
    }

    /* if we allocated any memory, free it off */
    if (received_connect_text != NULL) {
        free(received_connect_text);
        received_connect_text = NULL;
    }

    /* return our verdict */
    if (test_failed) {
        return PMGR_FAILURE;
    }
    return PMGR_SUCCESS;
}

/* issues a handshake across connection to verify we really connected to the right socket */
int pmgr_authenticate_connect(int fd, int rank, const char* hostname, int port, const char* connect_text, size_t connect_len, const char* accept_text, size_t accept_len, int reply_timeout)
{
    int test_failed = 0;

    /* write pmgr service id */
    if (!test_failed && pmgr_write_fd_suppress(fd, &pmgr_serviceid, sizeof(pmgr_serviceid), 1) < 0) {
        pmgr_debug(1, "Writing service id to %s on port %d @ file %s:%d",
            hostname, port, __FILE__, __LINE__
        );
        test_failed = 1;
    }

    /* write our connect text */
    if (!test_failed && pmgr_write_fd_suppress(fd, connect_text, connect_len, 1) < 0) {
       pmgr_debug(1, "Writing connect text to %s on port %d @ file %s:%d",
           hostname, port, __FILE__, __LINE__
       );
       test_failed = 1;
    }

    /* force our writes to be sent */
    if (!test_failed) {
        fsync(fd);
    }

    /* read the pmgr service id */
    unsigned int received_serviceid = 0;
    if (!test_failed && pmgr_read_fd_timeout(fd, &received_serviceid, sizeof(received_serviceid), reply_timeout) < 0) {
        pmgr_debug(1, "Receiving service id from %s on port %d failed @ file %s:%d",
            hostname, port, __FILE__, __LINE__
        );
        test_failed = 1;
    }

    /* check that we got the expected service id */
    if (!test_failed && received_serviceid != pmgr_serviceid) {
        test_failed = 1;
    }

    /* read the accept text */
    char* received_accept_text = NULL;
    if (!test_failed && accept_len > 0) {
        received_accept_text = (char*) malloc(accept_len);
        if (received_accept_text == NULL) {
            pmgr_debug(1, "Failed to allocate memory to receive accept text from %s on port %d failed @ file %s:%d",
                hostname, port, __FILE__, __LINE__
            );
            test_failed = 1;
        }

        if (!test_failed && pmgr_read_fd_timeout(fd, received_accept_text, accept_len, reply_timeout) < 0) {
            pmgr_debug(1, "Receiving accept text from %s on port %d failed @ file %s:%d",
                hostname, port, __FILE__, __LINE__
            );
            test_failed = 1;
        }
    }

    /* check that we got the expected accept text */
    if (!test_failed && received_accept_text != NULL) {
        size_t i = 0;
        for (i = 0 ; i < accept_len; i++) {
            if (received_accept_text[i] != accept_text[i]) {
                test_failed = 1;
                break;
            }
        }
    }

    /* write ack to finalize connection (no need to suppress write errors any longer) */
    unsigned int ack = 1;
    if (!test_failed && pmgr_write_fd(fd, &ack, sizeof(ack)) < 0) {
        pmgr_debug(1, "Writing ack to finalize connection to rank %d on %s port %d @ file %s:%d",
            rank, hostname, port, __FILE__, __LINE__
        );
        test_failed = 1;
    }

    /* force our writes to be sent */
    if (!test_failed) {
        fsync(fd);
    }

    /* if we allocated memory, free it off */
    if (received_accept_text != NULL) {
        free(received_accept_text);
        received_accept_text = NULL;
    }

    /* return our verdict */
    if (test_failed) {
        return PMGR_FAILURE;
    }
    return PMGR_SUCCESS;
}

/* Attempts to connect to a given hostname using a port list and timeouts */
int pmgr_connect_hostname(int rank, const char* hostname, const char* ports, const char* connect_text, size_t connect_len, const char* accept_text, size_t accept_len)
{
    int s = -1;

    /* lookup host address by name */
    struct hostent* he = gethostbyname(hostname);
    if (!he) {
        pmgr_error("Hostname lookup failed (gethostbyname(%s) %s h_errno=%d) @ file %s:%d",
                   hostname, hstrerror(h_errno), h_errno, __FILE__, __LINE__
        );
        return s;
    }

    /* get number of ports */
    int ports_count = 0;
    pmgr_range_numbers_size(ports, &ports_count);

    char* target_port = strdup(ports);
    size_t target_port_len = strlen(target_port) + 1;

    /* Loop until we make a connection or until our timeout expires. */
    struct timeval start, end;
    pmgr_gettimeofday(&start);
    double secs = 0;
    int connected = 0;
    int reply_timeout = mpirun_connect_timeout * 10;
    double mpirun_connect_timelimit = (double) 1000 * reply_timeout;
    while (!connected && secs < mpirun_connect_timelimit) {
        /* iterate over our ports trying to find a connection */
        int i;
        for (i = 0; i < ports_count; i++) {
            /* get the next port */
            int port;
            if (pmgr_range_numbers_nth(ports, i, target_port, target_port_len) == PMGR_SUCCESS) {
                port = atoi(target_port);
            } else {
                continue;
            }

            /* attempt to connect to hostname on this port */
            pmgr_debug(1, "Trying rank %d on port %d on %s", rank, port, hostname);
            s = pmgr_connect(*(struct in_addr *) (*he->h_addr_list), htons(port));
            if (s != -1) {
                /* got a connection, let's test it out */
                pmgr_debug(1, "Connected to rank %d port %d on %s", rank, port, hostname);

                if (pmgr_authenticate_connect(s, rank, hostname, port, connect_text, connect_len, accept_text, accept_len, reply_timeout) == PMGR_SUCCESS) {
                    /* it checks out, we're connected to the right process */
                    connected = 1;
                    break;
                } else {
                    /* don't know who we connected to, close the socket */
                    close(s);
                }
            }
        }

        /* sleep for some time before we try another port scan */
        if (!connected) {
            usleep(mpirun_connect_backoff * 1000);

            /* maybe we connected ok, but we were too impatient waiting for a reply,
             * extend the reply timeout for the next attempt */
            reply_timeout *= 2;
        }

        /* compute how many seconds we've spent trying to connect */
        pmgr_gettimeofday(&end);
        secs = pmgr_getsecs(&end, &start);
        if (secs >= mpirun_connect_timelimit) {
            pmgr_error("Time limit to connect to rank %d on %s expired @ file %s:%d",
                       rank, hostname, __FILE__, __LINE__
            );
        }
    }

    /* check that we successfully opened a socket */
    if (s == -1) {
        pmgr_error("Connecting socket to %s at %s failed @ file %s:%d",
                   he->h_name, inet_ntoa(*(struct in_addr *) (*he->h_addr_list)),
                   __FILE__, __LINE__
        );
        return s;
    }

    return s;
}
