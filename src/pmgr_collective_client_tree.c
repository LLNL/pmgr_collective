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

#include "pmgr_collective_ranges.h"
#include "pmgr_collective_client.h"
#include "pmgr_collective_client_mpirun.h"
#include "pmgr_collective_client_tree.h"
#include "pmgr_collective_client_slurm.h"

#include "pmi.h"

/* packet headers for messages in tree */
#define PMGR_TREE_HEADER_ABORT      (0)
#define PMGR_TREE_HEADER_COLLECTIVE (1)

extern int mpirun_use_pmi;
extern int mpirun_use_shm;

/* write an abort packet across socket */
static int pmgr_write_abort(int fd)
{
    /* just need to write the integer code for an abort message */
    int header = PMGR_TREE_HEADER_ABORT;
    int rc = pmgr_write_fd(fd, &header, sizeof(int));
    if (rc < sizeof(int)) {
        /* the write failed, close this socket, and return the error */
        return rc;
    }
    return rc;
}

/* write a collective packet across socket */
static int pmgr_write_collective(int fd, void* buf, int size)
{
    int rc = 0;

    /* first write the integer code for a collective message */
    int header = PMGR_TREE_HEADER_COLLECTIVE;
    rc = pmgr_write_fd(fd, &header, sizeof(int));
    if (rc < sizeof(int)) {
        /* the write failed, close the socket, and return an error */
        pmgr_error("Failed to write collective packet header @ file %s:%d",
            __FILE__, __LINE__
        );
        return rc;
    }

    /* now write the data for this message */
    rc = pmgr_write_fd(fd, buf, size);
    if (rc < size) {
        /* the write failed, close the socket, and return an error */
        pmgr_error("Failed to write collective packet data @ file %s:%d",
            __FILE__, __LINE__
        );
        return rc;
    }

    return rc;
}

/* receive a collective packet from socket */
static int pmgr_read_collective(int fd, void* buf, int size)
{
    int rc = 0;

    /* read the packet header */
    int header = 1;
    rc = pmgr_read_fd(fd, &header, sizeof(int));
    if (rc <= 0) {
        /* failed to read packet header, print error, close socket, and return error */
        pmgr_error("Failed to read packet header @ file %s:%d",
            __FILE__, __LINE__
        );
        return rc;
    }

    /* process the packet */
    if (header == PMGR_TREE_HEADER_COLLECTIVE) {
        /* got our collective packet, now read its data */
        rc = pmgr_read_fd(fd, buf, size);
        if (rc <= 0) {
            /* failed to read data from socket, print error, close socket, and return error */
            pmgr_error("Failed to read collective packet data @ file %s:%d",
                __FILE__, __LINE__
            );
            return rc;
        }
    } else if (header == PMGR_TREE_HEADER_ABORT) {
        /* received an abort packet, close the socket this packet arrived on,
         * broadcast an abort packet and exit with success */
        pmgr_abort_trees();
        exit(0);
    } else {
        /* unknown packet type, return an error */
        pmgr_error("Received unknown packet header %d @ file %s:%d",
            header, __FILE__, __LINE__
        );
        rc = -1;
    }

    return rc;
}

/* issue a connect to a child, and verify that we really connected to who we should */
static int pmgr_connect_child(struct in_addr ip, int port)
{
    int fd = -1;
    while (fd == -1) {
        fd = pmgr_connect(ip, port);
        if (fd >= 0) {
            /* connected to something, check that it's who we expected to connect to */
            if (pmgr_authenticate_connect(fd, NULL, 0, NULL, 0, 100) != PMGR_SUCCESS) {
                close(fd);
                fd = -1;
            }
        } else {
            /* error from connect */
        }
    }
    return fd;
}

static int pmgr_accept_parent(int sockfd, struct sockaddr* addr, socklen_t* len)
{
    int fd = -1;
    while (fd == -1) {
        fd = accept(sockfd, addr, len);
        if (fd >= 0) {
            /* connected to something, check that it's who we expected to connect to */
            if (pmgr_authenticate_accept(fd, NULL, 0, NULL, 0, 100) != PMGR_SUCCESS) {
                close(fd);
                fd = -1;
            }
        } else {
            /* error from accept */
        }
    }
    return fd;
}

/*
 * =============================
 * Initialize and free tree data structures
 * =============================
 */

/* initialize tree to null tree */
int pmgr_tree_init_null(pmgr_tree_t* t)
{
    if (t == NULL) {
        return PMGR_FAILURE;
    }

    t->type           = PMGR_GROUP_TREE_NULL;
    t->ranks          =  0;
    t->rank           = -1;
    t->is_open        =  0;
    t->parent         = -1;
    t->depth          = -1;
    t->parent_fd      = -1;
    t->num_child      = -1;
    t->num_child_incl = -1;
    t->child      = NULL;
    t->child_fd   = NULL;
    t->child_incl = NULL;
    t->child_ip   = NULL;
    t->child_port = NULL;

    return PMGR_SUCCESS;
}

/* given number of ranks and our rank with the group, create a binomail tree
 * fills in our position within the tree and allocates memory to hold socket info */
int pmgr_tree_init_binomial(pmgr_tree_t* t, int ranks, int rank)
{
    int i;

    if (t == NULL) {
        return PMGR_FAILURE;
    }

    pmgr_tree_init_null(t);

    /* compute the maximum number of children this task may have */
    int n = 1;
    int max_children = 0;
    while (n < ranks) {
        n <<= 1;
        max_children++;
    }

    /* prepare data structures to store our parent and children */
    t->type  = PMGR_GROUP_TREE_BINOMIAL;
    t->ranks = ranks;
    t->rank  = rank;
    t->depth          =  0;
    t->parent         = -1;
    t->num_child      =  0;
    t->num_child_incl =  0;
    if (max_children > 0) {
        t->child      = (int*)   pmgr_malloc(max_children * sizeof(int),   "Child rank array");
        t->child_fd   = (int*)   pmgr_malloc(max_children * sizeof(int),   "Child socket fd array");
        t->child_incl = (int*)   pmgr_malloc(max_children * sizeof(int),   "Child children count array");
        t->child_ip   = (struct in_addr*) pmgr_malloc(max_children * sizeof(struct in_addr), "Child IP array");
        t->child_port = (short*) pmgr_malloc(max_children * sizeof(short), "Child port array");
    }

    /* initialize parent and child socket file descriptors to -1 */
    t->parent_fd = -1;
    for(i = 0; i < max_children; i++) {
        t->child_fd[i] = -1;
    }

    /* find our parent rank and the ranks of our children */
    int depth = 1;
    int low  = 0;
    int high = ranks - 1;
    while (high - low > 0) {
        int mid = (high - low) / 2 + (high - low) % 2 + low;
        if (low == rank) {
            t->child[t->num_child] = mid;
            t->child_incl[t->num_child] = high - mid + 1;
            t->num_child++;
            t->num_child_incl += (high - mid + 1);
        }
        if (mid == rank) {
            t->depth  = depth;
            t->parent = low;
        }
        if (mid <= rank) {
            low  = mid;
        } else {
            high = mid-1;
            depth++;
        }
    }

    return PMGR_SUCCESS;
}

/* given number of ranks and our rank with the group, create a binary tree
 * fills in our position within the tree and allocates memory to hold socket info */
int pmgr_tree_init_binary(pmgr_tree_t* t, int ranks, int rank)
{
    int i;

    if (t == NULL) {
        return PMGR_FAILURE;
    }

    pmgr_tree_init_null(t);

    /* compute the maximum number of children this task may have */
    int max_children = 2;

    /* prepare data structures to store our parent and children */
    t->type  = PMGR_GROUP_TREE_BINOMIAL;
    t->ranks = ranks;
    t->rank  = rank;
    t->depth          =  0;
    t->parent         = -1;
    t->num_child      =  0;
    t->num_child_incl =  0;
    if (max_children > 0) {
        t->child      = (int*)   pmgr_malloc(max_children * sizeof(int),   "Child rank array");
        t->child_fd   = (int*)   pmgr_malloc(max_children * sizeof(int),   "Child socket fd array");
        t->child_incl = (int*)   pmgr_malloc(max_children * sizeof(int),   "Child children count array");
        t->child_ip   = (struct in_addr*) pmgr_malloc(max_children * sizeof(struct in_addr), "Child IP array");
        t->child_port = (short*) pmgr_malloc(max_children * sizeof(short), "Child port array");
    }

    /* initialize parent and child socket file descriptors to -1 */
    t->parent_fd = -1;
    for(i = 0; i < max_children; i++) {
        t->child_fd[i] = -1;
    }

    /* find our parent rank and the ranks of our children */
    int low  = 0;
    int high = ranks - 1;
    while (high - low > 0) {
        /* pick the midpoint of the remaining nodes, round up if not divisible by 2 */
        int mid = (high - low) / 2 + (high - low) % 2 + low;

        /* if we are the parent for this section, set our children */
        if (low == rank) {
            /* take the rank that is furthest away as the first child */
            t->child[t->num_child] = mid;
            t->child_incl[t->num_child] = high - mid + 1;
            t->num_child++;
            t->num_child_incl += (high - mid + 1);

            /* if there is another rank between us and the midpoint,
             * set the next highest rank as our second child */
            low++;
            if (mid > low) {
                t->child[t->num_child] = low;
                t->child_incl[t->num_child] = mid - low;
                t->num_child++;
                t->num_child_incl += (mid - low);
            }

            break;
        }

        /* increase our depth from the root by one */
        t->depth++;

        /* determine whether we're in the first or second half,
         * if our rank is the midpoint or the next highest from the current low
         * then we'll be a parent in the next step, so the current low is our parent */
        if (mid <= rank) {
            if (mid == rank) {
                t->parent = low;
            }
            low  = mid;
        } else {
            if (low+1 == rank) {
                t->parent = low;
            }
            low  = low + 1;
            high = mid - 1;
        }
    }

    return PMGR_SUCCESS;
}

/* free all memory allocated in tree */
int pmgr_tree_free(pmgr_tree_t* t)
{
    if (t == NULL) {
        return PMGR_FAILURE;
    }

    pmgr_free(t->child);
    pmgr_free(t->child_fd);
    pmgr_free(t->child_incl);
    pmgr_free(t->child_ip);
    pmgr_free(t->child_port);

    pmgr_tree_init_null(t);
  
    return PMGR_SUCCESS;
}

/* 
 * =============================
 * Functions to open/close/gather/bcast the TCP/socket tree.
 * =============================
*/

int pmgr_tree_is_open(pmgr_tree_t* t)
{
    return t->is_open;
}

/*
 * close down socket connections for tree (parent and any children),
 * free memory for tree data structures
 */
int pmgr_tree_close(pmgr_tree_t* t)
{
    /* close socket connection with parent */
    if (t->parent_fd >= 0) {
        close(t->parent_fd);
        t->parent_fd = -1;
    }

    /* close sockets to children */
    int i;
    for(i = 0; i < t->num_child; i++) {
        if (t->child_fd[i] >= 0) {
            close(t->child_fd[i]);
            t->child_fd[i] = -1;
        }
    }

    /* mark the tree as being closed */
    t->is_open = 0;

    /* TODO: really want to free the tree here? */
    /* free data structures */
    pmgr_tree_free(t);

    return PMGR_SUCCESS;
}

/*
 * send abort message across links, then close down tree
 */
int pmgr_tree_abort(pmgr_tree_t* t)
{
    /* send abort message to parent */
    if (t->parent_fd >= 0) {
        pmgr_write_abort(t->parent_fd);
    }

    /* send abort message to each child */
    int i;
    for(i = 0; i < t->num_child; i++) {
        if (t->child_fd[i] >= 0) {
            pmgr_write_abort(t->child_fd[i]);
        }
    }

    /* shut down our connections */
    pmgr_tree_close(t);

    return PMGR_SUCCESS;
}

/* check whether all tasks report success, exit if someone failed */
int pmgr_tree_check(pmgr_tree_t* t, int value)
{
    /* assume that everyone succeeded */
    int all_value = 1;

    /* read value from each child */
    int i;
    for (i = 0; i < t->num_child; i++) {
        if (t->child_fd[i] >= 0) {
            int child_value;
            if (pmgr_read_collective(t->child_fd[i], &child_value, sizeof(int)) < 0) {
                /* failed to read value from child, assume child failed */
                pmgr_error("Reading result from child (rank %d) at %s:%d failed @ file %s:%d",
                    t->child[i], inet_ntoa(t->child_ip[i]), t->child_port[i], __FILE__, __LINE__
                );
                pmgr_abort_trees();
                exit(1);
            } else {
                if (!child_value) {
                    /* child failed */
                    all_value = 0;
                }
            }
        } else {
            /* never connected to this child, assume child failed */
            all_value = 0;
        }
    }

    /* now consider my value */
    if (!value) {
        all_value = 0;
    }

    /* send result to parent */
    if (t->parent_fd >= 0) {
        /* send result to parent */
        if (pmgr_write_collective(t->parent_fd, &all_value, sizeof(int)) < 0) {
            pmgr_error("Writing check tree result to parent failed @ file %s:%d",
                __FILE__, __LINE__
            );
            pmgr_abort_trees();
            exit(1);
        }
    }

    /* read result from parent */
    if (t->parent_fd >= 0) {
        if (pmgr_read_collective(t->parent_fd, &all_value, sizeof(int)) < 0) {
            pmgr_error("Reading check tree result from parent failed @ file %s:%d",
                __FILE__, __LINE__
            );
            pmgr_abort_trees();
            exit(1);
        }
    }

    /* broadcast result to children */
    for (i = 0; i < t->num_child; i++) {
        if (t->child_fd[i] >= 0) {
            if (pmgr_write_collective(t->child_fd[i], &all_value, sizeof(int)) < 0) {
                pmgr_error("Writing result to child (rank %d) at %s:%d failed @ file %s:%d",
                    t->child[i], inet_ntoa(t->child_ip[i]), t->child_port[i], __FILE__, __LINE__
                );
                pmgr_abort_trees();
                exit(1);
            }
        }
    }

    /* if someone failed, exit */
    if (!all_value) {
        /* close down sockets and send close op code to srun */
        pmgr_close();

        /* exit with success if this process succeeded, exit with failure otherwise */
        if (value) {
            exit(0);
        }
        exit(1);
    }
    return PMGR_SUCCESS;
}

/* given a table of ranks number of ip:port entries, open a tree */
int pmgr_tree_open_table(pmgr_tree_t* t, int ranks, int rank, const void* table, int sockfd)
{
    int i;

    /* compute the size of each entry in the table ip:port */
    size_t addr_size = sizeof(struct in_addr) + sizeof(short);

    /* compute our depth, parent,and children */
//    pmgr_tree_init_binomial(t, ranks, rank);
    pmgr_tree_init_binary(t, ranks, rank);

    /* establish connections */
    if (t->depth % 2 == 0) {
        /* if i'm not rank 0, accept a connection from parent first */
        if (t->rank != 0) {
            socklen_t parent_len;
            struct sockaddr parent_addr;
            parent_len = sizeof(parent_addr);
            t->parent_fd = pmgr_accept_parent(sockfd, (struct sockaddr *) &parent_addr, &parent_len);
            if (t->parent_fd < 0) {
                pmgr_error("Failed to accept parent connection (%m errno=%d) @ file %s:%d",
                    errno, __FILE__, __LINE__
                );
                pmgr_abort_trees();
                exit(1);
            }
        }

        /* then, open connections to children */
        for (i=0; i < t->num_child; i++) {
            int c = t->child[i];
            t->child_ip[i]   = * (struct in_addr *)  ((char*)table + c*addr_size);
            t->child_port[i] = * (short*) ((char*)table + c*addr_size + sizeof(struct in_addr));
            t->child_fd[i]   = pmgr_connect_child(t->child_ip[i], t->child_port[i]);
            if (t->child_fd[i] < 0) {
                /* failed to connect to child */
                pmgr_error("Connecting to child (rank %d) at %s:%d failed @ file %s:%d",
                    t->child[i], inet_ntoa(t->child_ip[i]), t->child_port[i], __FILE__, __LINE__
                );
                pmgr_abort_trees();
                exit(1);
            }
        }
    } else {
        /* open connections to children first */
        for (i=0; i < t->num_child; i++) {
            int c = t->child[i];
            t->child_ip[i]   = * (struct in_addr *)  ((char*)table + c*addr_size);
            t->child_port[i] = * (short*) ((char*)table + c*addr_size + sizeof(struct in_addr));
            t->child_fd[i]   = pmgr_connect_child(t->child_ip[i], t->child_port[i]);
            if (t->child_fd[i] < 0) {
                /* failed to connect to child */
                pmgr_error("Connecting to child (rank %d) at %s:%d failed @ file %s:%d",
                    t->child[i], inet_ntoa(t->child_ip[i]), t->child_port[i], __FILE__, __LINE__
                );
                pmgr_abort_trees();
                exit(1);
            }
        }

        /* then, accept connection from parent */
        if (t->rank != 0) {
            socklen_t parent_len;
            struct sockaddr parent_addr;
            parent_len = sizeof(parent_addr);
            t->parent_fd = pmgr_accept_parent(sockfd, (struct sockaddr *) &parent_addr, &parent_len);
            if (t->parent_fd < 0) {
                pmgr_error("Failed to accept parent connection (%m errno=%d) @ file %s:%d",
                    errno, __FILE__, __LINE__
                );
                pmgr_abort_trees();
                exit(1);
            }
        }
    }

    /* mark the tree as being open */
    t->is_open = 1;

    /* check whether everyone succeeded in connecting */
    pmgr_tree_check(t, 1);

    return PMGR_SUCCESS;
}

static int pmgr_tree_open_nodelist_scan_connect_children(
    pmgr_tree_t* t, const char* nodelist, const char* portrange, int nodes )
{
    int i;
    for (i = 0; i < t->num_child; i++) {
        /* get the rank of the child we'll connect to */
        int child_rank = t->child[i];
        if (child_rank >= nodes) {
            /* child rank is out of range */
            pmgr_error("Child rank %d is out of range of %d nodes %s @ file %s:%d",
                child_rank, nodes, nodelist, __FILE__, __LINE__
            );
            pmgr_abort_trees();
            exit(1);
        }

        /* get hostname of child (note we need to add one to the rank) */
        char child_name[1024];
        if (pmgr_range_nodelist_nth(nodelist, child_rank+1, child_name, sizeof(child_name)) != PMGR_SUCCESS) {
            /* failed to extract child hostname from nodelist */
            pmgr_error("Failed to extract hostname for node %d from %s @ file %s:%d",
                child_rank+1, nodelist, __FILE__, __LINE__
            );
            pmgr_abort_trees();
            exit(1);
        }

        /* attempt to connect to child on this hostname using given portrange */
        struct in_addr ip;
        short port;
        int fd = pmgr_connect_hostname(
            child_rank, child_name, portrange, &ip, &port,
            NULL, 0, NULL, 0
        );
        if (fd >= 0) {
            /* connected to child, record ip, port, and socket */
            t->child_ip[i]   = ip;
            t->child_port[i] = port;
            t->child_fd[i]   = fd;
        } else {
            /* failed to connect to child */
            pmgr_error("Connecting to child (rank %d) on %s failed @ file %s:%d",
                child_rank, child_name, __FILE__, __LINE__
            );
            pmgr_abort_trees();
            exit(1);
        }
    }

    return PMGR_SUCCESS;
}

/* given a table of ranks number of ip:port entries, open a tree */
int pmgr_tree_open_nodelist_scan(pmgr_tree_t* t, const char* nodelist, const char* portrange, int sockfd)
{
    /* determine number of nodes in nodelist */
    int nodes;
    pmgr_range_nodelist_size(nodelist, &nodes);

    /* establish connections */
    if (t->depth % 2 == 0) {
        /* if i'm not rank 0, accept a connection from parent first */
        if (t->rank != 0) {
            socklen_t parent_len;
            struct sockaddr parent_addr;
            parent_len = sizeof(parent_addr);
            t->parent_fd = pmgr_accept_parent(sockfd, (struct sockaddr *) &parent_addr, &parent_len);
            if (t->parent_fd < 0) {
                pmgr_error("Failed to accept parent connection (%m errno=%d) @ file %s:%d",
                    errno, __FILE__, __LINE__
                );
                pmgr_abort_trees();
                exit(1);
            }
        }

        /* then, open connections to children */
        if (pmgr_tree_open_nodelist_scan_connect_children(t, nodelist, portrange, nodes) != PMGR_SUCCESS) {
            pmgr_error("Failed to connect to children @ file %s:%d",
                __FILE__, __LINE__
            );
            pmgr_abort_trees();
            exit(1);
        }
    } else {
        /* open connections to children first */
        if (pmgr_tree_open_nodelist_scan_connect_children(t, nodelist, portrange, nodes) != PMGR_SUCCESS) {
            pmgr_error("Failed to connect to children @ file %s:%d",
                __FILE__, __LINE__
            );
            pmgr_abort_trees();
            exit(1);
        }

        /* then, accept connection from parent */
        if (t->rank != 0) {
            socklen_t parent_len;
            struct sockaddr parent_addr;
            parent_len = sizeof(parent_addr);
            t->parent_fd = pmgr_accept_parent(sockfd, (struct sockaddr *) &parent_addr, &parent_len);
            if (t->parent_fd < 0) {
                pmgr_error("Failed to accept parent connection (%m errno=%d) @ file %s:%d",
                    errno, __FILE__, __LINE__
                );
                pmgr_abort_trees();
                exit(1);
            }
        }
    }

    /* mark the tree as being open */
    t->is_open = 1;

    /* check whether everyone succeeded in connecting */
    pmgr_tree_check(t, 1);

    return PMGR_SUCCESS;
}

#ifdef HAVE_PMI
/* build socket table using PMI */
static int pmgr_tree_table_pmi(int ranks, int rank, struct in_addr* ip, short port, void* table)
{
    int i;

    /* compute size of each table entry */
    size_t addr_size = sizeof(struct in_addr) + sizeof(short);

    /* get the number of bytes we need for our KVS name */
    int kvslen = 0;
    if (PMI_KVS_Get_name_length_max(&kvslen) != PMI_SUCCESS) {
        pmgr_error("Getting maximum length for PMI KVS name @ file %s:%d",
            __FILE__, __LINE__
        );
        PMI_Abort(1, "Failed to get maximum length for PMI KVS space name");
    }

    /* get the maximum number of bytes allowed for a KVS key */
    int keylen = 0;
    if (PMI_KVS_Get_key_length_max(&keylen) != PMI_SUCCESS) {
        pmgr_error("Getting maximum length for PMI key length @ file %s:%d",
            __FILE__, __LINE__
        );
        PMI_Abort(1, "Failed to get maximum length for PMI key length");
    }

    /* get the maximum number of bytes allowed for a KVS value */
    int vallen = 0;
    if (PMI_KVS_Get_value_length_max(&vallen) != PMI_SUCCESS) {
        pmgr_error("Getting maximum length for PMI value length @ file %s:%d",
            __FILE__, __LINE__
        );
        PMI_Abort(1, "Failed to get maximum length for PMI value length");
    }

    /* allocate space to hold kvs name, key, and value */
    char* kvsstr = (char*) pmgr_malloc(kvslen, "KVS name buffer");
    char* keystr = (char*) pmgr_malloc(keylen, "KVS key buffer");
    char* valstr = (char*) pmgr_malloc(vallen, "KVS value buffer");

    /* lookup our KVS name */
    if (PMI_KVS_Get_my_name(kvsstr, kvslen) != PMI_SUCCESS) {
        pmgr_error("Could not copy KVS name into buffer @ file %s:%d",
            __FILE__, __LINE__
        );
        PMI_Abort(1, "Could not copy KVS name into buffer");
    }

    /* insert our IP address, keyed by our rank */
    if (snprintf(keystr, keylen, "%d", rank) >= keylen) {
        pmgr_error("Could not copy rank into key buffer @ file %s:%d",
            __FILE__, __LINE__
        );
        PMI_Abort(1, "Could not copy rank into key buffer");
    }
    if (snprintf(valstr, vallen, "%s:%d", inet_ntoa(*ip), port) >= vallen) {
        pmgr_error("Could not copy ip:port into value buffer @ file %s:%d",
            __FILE__, __LINE__
        );
        PMI_Abort(1, "Could not copy ip:port into value buffer");
    }
    if (PMI_KVS_Put(kvsstr, keystr, valstr) != PMI_SUCCESS) {
        pmgr_error("Failed to put IP address in PMI %s/%s @ file %s:%d",
            keystr, valstr, __FILE__, __LINE__
        );
        PMI_Abort(1, "Failed to put IP address in PMI");
    }

    /* commit our ip:port value and issue a barrier */
    if (PMI_KVS_Commit(kvsstr) != PMI_SUCCESS) {
        pmgr_error("Failed to commit IP KVS in PMI @ file %s:%d",
            __FILE__, __LINE__
        );
        PMI_Abort(1, "Failed to commit IP address in PMI");
    }
    if (PMI_Barrier() != PMI_SUCCESS) {
        pmgr_error("Failed to complete barrier after commit in PMI @ file %s:%d",
            __FILE__, __LINE__
        );
        PMI_Abort(1, "Failed to complete barrier after commit in PMI");
    }

    /* extract ip:port for each process */
    for(i=0; i < ranks; i++) {
        /* build the key for this process */
        if (snprintf(keystr, keylen, "%d", i) >= keylen) {
            pmgr_error("Could not copy rank %d into key buffer @ file %s:%d",
                i, __FILE__, __LINE__
            );
            PMI_Abort(1, "Could not copy rank into key buffer");
        }

        /* extract value for this process */
        if (PMI_KVS_Get(kvsstr, keystr, valstr, vallen) != PMI_SUCCESS) {
            pmgr_error("Could not get key/value for %s @ file %s:%d",
                keystr, __FILE__, __LINE__
            );
            PMI_Abort(1, "Could not copy rank into key buffer");
        }

        /* break the ip:port string */
        char* ipstr = strtok(valstr, ":");
        char* portstr = strtok(NULL, ":");

        /* prepare IP and port to be stored in the table */
        struct in_addr ip_tmp;
        if (inet_aton(ipstr, &ip_tmp) == 0) {
            pmgr_error("Failed to convert dotted decimal notation to struct in_addr for %s @ file %s:%d",
                ipstr, __FILE__, __LINE__
            );
            PMI_Abort(1, "Could not convert IP address string to struct");
        }
        short port_tmp = atoi(portstr);

        /* write the ip and port to our table */
        memcpy((char*)table + i*addr_size,                  &ip_tmp,   sizeof(ip_tmp));
        memcpy((char*)table + i*addr_size + sizeof(ip_tmp), &port_tmp, sizeof(port_tmp));
    }

    /* free the kvs name, key, and value */
    pmgr_free(valstr);
    pmgr_free(keystr);
    pmgr_free(kvsstr);

    return PMGR_SUCCESS;
}
#endif /* ifdef HAVE_PMI */

/* open socket tree across MPI tasks */
int pmgr_tree_open_mpirun(pmgr_tree_t* t, int ranks, int rank)
{
    int i;

    /* initialize the tree as a binomial tree */
    pmgr_tree_init_binomial(t, ranks, rank);

    /* create a socket to accept connection from parent */
    int sockfd = -1;
    struct in_addr ip;
    short port;
    if (pmgr_open_listening_socket(NULL, &sockfd, &ip, &port) != PMGR_SUCCESS) {
        pmgr_error("Creating listening socket @ file %s:%d",
            __FILE__, __LINE__
        );
        exit(1);
    }

    /* allocate buffer to receive ip:port table for all tasks */
    int sendcount = sizeof(ip) + sizeof(port);
    void* recvbuf = (void*) pmgr_malloc(sendcount * t->ranks, "Receive buffer for socket table");

    /* fill in send buffer with our ip:port */
    void* sendbuf = (void*) pmgr_malloc(sendcount, "Send buffer for socket data");
    memcpy(sendbuf, &ip, sizeof(ip));
    memcpy((char*)sendbuf + sizeof(ip), &port, sizeof(port));

    /* gather ip:port info to rank 0 via mpirun -- explicitly call mpirun_gather since tcp tree is not setup */
    pmgr_mpirun_gather(sendbuf, sendcount, recvbuf, 0);

    pmgr_free(sendbuf);

    /* if i'm not rank 0, accept a connection (from parent) and receive socket table */
    if (t->rank != 0) {
        socklen_t parent_len;
        struct sockaddr parent_addr;
	parent_len = sizeof(parent_addr);
        t->parent_fd = pmgr_accept_parent(sockfd, (struct sockaddr *) &parent_addr, &parent_len);
        if (t->parent_fd >= 0) {
            /* if we're not using PMI, we need to read the ip:port table */
            if (pmgr_read_collective(t->parent_fd, recvbuf, sendcount * t->ranks) < 0) {
                pmgr_error("Receiving IP:port table from parent failed @ file %s:%d",
                    __FILE__, __LINE__
                );
                pmgr_abort_trees();
                exit(1);
            }
        } else {
            pmgr_error("Failed to accept parent connection (%m errno=%d) @ file %s:%d",
                errno, __FILE__, __LINE__
            );
            pmgr_abort_trees();
            exit(1);
        }
    }

    /* for each child, open socket connection and forward socket table */
    for (i=0; i < t->num_child; i++) {
        int c = t->child[i];
        t->child_ip[i]   = * (struct in_addr *)  ((char*)recvbuf + sendcount*c);
        t->child_port[i] = * (short*) ((char*)recvbuf + sendcount*c + sizeof(ip));
        t->child_fd[i]   = pmgr_connect_child(t->child_ip[i], t->child_port[i]);
        if (t->child_fd[i] >= 0) {
            /* connected to child, now forward IP table */
            if (pmgr_write_collective(t->child_fd[i], recvbuf, sendcount * t->ranks) < 0) {
                pmgr_error("Writing IP:port table to child (rank %d) at %s:%d failed @ file %s:%d",
                    t->child[i], inet_ntoa(t->child_ip[i]), t->child_port[i], __FILE__, __LINE__
                );
                pmgr_abort_trees();
                exit(1);
            }
        } else {
            /* failed to connect to child */
            pmgr_error("Connecting to child (rank %d) at %s:%d failed @ file %s:%d",
                t->child[i], inet_ntoa(t->child_ip[i]), t->child_port[i], __FILE__, __LINE__
            );
            pmgr_abort_trees();
            exit(1);
        }
    }

    /* free off the ip:port table */
    pmgr_free(recvbuf);

    /* close our listening socket */
    if (sockfd >= 0) {
        close(sockfd);
        sockfd = -1;
    }

    /* mark the tree as being open */
    t->is_open = 1;

    /* check whether everyone succeeded in connecting */
    pmgr_tree_check(t, 1);

    return PMGR_SUCCESS;
}

/* open socket tree across MPI tasks */
int pmgr_tree_open(pmgr_tree_t* t, int ranks, int rank)
{
    if (mpirun_use_shm) {
        int rc = pmgr_tree_open_slurm(t, ranks, rank);
        return rc;
    }

    if (mpirun_use_pmi) {
#ifdef HAVE_PMI
        /* allocate space to hold a table of ip:port entries from each rank */
        size_t addr_size = sizeof(struct in_addr) + sizeof(short);
        void* table = malloc(ranks * addr_size);

        /* create a socket to accept connection from parent */
        int sockfd = -1;
        struct in_addr ip;
        short port;
        if (pmgr_open_listening_socket(NULL, &sockfd, &ip, &port) != PMGR_SUCCESS) {
            pmgr_error("Creating listening socket @ file %s:%d",
                __FILE__, __LINE__
            );
            exit(1);
        }

        /* collect entries from all procs via PMI */
        pmgr_tree_table_pmi(ranks, rank, &ip, port, table);

        /* open the tree using those entries */
        pmgr_tree_open_table(t, ranks, rank, table, sockfd);

        /* free off the ip:port table */
        if (table != NULL) {
            free(table);
            table = NULL;
        }

        /* close our listening socket */
        if (sockfd >= 0) {
            close(sockfd);
            sockfd = -1;
        }
#endif
    } else {
        /* otherwise we bounce off of mpirun to setup our tree */
        pmgr_tree_open_mpirun(t, ranks, rank);
    }

    return PMGR_SUCCESS;
}

/*
 * =============================
 * Colletive implementations over tree
 * As written, these algorithms work for any tree whose children collectively
 * cover a consecutive range of ranks starting with the rank one more than the
 * parent.  Further more, the "last" child should be the nearest and the "first"
 * child should be the one furthest away from the parent.
 * =============================
 */

/* broadcast size bytes from buf on rank 0 using socket tree */
int pmgr_tree_bcast(pmgr_tree_t* t, void* buf, int size)
{
    /* if i'm not rank 0, receive data from parent */
    if (t->rank != 0) {
        if (pmgr_read_collective(t->parent_fd, buf, size) < 0) {
            pmgr_error("Receiving broadcast data from parent failed @ file %s:%d",
                __FILE__, __LINE__
            );
            pmgr_abort_trees();
            exit(1);
        }
    }

    /* for each child, forward data */
    int i;
    for(i = 0; i < t->num_child; i++) {
        if (pmgr_write_collective(t->child_fd[i], buf, size) < 0) {
            pmgr_error("Broadcasting data to child (rank %d) at %s:%d failed @ file %s:%d",
                t->child[i], inet_ntoa(t->child_ip[i]), t->child_port[i], __FILE__, __LINE__
            );
            pmgr_abort_trees();
            exit(1);
        }
    }

    /* check that everyone succeeded */
    int all_success = pmgr_tree_check(t, 1);
    return all_success;
}

/* gather sendcount bytes from sendbuf on each task into recvbuf on rank 0 */
int pmgr_tree_gather(pmgr_tree_t* t, void* sendbuf, int sendcount, void* recvbuf)
{
    int bigcount = (t->num_child_incl+1) * sendcount;
    void* bigbuf = recvbuf;

    /* if i'm not rank 0, create a temporary buffer to gather child data */
    if (t->rank != 0) {
        bigbuf = (void*) pmgr_malloc(bigcount, "Temporary gather buffer in pmgr_gather_tree");
    }

    /* copy my own data into buffer */
    memcpy(bigbuf, sendbuf, sendcount);

    /* if i have any children, receive their data */
    int i;
    int offset = sendcount;
    for(i = t->num_child-1; i >= 0; i--) {
        if (pmgr_read_collective(t->child_fd[i], (char*)bigbuf + offset, sendcount * t->child_incl[i]) < 0) {
            pmgr_error("Gathering data from child (rank %d) at %s:%d failed @ file %s:%d",
                t->child[i], inet_ntoa(t->child_ip[i]), t->child_port[i], __FILE__, __LINE__
            );
            pmgr_abort_trees();
            exit(1);
        }
        offset += sendcount * t->child_incl[i];
    }

    /* if i'm not rank 0, send to parent and free temporary buffer */
    if (t->rank != 0) {
        if (pmgr_write_collective(t->parent_fd, bigbuf, bigcount) < 0) {
            pmgr_error("Sending gathered data to parent failed @ file %s:%d",
                __FILE__, __LINE__
            );
            pmgr_abort_trees();
            exit(1);
        }
        pmgr_free(bigbuf);
    }

    /* check that everyone succeeded */
    int all_success = pmgr_tree_check(t, 1);
    return all_success;
}

/* scatter sendcount byte chunks from sendbuf on rank 0 to recvbuf on each task */
int pmgr_tree_scatter(pmgr_tree_t* t, void* sendbuf, int sendcount, void* recvbuf)
{
    int bigcount = (t->num_child_incl+1) * sendcount;
    void* bigbuf = sendbuf;

    /* if i'm not rank 0, create a temporary buffer to receive data from parent */
    if (t->rank != 0) {
        bigbuf = (void*) pmgr_malloc(bigcount, "Temporary scatter buffer in pmgr_scatter_tree");
        if (pmgr_read_collective(t->parent_fd, bigbuf, bigcount) < 0) {
            pmgr_error("Receiving scatter data from parent failed @ file %s:%d",
                __FILE__, __LINE__
            );
            pmgr_abort_trees();
            exit(1);
        }
    }

    /* if i have any children, scatter data to them */
    int i;
    for(i = 0; i < t->num_child; i++) {
        if (pmgr_write_collective(t->child_fd[i], (char*)bigbuf + sendcount * (t->child[i] - t->rank), sendcount * t->child_incl[i]) < 0) {
            pmgr_error("Scattering data to child (rank %d) at %s:%d failed @ file %s:%d",
                t->child[i], inet_ntoa(t->child_ip[i]), t->child_port[i], __FILE__, __LINE__
            );
            pmgr_abort_trees();
            exit(1);
        }
    }

    /* copy my data into my receive buffer */
    memcpy(recvbuf, bigbuf, sendcount);

    /* if i'm not rank 0, free temporary buffer */
    if (t->rank != 0) {
        pmgr_free(bigbuf);
    }

    /* check that everyone succeeded */
    int all_success = pmgr_tree_check(t, 1);
    return all_success;
}

#define REDUCE_SUM (1)
#define REDUCE_MAX (2)

/* computes maximum integer across all processes and saves it to recvbuf on rank 0 */
int pmgr_tree_reduceint(pmgr_tree_t* t, int* sendint, int* recvint, int op)
{
    /* initialize current value using our value */
    int val = *sendint;

    /* if i have any children, receive and reduce their data */
    int i;
    for(i = t->num_child-1; i >= 0; i--) {
        int child_value;
        if (pmgr_read_collective(t->child_fd[i], &child_value, sizeof(int)) < 0) {
            pmgr_error("Reducing data from child (rank %d) at %s:%d failed @ file %s:%d",
                t->child[i], inet_ntoa(t->child_ip[i]), t->child_port[i], __FILE__, __LINE__
            );
            pmgr_abort_trees();
            exit(1);
        } else {
            if (op == REDUCE_SUM) {
                val += child_value;
            } else if (op == REDUCE_MAX && child_value > val) {
                val = child_value;
            }
        }
    }

    /* if i'm not rank 0, send to parent, otherwise copy val to recvint */
    if (t->rank != 0) {
        if (pmgr_write_collective(t->parent_fd, &val, sizeof(int)) < 0) {
            pmgr_error("Sending reduced data to parent failed @ file %s:%d",
                __FILE__, __LINE__
            );
            pmgr_abort_trees();
            exit(1);
        }
    } else {
        /* this is rank 0, save val to recvint */
        *recvint = val;
    }

    /* check that everyone succeeded */
    int all_success = pmgr_tree_check(t, 1);
    return all_success;
}

/* computes maximum integer across all processes and saves it to recvbuf on rank 0 */
int pmgr_tree_reducemaxint(pmgr_tree_t* t, int* sendint, int* recvint)
{
    int rc = pmgr_tree_reduceint(t, sendint, recvint, REDUCE_MAX);
    return rc;
}

/* collects all data from all tasks into recvbuf which is at most max_recvcount bytes big,
 * effectively works like a gatherdv */
int pmgr_tree_aggregate(pmgr_tree_t* t, const void* sendbuf, int sendcount, void* recvbuf, int max_recvcount, int* out_count)
{
    /* get total count of incoming bytes */
    int total;
    if (pmgr_tree_reduceint(t, &sendcount, &total, REDUCE_SUM) != PMGR_SUCCESS) {
        pmgr_error("Summing values to rank 0 failed @ file %s:%d",
            __FILE__, __LINE__
        );
        pmgr_abort_trees();
        exit(1);
    }
    if (pmgr_tree_bcast(t, &total, sizeof(total)) != PMGR_SUCCESS) {
        pmgr_error("Bcasting sum from rank 0 failed @ file %s:%d",
            __FILE__, __LINE__
        );
        pmgr_abort_trees();
        exit(1);
    }

    /* check that user's buffer is big enough */
    if (total > max_recvcount) {
        pmgr_error("Receive buffer is too small to hold incoming data 0 failed @ file %s:%d",
            __FILE__, __LINE__
        );
        pmgr_abort_trees();
        exit(1);
    }

    /* copy my own data into buffer */
    memcpy(recvbuf, sendbuf, sendcount);

    /* if i have any children, receive their data */
    int i;
    int offset = sendcount;
    for(i = t->num_child-1; i >= 0; i--) {
        /* read number of incoming bytes */
        int incoming;
        if (pmgr_read_collective(t->child_fd[i], &incoming, sizeof(int)) < 0) {
            pmgr_error("Receiving incoming byte count from child (rank %d) at %s:%d failed @ file %s:%d",
                t->child[i], inet_ntoa(t->child_ip[i]), t->child_port[i], __FILE__, __LINE__
            );
            pmgr_abort_trees();
            exit(1);
        }

        /* now receive the data */
        if (pmgr_read_collective(t->child_fd[i], recvbuf + offset, incoming) < 0) {
            pmgr_error("Gathering data from child (rank %d) at %s:%d failed @ file %s:%d",
                t->child[i], inet_ntoa(t->child_ip[i]), t->child_port[i], __FILE__, __LINE__
            );
            pmgr_abort_trees();
            exit(1);
        }

        /* increase our offset */
        offset += incoming;
    }

    /* if i'm not rank 0, send to parent and free temporary buffer */
    if (t->rank != 0) {
        /* write number of bytes we'll send to parent */
        if (pmgr_write_collective(t->parent_fd, &offset, sizeof(int)) < 0) {
            pmgr_error("Sending byte count to parent failed @ file %s:%d",
                __FILE__, __LINE__
            );
            pmgr_abort_trees();
            exit(1);
        }

        /* now write the bytes */
        if (pmgr_write_collective(t->parent_fd, recvbuf, offset) < 0) {
            pmgr_error("Sending gathered data to parent failed @ file %s:%d",
                __FILE__, __LINE__
            );
            pmgr_abort_trees();
            exit(1);
        }
    }

    /* finally bcast whole buffer from root */
    if (pmgr_tree_bcast(t, recvbuf, total) != PMGR_SUCCESS) {
        pmgr_error("Bcasting data from rank 0 failed @ file %s:%d",
            __FILE__, __LINE__
        );
        pmgr_abort_trees();
        exit(1);
    }

    /* record number of bytes actually gathered */
    *out_count = total;

    /* check that everyone succeeded */
    int all_success = pmgr_tree_check(t, 1);
    return all_success;
}

/* alltoall sendcount bytes from each process to each process via tree */
int pmgr_tree_alltoall(pmgr_tree_t* t, void* sendbuf, int sendcount, void* recvbuf)
{
    /* compute total number of bytes we'll receive from children and send to our parent */
    int tmp_recv_count = (t->num_child_incl)   * t->ranks * sendcount;
    int tmp_send_count = (t->num_child_incl+1) * t->ranks * sendcount;

    /* allocate temporary buffers to hold the data */
    void* tmp_recv_buf = NULL;
    void* tmp_send_buf = NULL;
    if (tmp_recv_count > 0) {
        tmp_recv_buf = (void*) pmgr_malloc(tmp_recv_count, "Temporary recv buffer");
    }
    if (tmp_send_count > 0) {
        tmp_send_buf = (void*) pmgr_malloc(tmp_send_count, "Temporary send buffer");
    }

    /* if i have any children, receive their data */
    int i;
    int offset = 0;
    for(i = t->num_child-1; i >= 0; i--) {
        if (pmgr_read_collective(t->child_fd[i], (char*)tmp_recv_buf + offset, t->ranks * sendcount * t->child_incl[i]) < 0) {
            pmgr_error("Gathering data from child (rank %d) at %s:%d failed @ file %s:%d",
                t->child[i], inet_ntoa(t->child_ip[i]), t->child_port[i], __FILE__, __LINE__
            );
            pmgr_abort_trees();
            exit(1);
        }
        offset += t->ranks * sendcount * t->child_incl[i];
    }

    /* order data by destination process */
    int j;
    offset = 0;
    for(j = 0; j < t->ranks; j++) {
        /* copy my own data into send buffer */
        memcpy(tmp_send_buf + offset, (char*)sendbuf + sendcount * j, sendcount);
        offset += sendcount;

        /* copy each entry of our child data */
        int child_count = 0;
        for(i = t->num_child-1; i >= 0; i--) {
            memcpy(tmp_send_buf + offset,
                   (char*)tmp_recv_buf + t->ranks * sendcount * child_count + sendcount * j * t->child_incl[i],
                   sendcount * t->child_incl[i]
            );
            offset += sendcount * t->child_incl[i];
            child_count += t->child_incl[i];
        }
    }

    /* if i'm not rank 0, send to parent and free temporary buffer */
    if (t->rank != 0) {
        if (pmgr_write_collective(t->parent_fd, tmp_send_buf, tmp_send_count) < 0) {
            pmgr_error("Sending alltoall data to parent failed @ file %s:%d",
                __FILE__, __LINE__
            );
            pmgr_abort_trees();
            exit(1);
        }
    }

    /* scatter data from rank 0 */
    pmgr_tree_scatter(t, tmp_send_buf, sendcount * t->ranks, recvbuf);

    /* free our temporary buffers */
    if (tmp_recv_buf != NULL) {
      free(tmp_recv_buf);
      tmp_recv_buf = NULL;
    }
    if (tmp_send_buf != NULL) {
      free(tmp_send_buf);
      tmp_send_buf = NULL;
    }

    /* check that everyone succeeded */
    int all_success = pmgr_tree_check(t, 1);
    return all_success;
}
