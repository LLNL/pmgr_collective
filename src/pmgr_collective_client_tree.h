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

#ifndef _PMGR_COLLECTIVE_CLIENT_TREE_H
#define _PMGR_COLLECTIVE_CLIENT_TREE_H

#include "pmgr_collective_common.h"

#define PMGR_GROUP_TREE_NULL     (0)
#define PMGR_GROUP_TREE_BINOMIAL (1)

typedef struct pmgr_tree {
    int type;      /* type of group */
    int ranks;     /* number of ranks in group */
    int rank;      /* rank of process within group */
    int is_open;   /* records whether group is connected */
    int depth;     /* depth within the tree */
    int parent;    /* rank of parent within group */
    int parent_fd; /* socket to parent */
    int num_child; /* number of children this process has */
    int num_child_incl; /* total number of procs below parent (including itself) */
    int*            child;      /* rank of each child within group */
    int*            child_fd;   /* file descriptor to each child */
    int*            child_incl; /* number of procs each child is responsible for */
    struct in_addr* child_ip;   /* ip address of each child */
    short*          child_port; /* port of each child */
} pmgr_tree_t;

/*
 * =============================
 * This function is implemented in pmgr_collective_client.c
 * where the necessary variables are defined, but it is called
 * from many files dealing with trees.
 * =============================
 */

/* abort all open trees */
int pmgr_abort_trees();

/*
 * =============================
 * Initialize and free tree data structures
 * =============================
 */

/* initialize tree to null tree */
int pmgr_tree_init_null(pmgr_tree_t* t);

/* given number of ranks and our rank with the group, create a binomail tree
 * fills in our position within the tree and allocates memory to hold socket info */
int pmgr_tree_init_binomial(pmgr_tree_t* t, int ranks, int rank);

/* given number of ranks and our rank with the group, create a binary tree
 * fills in our position within the tree and allocates memory to hold socket info */
int pmgr_tree_init_binary(pmgr_tree_t* t, int ranks, int rank);

/* free all memory allocated in tree */
int pmgr_tree_free(pmgr_tree_t* t);

/*
 * =============================
 * Open, close, and abort trees
 * =============================
 */

/* returns 1 if tree is open, 0 otherwise */
int pmgr_tree_is_open(pmgr_tree_t* t);

/* close down socket connections for tree (parent and any children),
 * free memory for tree data structures */
int pmgr_tree_close(pmgr_tree_t* t);

/* send abort message across links, then close down tree */
int pmgr_tree_abort(pmgr_tree_t* t);

/* check whether all tasks report success, exit if someone failed */
int pmgr_tree_check(pmgr_tree_t* t, int value);

/* open socket tree across MPI tasks */
int pmgr_tree_open(pmgr_tree_t* t, int ranks, int rank);

/* given a table of ranks number of ip:port entries, open the tree */
int pmgr_tree_open_table(pmgr_tree_t* t, int ranks, int rank, const void* table, int sockfd);

/* given a table of ranks number of ip:port entries, open a tree */
int pmgr_tree_open_nodelist_scan(pmgr_tree_t* t, const char* nodelist, const char* portrange, int sockfd);

/*
 * =============================
 * Colletive implementations over tree
 * =============================
 */

/* broadcast size bytes from buf on rank 0 using socket tree */
int pmgr_tree_bcast(pmgr_tree_t* t, void* buf, int size);

/* gather sendcount bytes from sendbuf on each task into recvbuf on rank 0 */
int pmgr_tree_gather(pmgr_tree_t* t, void* sendbuf, int sendcount, void* recvbuf);

/* scatter sendcount byte chunks from sendbuf on rank 0 to recvbuf on each task */
int pmgr_tree_scatter(pmgr_tree_t* t, void* sendbuf, int sendcount, void* recvbuf);

/* computes maximum integer across all processes and saves it to recvbuf on rank 0 */
int pmgr_tree_reducemaxint(pmgr_tree_t* t, int* sendint, int* recvint);

/* collects all data from all tasks into recvbuf which is at most max_recvcount bytes big,
 * effectively works like a gatherdv */
int pmgr_tree_aggregate(pmgr_tree_t*, const void* sendbuf, int sendcount, void* recvbuf, int max_recvcount, int* out_count);

/* alltoall sendcount bytes from each process to each process via tree */
int pmgr_tree_alltoall(pmgr_tree_t* t, void* sendbuf, int sendcount, void* recvbuf);

#endif /* _PMGR_COLLECTIVE_CLIENT_TREE_H */