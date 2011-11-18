#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <sys/file.h> /* flock() */
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/mman.h>

#include "pmgr_collective_client.h"
#include "pmgr_collective_ranges.h"
#include "pmgr_collective_client_tree.h"
#include "pmgr_collective_client_slurm.h"

#ifndef PMGR_CLIENT_SLURM_PREFIX
#define PMGR_CLIENT_SLURM_PREFIX ("/dev/shm")
#endif

/* wait until file reaches expected size, read in data, and return number of bytes read */
static size_t pmgr_slurm_wait_check_in(const char* file, int max_local, void* mem, size_t expected_size)
{
    /* wait until the file becomes the right size */
    off_t size = -1;
    while (size != (off_t) expected_size) {
      struct stat filestat;
      stat(file, &filestat);
      size = filestat.st_size;
    }

    /* read the data into mem */
    int fd = open(file, O_RDONLY);
    if (fd < 0) {
      return PMGR_FAILURE;
    }

    read(fd, mem, size);

    close(fd);

    return (size_t) size;
}

/* append our rank, ip, and port to file */
static int pmgr_slurm_check_in(const char* file, int local, int rank)
{
    int rc = PMGR_SUCCESS;

    int fd = -1;

    /* open file */
    /* we're careful to set permissions so only the current user can access the file,
     * which prevents another user from attaching to the file while we have it open */
    if (local == 0) {
        /* only the leader creates the file */
        fd = open(file, O_RDWR | O_CREAT | O_TRUNC, S_IRWXU);
    } else {
        /* don't try to open until we see the file */
        while (access(file, F_OK) < 0) { ; }

        /* we either timed out, or the file now exists */
        if (access(file, F_OK) == 0) {
            /* the file exists, so try to open it */
            fd = open(file, O_RDWR);
        }
    }

    /* check that we got a file */
    if (fd < 0) {
        return PMGR_FAILURE;
    }

    /* wait for an exclusive lock */
    if (flock(fd, LOCK_EX) != 0) {
        pmgr_error("Failed to acquire file lock on %s: flock(%d, %d) errno=%d %m @ %s:%d",
            file, fd, LOCK_EX, errno, __FILE__, __LINE__
        );
        return PMGR_FAILURE;
    }

    /* seek to end */
    lseek(fd, 0, SEEK_END);

    /* append our data to file */
    write(fd, &rank, sizeof(rank));

    /* fsync */
    fsync(fd);

    /* unlock the file */
    if (flock(fd, LOCK_UN) != 0) {
        pmgr_error("Failed to release file lock on %s: flock(%d, %d) errno=%d %m @ %s:%d",
            file, fd, LOCK_UN, errno, __FILE__, __LINE__
        );
        return PMGR_FAILURE;
    }

    /* close */
    close(fd);

    return rc;
}

static int pmgr_slurm_get_max_local(const char* tasks_per_node)
{
    int max = -1;

    char* scratch = strdup(tasks_per_node);
    if (scratch != NULL) {
        int count = 0;
        const char* p = tasks_per_node;
        while(*p >= '0' && *p <= '9') {
            scratch[count] = *p;
            count++;
            p++;
        }
        scratch[count] = '\0';

        if (count > 0) {
            max = atoi(scratch);
        }

        free(scratch);
        scratch = NULL;
    }

    return max;
}

/* initialize the memory region we use to execute barriers,
 * this must be done before any procs try to issue a barrier */
static void pmgr_slurm_barrier_init(void* buf, int ranks, int rank)
{
    int i;
    int* mem = buf;
    for (i = 0; i < ranks*2; i++) {
        mem[i] = 0;
    }
}

/* rank 0 waits until all procs have set their field to 1 */
static void pmgr_slurm_barrier_signal(void* buf, int ranks, int rank)
{
    int i;
    int* mem = buf;
    if (rank == 0) {
        /* wait until all procs have set their field to 1 */
        for (i = 1; i < ranks; i++) { 
            while (mem[i] == 0) {
                ; /* MEM_READ_SYNC */
            }
        }

        /* set all fields back to 0 for next iteration */
        for (i = 1; i < ranks; i++){ 
            mem[i] = 0;
        }
    } else {
        /* just need to set our field to 1 */
        mem[rank] = 1;
    }
    /* MEM_WRITE_SYNC */
}

/* all ranks wait for rank 0 to set their field to 1 */
static void pmgr_slurm_barrier_wait(void* buf, int ranks, int rank)
{
    int i;
    int* mem = buf + ranks * sizeof(int);
    if (rank == 0) {
        /* change all flags to 1 */
        for (i = 1; i < ranks; i++){ 
            mem[i] = 1;
        }
    } else {
        /* wait until leader flips our flag to 1 */
        while (mem[rank] == 0) {
            ; /* MEM_READ_SYNC */
        }

        /* set our flag back to 0 for the next iteration */
        mem[rank] = 0;
    }
    /* MEM_WRITE_SYNC */
}

/* execute a shared memory barrier, note this is a two phase process
 * to prevent procs from escaping ahead */
static void pmgr_slurm_barrier(void* buf, int ranks, int rank)
{
    pmgr_slurm_barrier_signal(buf, ranks, rank);
    pmgr_slurm_barrier_wait(  buf, ranks, rank);
}

/* attach to the shared memory segment, rank 0 should do this before any others */
static void* pmgr_slurm_attach_shm_segment(size_t size, const char* file, int rank)
{
    /* open the file on all processes,
     * we're careful to set permissions so only the current user can access the file,
     * which prevents another user from attaching to the file while we have it open */
    int fd = -1;
    if (rank == 0) {
        fd = open(file, O_RDWR | O_CREAT, S_IRWXU);
    } else {
        fd = open(file, O_RDWR);
    }
    if (fd < 0) {
        return NULL;
    }

    /* set the size, but be careful not to actually touch any memory */
    if (rank == 0) {
        ftruncate(fd, 0);
        ftruncate(fd, (off_t) size);
        lseek(fd, 0, SEEK_SET);
    }

    /* mmap the file on all tasks */
    void* ptr = mmap(0, (off_t) size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) {
        pmgr_error("Failed to map shared memory segment (errno=%d %m) @ file %s:%d",
            errno, __FILE__, __LINE__
        );
        return NULL;
    }

    /* close the file descriptor */
    int rc = close(fd);
    if (rc == -1) {
//      return NULL;
    }

    return ptr;
}

static void pmgr_slurm_exchange_leaders(void* sendbuf, size_t sendcount, void* recvbuf, size_t* recvcount)
{
    memcpy(recvbuf, sendbuf, sendcount);
    *recvcount = sendcount;
}

int pmgr_tree_open_slurm(pmgr_tree_t* t, int ranks, int rank)
{
    int rc = PMGR_SUCCESS;
    char* value;

    char* prefix = NULL;
    char* slurm_step_nodelist = NULL;
    char* slurm_step_tasks_per_node = NULL;
    int slurm_jobid   = -1;
    int slurm_stepid  = -1;
    int slurm_nodeid  = -1;
    int slurm_localid = -1;

    /* TODO: eventually, read this from enviroment to allow user to override */
    prefix = strdup(PMGR_CLIENT_SLURM_PREFIX);

    /* read SLURM environment variables */
    if ((value = getenv("SLURM_JOBID")) != NULL) {
        slurm_jobid = atoi(value);
    } else {
        pmgr_error("Failed to read SLURM_JOBID @ file %s:%d",
            __FILE__, __LINE__
        );
        exit(1);
    }

    if ((value = getenv("SLURM_STEPID")) != NULL) {
        slurm_stepid = atoi(value);
    } else {
        pmgr_error("Failed to read SLURM_STEPID @ file %s:%d",
            __FILE__, __LINE__
        );
        exit(1);
    }

    if ((value = getenv("SLURM_NODEID")) != NULL) {
        slurm_nodeid = atoi(value);
    } else {
        pmgr_error("Failed to read SLURM_NODEID @ file %s:%d",
            __FILE__, __LINE__
        );
        exit(1);
    }

    if ((value = getenv("SLURM_LOCALID")) != NULL) {
        slurm_localid = atoi(value);
    } else {
        pmgr_error("Failed to read SLURM_LOCALID @ file %s:%d",
            __FILE__, __LINE__
        );
        exit(1);
    }

    if ((value = getenv("SLURM_STEP_NODELIST")) != NULL) {
        slurm_step_nodelist = strdup(value);
    } else {
        pmgr_error("Failed to read SLURM_STEP_NODELIST @ file %s:%d",
            __FILE__, __LINE__
        );
        exit(1);
    }

    if ((value = getenv("SLURM_STEP_TASKS_PER_NODE")) != NULL) {
        slurm_step_tasks_per_node = strdup(value);
    } else {
        pmgr_error("Failed to read SLURM_STEP_TASKS_PER_NODE @ file %s:%d",
            __FILE__, __LINE__
        );
        exit(1);
    }

    int nodelist_size;
    pmgr_range_nodelist_size("atlas37", &nodelist_size);
    pmgr_range_nodelist_size("atlas[37]", &nodelist_size);
    pmgr_range_nodelist_size("atlas[37-39]", &nodelist_size);
    pmgr_range_nodelist_size("atlas[37-39,43]", &nodelist_size);
    pmgr_range_nodelist_size("atlas[37-39,43,57]", &nodelist_size);
    pmgr_range_nodelist_size("atlas37,atlas43,atlas57", &nodelist_size);

    char nodelist_nth[1024];
    pmgr_range_nodelist_nth("atlas37", 1, nodelist_nth, sizeof(nodelist_nth));
    pmgr_range_nodelist_nth("atlas[37]", 2, nodelist_nth, sizeof(nodelist_nth));
    pmgr_range_nodelist_nth("atlas[37-39]", 2, nodelist_nth, sizeof(nodelist_nth));
    pmgr_range_nodelist_nth("atlas[37-39,43]", 4, nodelist_nth, sizeof(nodelist_nth));
    pmgr_range_nodelist_nth("atlas[37-39,43,57]", 5, nodelist_nth, sizeof(nodelist_nth));
    pmgr_range_nodelist_nth("atlas37,atlas43,atlas57", 2, nodelist_nth, sizeof(nodelist_nth));

    /* extract number of procs on node from slurm_step_tasks_per_node */
    int max_local = pmgr_slurm_get_max_local(slurm_step_tasks_per_node);

    /* build file name: jobid.stepid.checkin */
    char file_check_in[1024];
    if (snprintf(file_check_in, sizeof(file_check_in), "%s/%d.%d.checkin", prefix, slurm_jobid, slurm_stepid) >= sizeof(file_check_in)) {
        pmgr_error("Filename too long %s/%d.%d.checkin @ file %s:%d",
            prefix, slurm_jobid, slurm_stepid, __FILE__, __LINE__
        );
        exit(1);
    }

    /* build file name: jobid.stepid.table */
    char file_table[1024];
    if (snprintf(file_table, sizeof(file_table), "%s/%d.%d.table", prefix, slurm_jobid, slurm_stepid) >= sizeof(file_table)) {
        pmgr_error("Filename too long %s/%d.%d.table @ file %s:%d",
            prefix, slurm_jobid, slurm_stepid, __FILE__, __LINE__
        );
        exit(1);
    }

    /* number of bytes for each address we'll store in table */
    size_t addr_size = sizeof(struct in_addr) + sizeof(short);

    /* compute the size of the shared memory segment */
    void* segment = NULL;
    size_t barrier_offset = 0;
    size_t node_offset    = barrier_offset + max_local * 2 * sizeof(int);
    size_t count_offset   = node_offset    + max_local * addr_size;
    size_t table_offset   = count_offset   + sizeof(int);
    off_t  segment_size   = (off_t) (table_offset + ranks * addr_size);

    /* have leader create and initialize shared memory segment *before* checking in */
    if (slurm_localid == 0) {
      /* create the shm segment */
      segment = pmgr_slurm_attach_shm_segment(segment_size, file_table, slurm_localid);

      /* intialize the shared memory for communication */
      if (segment != NULL) {
        /* initialize space for the barrier messages */
        pmgr_slurm_barrier_init(segment + barrier_offset, max_local, slurm_localid);
      }
    }

    /* add our (rank,ip,port) entry to file using flock to ensure exclusive writes */
    if (pmgr_slurm_check_in(file_check_in, slurm_localid, rank) != PMGR_SUCCESS) {
        pmgr_error("Failed to write rank to file @ file %s:%d",
            __FILE__, __LINE__
        );
        exit(1);
    }

    void*  data_node = NULL;
    size_t data_node_size = 0;
    if (slurm_localid == 0) {
        /* compute total number of bytes we'd expect given max_local procs will add entries */
        size_t max_size = max_local * sizeof(int);
        data_node = pmgr_malloc(max_size, "Buffer to store data from local node");

        /* wait until all procs have written to file */
        data_node_size = pmgr_slurm_wait_check_in(file_check_in, max_local, data_node, max_size);
    } else {
        /* attach to the shm segment */
        segment = pmgr_slurm_attach_shm_segment(segment_size, file_table, slurm_localid);
    }

    /* issue a barrier to signal that procs can write their IP:port to shared memory */
    pmgr_slurm_barrier(segment + barrier_offset, max_local, slurm_localid);

    /* create a socket to accept connection from parent */
    int sockfd = -1;
    struct in_addr ip;
    short port;
    if (pmgr_open_listening_socket(&sockfd, &ip, &port) != PMGR_SUCCESS) {
        pmgr_error("Creating listening socket @ file %s:%d",
            __FILE__, __LINE__
        );
        exit(1);
    }

    /* write to our rank,ip,port info to shared memory */
    size_t entry_size   = sizeof(int) + addr_size;
    void*  entry_offset = segment + node_offset + slurm_localid * entry_size;
    memcpy(entry_offset, &rank, sizeof(rank));
    entry_offset += sizeof(rank);
    memcpy(entry_offset, &ip, sizeof(ip));
    entry_offset += sizeof(ip);
    memcpy(entry_offset, &port, sizeof(port));

    /* signal to rank 0 that all procs are done */
    pmgr_slurm_barrier(segment + barrier_offset, max_local, slurm_localid);

    /* exchange data with other nodes and record all entries in table */
    if (slurm_localid == 0) {
        /* exchange data with other leaders */
        void* data_all = pmgr_malloc(ranks * entry_size, "Buffer to receive data from each node");
        size_t data_all_size;
        pmgr_slurm_exchange_leaders(data_node, data_node_size, data_all, &data_all_size);

        /* write ip:port values to table in shared memory, ordered by global rank */
        size_t offset = 0;
        int num_ranks = 0;
        while (offset < data_all_size) {
            int current_rank = *(int*) (data_all + offset);
            memcpy(segment + table_offset + current_rank * addr_size, data_all + offset + sizeof(int), addr_size);
            offset += entry_size;
            num_ranks++;
        }

        /* set the count */
        memcpy(segment + count_offset, &num_ranks, sizeof(int));

        /* free the buffers used to collect data */
        pmgr_free(data_all);
        pmgr_free(data_node);
    }

    /* signal to indicate that exchange is complete */
    pmgr_slurm_barrier(segment + barrier_offset, max_local, slurm_localid);

    /* check that number of entries matches number of ranks */
    int table_ranks = *(int*) (segment + count_offset);

    /* now that we have our table, open our tree */
    pmgr_tree_open_table(t, table_ranks, rank, segment + table_offset, sockfd);

    /* issue barrier to signal that files can be deleted */
    pmgr_slurm_barrier(segment + barrier_offset, max_local, slurm_localid);

    /* delete the files */
    if (slurm_localid == 0) {
        unlink(file_check_in);
        unlink(file_table);
    }

    /* TODO: unmap shared memory segment */

    /* close our listening socket */
    if (sockfd >= 0) {
        close(sockfd);
        sockfd = -1;
    }

    /* free off strdup'd strings */
    if (prefix != NULL) {
        free(prefix);
        prefix = NULL;
    }

    if (slurm_step_nodelist != NULL) {
        free(slurm_step_nodelist);
        slurm_step_nodelist = NULL;
    }

    if (slurm_step_tasks_per_node != NULL) {
        free(slurm_step_tasks_per_node);
        slurm_step_tasks_per_node = NULL;
    }

    return rc;
}
