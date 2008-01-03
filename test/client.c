#include <stdio.h>
#include <stdlib.h>
#include "pmgr_collective_client.h"

int main(int argc, char* argv[])
{
  int ranks, my_rank, my_id;
  char** procs;

  /* initialize the client (read environment variables) */
  if (pmgr_init(&argc, &argv, &ranks, &my_rank, &my_id, &procs) != PMGR_SUCCESS) {
    printf("Failed to init\n");
    exit(1);
  }

  /* open connections (connect to launcher and build the TCP tree) */
  if (pmgr_open() != PMGR_SUCCESS) {
    printf("Failed to open\n");
    exit(1);
  }

  /* test pmgr_barrier */
  if (pmgr_barrier() != PMGR_SUCCESS) {
    printf("Barrier failed\n");
    exit(1);
  }

  /* close connections (close connection with launcher and tear down the TCP tree) */
  if (pmgr_close() != PMGR_SUCCESS) {
    printf("Failed to close\n");
    exit(1);
  }

  /* shutdown */
  if (pmgr_finalize() != PMGR_SUCCESS) {
    printf("Failed to finalize\n");
    exit(1);
  }

  /* needed this sleep so that mpirun prints out all debug info (don't know why yet) */
  sleep(1);

  return 0;
}
