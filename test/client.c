#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "pmgr_collective_client.h"

int main(int argc, char* argv[])
{
  int ranks, my_rank, my_id;

  /* initialize the client (read environment variables) */
  if (pmgr_init(&argc, &argv, &ranks, &my_rank, &my_id) != PMGR_SUCCESS) {
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

  int max;
  if (pmgr_allreducemaxint(&my_rank, &max) != PMGR_SUCCESS) {
    printf("Allreducemaxint failed\n");
    exit(1);
  }
  printf("%d: Max int %d\n", my_rank, max);

  char** hosts;
  char*  hostsbuf;
  char   host[255];
  gethostname(host, 255);
  if (pmgr_allgatherstr(host, &hosts, &hostsbuf) != PMGR_SUCCESS) {
    printf("Allgatherstr failed\n");
    exit(1);
  }
  int i;
  if (my_rank == 0 || my_rank == ranks-1) { 
    for (i=0; i<ranks; i++) {
      printf("%d: hosts[%d] = %s\n", my_rank, i, hosts[i]);
    }
  }
  free(hosts);
  free(hostsbuf);

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
