#include <stdio.h>
#include <stdlib.h>
#include "pmgr_collective_client.h"

int main(int argc, char* argv[])
{
  int ranks, my_rank, my_id;
  char** procs;

  if (pmgr_init(&argc, &argv, &ranks, &my_rank, &my_id, &procs) != PMGR_SUCCESS) {
    printf("Failed to init\n");
    exit(1);
  }

  if (pmgr_open() != PMGR_SUCCESS) {
    printf("Failed to open\n");
    exit(1);
  }

  if (pmgr_close() != PMGR_SUCCESS) {
    printf("Failed to close\n");
    exit(1);
  }

  if (pmgr_finalize() != PMGR_SUCCESS) {
    printf("Failed to finalize\n");
    exit(1);
  }

  return 0;
}
