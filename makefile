# To bootstap pmgr using PMI
OPT ?= -g -O0 -Wall -DHAVE_PMI -DMPIRUN_PMI_ENABLE=1
LIBS := -lpmi

#OPT ?= -g -O0 -Wall
PREFIX ?= /usr/local/tools/pmgr_collective
PMGR_INC := -I../include
PMGR_LIB := -L../lib -lpmgr_collective

PMGR_OBJS := \
	pmgr_collective_common.o \
	pmgr_collective_ranges.o \
	pmgr_collective_client_common.o \
	pmgr_collective_client_mpirun.o \
	pmgr_collective_client_tree.o \
	pmgr_collective_client_slurm.o \
	pmgr_collective_client.o \
	pmgr_collective_mpirun.o

all: clean
	mkdir lib
	mkdir include
	cd src && \
	  gcc $(OPT) $(CFLAGS) -fPIC -Wall -c -o pmgr_collective_common.o pmgr_collective_common.c && \
	  gcc $(OPT) $(CFLAGS) -fPIC -Wall -c -o pmgr_collective_ranges.o pmgr_collective_ranges.c && \
	  gcc $(OPT) $(CFLAGS) -fPIC -Wall -c -o pmgr_collective_client_common.o pmgr_collective_client_common.c && \
	  gcc $(OPT) $(CFLAGS) -fPIC -Wall -c -o pmgr_collective_client_mpirun.o pmgr_collective_client_mpirun.c && \
	  gcc $(OPT) $(CFLAGS) -fPIC -Wall -c -o pmgr_collective_client_slurm.o  pmgr_collective_client_slurm.c && \
	  gcc $(OPT) $(CFLAGS) -fPIC -Wall -c -o pmgr_collective_client_tree.o   pmgr_collective_client_tree.c && \
	  gcc $(OPT) $(CFLAGS) -fPIC -Wall -c -o pmgr_collective_client.o pmgr_collective_client.c && \
	  gcc $(OPT) $(CFLAGS) -fPIC -Wall -c -o pmgr_collective_mpirun.o pmgr_collective_mpirun.c && \
	  g++ $(OPT) $(CFLAGS) -fPIC -Wall -c -o pmi.o pmi.cpp && \
	  ar rcs libpmgr_collective.a $(PMGR_OBJS) && \
	  ar rcs libpmi.a pmi.o $(PMGR_OBJS) && \
	  gcc $(OPT) $(LDFLAGS) -fPIC -shared -Wl,-soname,libpmgr_collective.so.1 -o libpmgr_collective.so.1.0.1 \
		$(PMGR_OBJS) $(LIBS) && \
	  g++ $(OPT) $(LDFLAGS) -fPIC -shared -Wl,-soname,libpmi.so.1 -o libpmi.so.1.0.1 \
		pmi.o $(PMGR_OBJS) $(LIBS) && \
	  mv libpmgr_collective.* ../lib/. && \
	  mv libpmi* ../lib && \
	  ln -s libpmgr_collective.so.1.0.1 ../lib/libpmgr_collective.so.1 && \
	  ln -s libpmgr_collective.so.1     ../lib/libpmgr_collective.so && \
	  ln -s libpmi.so.1.0.1 ../lib/libpmi.so.1 && \
	  ln -s libpmi.so.1     ../lib/libpmi.so && \
	  cp *.h ../include
	cd test && \
	  gcc $(OPT) -o client     client.c     $(PMGR_INC) $(PMGR_LIB) && \
	  gcc $(OPT) -o mpirun_rsh mpirun_rsh.c $(PMGR_INC) $(PMGR_LIB)

install:
	mkdir -p $(PREFIX) && \
	  cp -a include $(PREFIX) && \
	  cp -a lib     $(PREFIX)

clean:
	rm -rf src/*.o
	rm -rf include
	rm -rf lib
	rm -rf test/*.o test/client test/mpirun_rsh
