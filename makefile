OPT ?= -g -O0
#OPT ?= -g -O1
#OPT ?= -g -O2
#OPT ?= -g -O3
PREFIX ?= /usr/local/tools/pmgr_collective

all: clean
	mkdir lib
	mkdir include
	cd src && \
	  gcc $(OPT) -fPIC -Wall -c -o pmgr_collective_common.o pmgr_collective_common.c && \
	  gcc $(OPT) -fPIC -Wall -c -o pmgr_collective_ranges.o pmgr_collective_ranges.c && \
	  gcc $(OPT) -fPIC -Wall -c -o pmgr_collective_client_common.o pmgr_collective_client_common.c && \
	  gcc $(OPT) -fPIC -Wall -c -o pmgr_collective_client_mpirun.o pmgr_collective_client_mpirun.c && \
	  gcc $(OPT) -fPIC -Wall -c -o pmgr_collective_client_slurm.o  pmgr_collective_client_slurm.c && \
	  gcc $(OPT) -fPIC -Wall -c -o pmgr_collective_client_tree.o   pmgr_collective_client_tree.c -DHAVE_PMI -I/usr/include/slurm && \
	  gcc $(OPT) -fPIC -Wall -c -o pmgr_collective_client.o pmgr_collective_client.c -DHAVE_PMI -I/usr/include/slurm && \
	  gcc $(OPT) -fPIC -Wall -c -o pmgr_collective_mpirun.o pmgr_collective_mpirun.c && \
	  ar rcs libpmgr_collective.a \
		pmgr_collective_common.o \
		pmgr_collective_ranges.o \
		pmgr_collective_client_common.o \
		pmgr_collective_client_mpirun.o \
		pmgr_collective_client_tree.o \
		pmgr_collective_client_slurm.o \
		pmgr_collective_client.o \
		pmgr_collective_mpirun.o && \
	  gcc $(OPT) -fPIC -shared -Wl,-soname,libpmgr_collective.so.1 -o libpmgr_collective.so.1.0.1 \
		pmgr_collective_common.o \
		pmgr_collective_ranges.o \
		pmgr_collective_client_common.o \
		pmgr_collective_client_mpirun.o \
		pmgr_collective_client_tree.o \
		pmgr_collective_client_slurm.o \
		pmgr_collective_client.o \
		pmgr_collective_mpirun.o \
		-lpmi && \
	  mv libpmgr_collective.a        ../lib/. && \
	  mv libpmgr_collective.so.1.0.1 ../lib/. && \
	  ln -s libpmgr_collective.so.1.0.1 ../lib/libpmgr_collective.so.1 && \
	  ln -s libpmgr_collective.so.1     ../lib/libpmgr_collective.so && \
	  cp *.h ../include
	cd test && \
	  gcc $(OPT) -o client     client.c     -I../include -L../lib -lpmgr_collective && \
	  gcc $(OPT) -o mpirun_rsh mpirun_rsh.c -I../include -L../lib -lpmgr_collective

install:
	mkdir -p $(PREFIX) && \
	  cp -a include $(PREFIX) && \
	  cp -a lib     $(PREFIX)

clean:
	rm -rf src/*.o
	rm -rf include
	rm -rf lib
	rm -rf test/*.o test/client test/mpirun_rsh
