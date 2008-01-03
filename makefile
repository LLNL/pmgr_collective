all: clean
	mkdir lib
	mkdir include
	cd src && \
	  gcc -g -O0 -fPIC -Wall -c -I../include -o pmgr_collective_common.o pmgr_collective_common.c && \
	  gcc -g -O0 -fPIC -Wall -c -I../include -o pmgr_collective_client.o pmgr_collective_client.c && \
	  gcc -g -O0 -fPIC -Wall -c -I../include -o pmgr_collective_mpirun.o pmgr_collective_mpirun.c && \
	  ar rcs libpmgr_collective.a pmgr_collective_common.o pmgr_collective_client.o pmgr_collective_mpirun.o && \
	  gcc -g -O0 -fPIC -shared -Wl,-soname,libpmgr_collective.so.1 -o libpmgr_collective.so.1.0.1 \
		pmgr_collective_common.o pmgr_collective_client.o pmgr_collective_mpirun.o && \
	  mv libpmgr_collective.a ../lib/. && \
	  mv libpmgr_collective.so.1.0.1 ../lib/. && \
	  ln -s libpmgr_collective.so.1.0.1 ../lib/libpmgr_collective.so.1 && \
	  ln -s libpmgr_collective.so.1     ../lib/libpmgr_collective.so && \
	  cp *.h ../include
	cd test && \
	  gcc -g -O0 -o client     client.c     -I../include -L../lib -lpmgr_collective && \
	  gcc -g -O0 -o mpirun_rsh mpirun_rsh.c -I../include -L../lib -lpmgr_collective


clean:
	rm -rf src/*.o
	rm -rf lib
	rm -rf include
	rm -rf test/*.o test/client

lustre:
	rm -rf /p/lscratchc/moody20/scr/*
	cp test/test_synccopy test/test_asynccopy /p/lscratchc/moody20/scr/.
