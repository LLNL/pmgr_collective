all: clean
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
	  ln -s libpmgr_collective.so.1     ../lib/libpmgr_collective.so

clean:
	rm -rf src/*.o
	rm -rf lib/*

lustre:
	rm -rf /p/lscratchc/moody20/scr/*
	cp test/test_synccopy test/test_asynccopy /p/lscratchc/moody20/scr/.
