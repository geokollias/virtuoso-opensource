./autogen.sh
./configure --with-debug 
make clean 
make -j 22 >qq 2>qq
cd binsrc/tests/suite
./test_server.sh -sc
