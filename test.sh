#!/bin/bash
unzip $1.zip
cd $1
cd codebase
cd ix
make clean
make
./ixtest1
./ixtest2
./ixtest3
./ixtest4a
./ixtest4b
./ixtest4c
./ixtest5
./ixtest6
./ixtest7
./ixtest8
./ixtest_extra_1
./ixtest_extra_2a
./ixtest_extra_2b
./ixtest_extra_2c
./ixtest_extra_2d
