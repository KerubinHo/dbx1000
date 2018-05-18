#!/bin/bash

cp silo_config.h config.h
cp ./system/silo_main.temp ./system/main.cpp
cp silo_make Makefile
make clean
make -j
cp hstore_config.h config.h
cp ./system/hstore_main.temp ./system/main.cpp
cp hstore_make Makefile
make clean
make -j
cp no_wait_config.h config.h
cp ./system/no_wait_main.temp ./system/main.cpp
cp no_wait_make Makefile
make clean
make -j
cp mvcc_config.h config.h
cp ./system/mvcc_main.temp ./system/main.cpp
cp mvcc_make Makefile
make clean
make -j
