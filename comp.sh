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
cp wait_die_config.h config.h
cp ./system/wait_die_main.temp ./system/main.cpp
cp wait_die_make Makefile
make clean
make -j
cp dl_detect_config.h config.h
cp ./system/dl_detect_main.temp ./system/main.cpp
cp dl_detect_make Makefile
make clean
make -j
cp hekaton_config.h config.h
cp ./system/hekaton_main.temp ./system/main.cpp
cp hekaton_make Makefile
make clean
make -j
cp vll_config.h config.h
cp ./system/vll_main.temp ./system/main.cpp
cp vll_make Makefile
make clean
make -j
cp tictoc_config.h config.h
cp ./system/tictoc_main.temp ./system/main.cpp
cp tictoc_make Makefile
make clean
make -j
cp occ_config.h config.h
cp ./system/occ_main.temp ./system/main.cpp
cp occ_make Makefile
make clean
make -j
