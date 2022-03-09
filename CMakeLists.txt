cmake_minimum_required(VERSION 3.6.2)
project(Aurogon)

#SET(CMAKE_CXX_COMPILER "/usr/bin/g++")
#SET(CMAKE_BUILD_TYPE Release)
#set(CMAKE_CXX_STANDARD 11)
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR})
set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR})
set( CMAKE_C_FLAGS "-lmlx5 -ggdb -g -O2 -Wall -lpthread -libverbs -lmemcached" )
set( CMAKE_CXX_FLAGS "-lmlx5 -ggdb -g -O2 -Wall -lpthread -libverbs -lmemcached" )

add_compile_options(-std=c++11)

add_executable(
        aurogon codes/main.cpp
        codes/rdma.cpp codes/rdma.h
        codes/thread.cpp codes/thread.h
        codes/config.h codes/table.cpp
        codes/table.h codes/txn.cpp
        codes/txn.h codes/util.cpp
        codes/util.h codes/config.cpp
        codes/global.h
        codes/global.cpp
        codes/rdma.h
        codes/rdma_ts.cpp
        codes/svm.h
        codes/svm.cpp
        codes/clock_sync.h
        codes/clock_sync.cpp
        codes/tpcc.h codes/tpcc.cpp
        codes/ud_ts.cpp codes/ud_ts.h
        codes/rc_ts.cpp codes/rc_ts.h

)
add_executable(
        gt_center codes/GT_center.cpp
        codes/global.h
        codes/global.cpp
        codes/config.h
        codes/config.cpp
        codes/rdma.h
        codes/rdma_ts.cpp
        codes/svm.h
        codes/svm.cpp
        codes/clock_sync.h
        codes/clock_sync.cpp
        codes/util.cpp
        codes/util.h
        codes/ud_ts.cpp codes/ud_ts.h
        codes/rc_ts.cpp codes/rc_ts.h
)
#add_executable(
#        test_lock codes/test_lock.cpp
#        codes/rdma.cpp codes/rdma.h
#        codes/thread.cpp codes/thread.h
#        codes/config.h codes/table.cpp
#        codes/table.h codes/txn.cpp
#        codes/txn.h codes/util.cpp
#        codes/util.h codes/config.cpp
#        codes/global.h
#        codes/global.cpp
#        codes/rdma.h
#        codes/rdma_ts.cppc
#        codes/svm.h
#        codes/svm.cpp
#        codes/clock_sync.h
#        codes/clock_sync.cpp
#        codes/tpcc.h codes/tpcc.cpp
#)

include_directories("codes")
