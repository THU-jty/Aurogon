CC := g++
AR := ar
CFLAGS=-Wall -g -std=c++11
LIBS := -libverbs -lpthread -lmlx5 -lmemcached

SRC = $(wildcard *.cpp)
OBJ = $(patsubst %.cpp,%.o,${SRC})


## clock synchronization static lib
#sync_lib := libtsync.a
#
## two nodes ping-pang test
#synctest := sync_client sync_server
#
#pptest := pp_client pp_server
#gtest := gt_center gt_master
#bw_load := bw_load_server bw_load_client
#
#APPS := $(sync_lib) $(pptest) $(synctest) $(gtest) $(bw_load)

test := clock_model GT_center

APPS := $(test)
SOURCE =  table.cpp config.cpp thread.cpp util.cpp txn.cpp rdma.cpp global.cpp rdma_ts.cpp rdma_l.cpp sock.cpp svm.cpp clock_sync.cpp

all: $(APPS)

clock_model: main.cpp $(SOURCE)
	$(CC) $^ -o $@ $(LIBS) $(CFLAGS)

GT_center: GT_center.cpp $(SOURCE)
	$(CC) $^ -o $@ $(LIBS) $(CFLAGS)

.PHONY: clean
clean:
	rm -rf $(APPS) *.o *.a