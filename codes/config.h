//
// Created by prince_de_marcia on 2021/3/25.
//

#ifndef CLOCK_MODEL_CONFIG_H
#define CLOCK_MODEL_CONFIG_H

#include<cstdlib>
#include<cstring>
#include<cstdio>
#include<string>
#include<algorithm>
#include<vector>
#include<queue>
#include<assert.h>
#include<sched.h>
#include<pthread.h>
#include <sstream>
#include <iostream>
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <libmemcached/memcached.h>
//#include <gperftools/profiler.h>
#include <atomic>
#include <unistd.h>
#include <fstream>
#include <csignal>

//#define __DEBUG

//#define __NO_TS
//#define RDTSC
#define REQ_REORDER
#define __PRILOCK
//#define __gperf

#ifdef __DEBUG
#define DEBUG(info,...)  \
fprintf(stdout, info, ##__VA_ARGS__)
#else
#define DEBUG(info,...)
#endif

#define barrier() __asm__ __volatile__("": : :"memory")
#define VALUE_SIZE 15

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)

#define INC(name) do { if( record_flag ){name ++;} } while(0)

typedef long long ll;
typedef unsigned long long ull;

class RDMA;
class Txn_manager;
class Txn;
class MyThread;
class WorkerThread;
class ClientThread;
class ClientGen;
class ConnectThread;
class txn_item;
class row;
class Table;
class Message;
class QueryMessage;
class AckMessage;
class Request;
class Queue;
class Linklist;
class Statistics;
class GlobalTimer;
class Tpcc;
class Zipfan_gen;
class Pri_lock;
class Read_req_item;
class Hotpoint;

enum RC { OK=0, FAIL=1 };
enum RW { READ=0, WRITE=1, PWRITE=2, RMW=3};
enum MT {QUERY=0, ACK=1, CMT=2};

extern ll my_node_id;
extern ll txn_per_worker;
extern ll prepared_req_per_thread;
extern ll node_cnt;
extern ll worker_per_server;
extern ll client_per_server;
extern ll ib_port;
extern ll GID;
extern ll qp_size;
extern ll cq_size;
extern ll max_cq_num;
extern ll rdma_buffer_size;
extern ll txn_size;
extern ll read_req_ratio;
extern ll write_req_ratio_per_txn;
extern ll local_req_ratio;
extern ll key_range;
extern ll key_partition_range;
extern ll req_buffer_size;
extern ll read_req_buffer_size;
extern ll read_req_pending_size;
extern ll recycle_entry_once;
extern ll total_issued_cnt_per_thd;
extern ll warmup_time;
extern ll running_time;
extern ll poll_size;
extern ll local_poll_num;
extern ll remote_poll_num;
extern ll ret_read_poll_num;
extern ll repeat_poll_num;
extern ll in_flight_recv_area;
extern ll one_epoch_recv_wr;
extern ll show_sync_time;
extern ll clock_type;

extern std::string MemcacheAddr;
extern ll MemcachePort;
extern ll hp_connect_port;
extern ll stop_running;
extern ll start_running;
extern ll base_time_offset;
extern ll record_flag;

extern ll mvcc_type;
extern ll workload;
extern ll commit_phase;
extern ll immortal_writes;
extern ll stale_reads;
extern ll retry_txns;
extern ll retry_interval;
extern ll reordering;
extern ll insert_skew;
extern ll use_pri_lock;
extern ll rmw_delay;
extern ll key_delay;
extern ll hotpoint_sta_range;
extern ll hotpoint_num;
extern ll lower_delay;
extern ll batch_send;

extern ll read_snapshot_ratio;
extern ll rmw_ratio;
extern ll order_request;
extern double zipfan;
extern ll zip_group;
extern ll ext_req_buffer_size;
extern ll ext_keys;

extern ll wh_pre_server;
extern ll item_range;
extern ll new_order_ratio;
extern ll neworder_local_ratio;
extern ll payment_local_ratio;
extern ll wh_req_buffer;
extern ll dst_req_buffer;
extern ll dst_per_wh;
extern ll nu_rand;
extern ll scale_factor;

extern WorkerThread * worker_thds;
extern ClientThread * client_thds;
extern Table table;
extern GlobalTimer gt;
extern Tpcc tpcc;
extern ll bind_cores[40];

ll get_timestamp();
ll try_lock( std::atomic<ll>* _lock );
ll lock( int* _lock );
ll unlock( int* _lock );
void die(const char *reason);
bool cmp( txn_item a, txn_item b );

#endif //CLOCK_MODEL_CONFIG_H
