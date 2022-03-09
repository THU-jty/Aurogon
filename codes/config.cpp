//
// Created by prince_de_marcia on 2021/3/25.
//

#include "config.h"
#include "table.h"
#include "tpcc.h"
#include "clock_sync.h"

ll my_node_id;
ll txn_per_worker = 1;
ll prepared_req_per_thread = 4096;
ll node_cnt = 4;
ll worker_per_server = 4;
ll client_per_server = worker_per_server;
ll ib_port = 1;
ll GID = 0;
ll rdma_buffer_size = 4096;
ll txn_size = 4;
ll max_cq_num = txn_per_worker*txn_size*32;
ll qp_size = 4000;
//ll qp_size = txn_per_worker*txn_size*4;
ll cq_size = 4000;
//ll cq_size = txn_per_worker*txn_size*node_cnt*4;
ll req_buffer_size = 16;
ll read_req_buffer_size = txn_size*worker_per_server\
*node_cnt*txn_per_worker*10;
ll read_req_pending_size = 500000;
ll recycle_entry_once = req_buffer_size/5;
#ifdef __DEBUG
ll total_issued_cnt_per_thd = 2000;
#else
//ll total_issued_cnt_per_thd = 500;
ll total_issued_cnt_per_thd = (1LL<<60);
#endif
ll warmup_time = 0*1000000000LL;// ns
ll running_time = 10*1000000000LL;// ns
ll poll_size = 32;
ll local_poll_num = 16;
ll remote_poll_num = 1;
ll ret_read_poll_num = 16;
ll repeat_poll_num = 1;
ll one_epoch_recv_wr = max_cq_num/2;
ll in_flight_recv_area = max_cq_num-one_epoch_recv_wr;
ll show_sync_time = 1;

ll mvcc_type = 2;
ll workload = 3;
// 3 TPC-C 4 YCSB
ll commit_phase = 1;
ll immortal_writes = 1;
ll stale_reads = 1;
ll retry_txns = 1;
ll retry_interval = 100;// 100us
ll reordering = 9;
ll insert_skew = 1;
ll use_pri_lock = 1;
ll rmw_delay = 20;
ll key_delay = 0;
ll hotpoint_sta_range = 500*1000000LL; // 500ms
ll hotpoint_num = 10000;
ll lower_delay = 0*1000LL; // 0us
ll batch_send = 0;
ll clock_type = 2;

ll key_range = 4;
ll key_partition_range = key_range/node_cnt;

/*YCSB workload args*/
ll read_req_ratio = 50;
ll write_req_ratio = 100-read_req_ratio;
ll local_req_ratio = -1;// -1 represents no control

/*Smallbank workload args*/
ll read_snapshot_ratio = 50;
ll rmw_ratio = 100-read_snapshot_ratio;
ll order_request = 1;
double zipfan = -1.0;
ll zip_group = 1;
ll ext_req_buffer_size = 5000;
ll ext_keys = 20;

/*TPCC workload args*/
ll wh_pre_server = 1;
ll item_range = 90000;
ll new_order_ratio = 100;
ll neworder_local_ratio = 99;
ll payment_local_ratio = 85;
ll wh_req_buffer = 500;
ll dst_req_buffer = 500;
ll dst_per_wh = 10;
ll nu_rand = 1;
ll scale_factor = 0;

ll base_time_offset = 0;
ll stop_running;
ll start_running;
ll record_flag = 0;

WorkerThread * worker_thds;
ClientThread * client_thds;
Table table;
Tpcc tpcc;
GlobalTimer gt;

std::string MemcacheAddr;
ll MemcachePort;
ll hp_connect_port = 12450;

ll bind_cores[40] = {0,1,2,3,4,5,6,7,8,9,
                     20,21,22,23,24,25,26,27,28,29,
                     10,11,12,13,14,15,16,17,18,19,
                     30,31,32,33,34,35,36,37,38,39};

ll get_timestamp(){
#ifdef RDTSC
    uint32_t lval, hval;
    asm volatile ("rdtsc" : "=a" (lval), "=d" (hval));
    ll tmp = hval;
    tmp = (tmp << 32) | lval;
    return tmp;
#else
    struct timespec current_tn;
    clock_gettime(CLOCK_REALTIME, &current_tn);
//    clock_gettime(CLOCK_MONOTONIC, &current_tn);
    ll ret = current_tn.tv_sec*1000000000LL+current_tn.tv_nsec;
    return ret;
#endif
}

ll try_lock( std::atomic<ll>* _lock ){
    ll n = 0;
    if(std::atomic_compare_exchange_weak( _lock, &n, 1LL )){
        return 1;
    }
    else return 0;
}
ll lock( int *lock ){
    int a = 0, b = 1;
    while(!__sync_bool_compare_and_swap(lock, a, b)){
    }
    return 0;

}
ll unlock( int *lock ){
    int a = 0, b = 1;
    while( !__sync_bool_compare_and_swap(lock, b, a) ){
    }
    return 0;

}

void die(const char *reason)
{
    fprintf(stderr, "%s\n", reason);
    exit(EXIT_FAILURE);
}

bool cmp( txn_item a, txn_item b ){
    return ( ( a.node_id == my_node_id ) \
    < ( b.node_id == my_node_id ) );
}