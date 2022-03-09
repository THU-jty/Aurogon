//
// Created by prince_de_marcia on 2021/3/25.
//

#ifndef CLOCK_MODEL_THREAD_H
#define CLOCK_MODEL_THREAD_H

#include "config.h"
#include "table.h"
#include "tpcc.h"

class ClientGen{
public:
    ll thd_id;
    ll req_cnt;
    ll req_issued_cnt;
    ll req_issued_tot;
    ll last_generate_id;

    std::queue< std::pair<ll,ll> > *retry_queue;
    std::vector<ll> * lat;
    std::vector<ll> * abort_cnt;
    Request* req_table;
    Request** req_tm_table;
    void init( ll _thd_id );
    ll generate_task( ll txn_manager_id );
    ll generate_key();
    Request *get_req( ll txn_manager_id );
    void recycle( Request *req );
};


class MyThread{
public:
    ll thd_id;
    ll node_id;
    ull run_starttime;
    ll ready_running;

    MyThread(){
        ready_running = 0;
    }

    void setup( ll _node_id, ll _thd_id );
    virtual void run();
};

class Read_req_item{
public:
    ll key;
    ll version_id;
    ll timestamp;
    ll txn_mg_id;
    ll txn_id;
    ll sz;
    ll req_id;
    ll s_id;
    ll start_ts;
//    ll thd_id;
    void init( ll _key, ll _v_id, ll _ts, ll _mg_id, ll _txn_id, ll _sz, ll _req_id, ll _s_id, ll _start_ts ){
        key = _key;
        version_id = _v_id;
        timestamp = _ts;
        txn_mg_id = _mg_id;
        txn_id = _txn_id;
        sz = _sz;
        req_id = _req_id;
        s_id = _s_id;
        start_ts = _start_ts;
    }
};

class WorkerThread : public MyThread{
public:
    ll client_thd_id;
    Txn *txn;
    RDMA *rdma;

    ll *send_cnt;
    ll *send_finished;
    ll *recv_cnt;
    ll *recv_finished;

    Queue *request_callback_queue;
    Queue *request_issue_queue;
    Queue *request_retry_queue;
    Request* req_table;

    Queue *shared_upd_read;
    ClientGen client;

    Queue ins_read;
    Queue free_ins_read;
    Read_req_item *read_req_q_pool;

    /*local msg buffer*/
    Queue *free_msg_q;
    Queue* pending_msg_q;
    char *local_msg;
    MessageControl mctrl;

    /*local table buffer*/
    QueryMessage *read;
    Linklist read_lst;
    Queue read_q;

    ll txn_cnt;
    ll timestamp_offset;
    ll ts_cnt;

    ll txn_finished;
    ll txn_aborted;
    ll txn_executed;
    ll req_cnt;
    ll req_aborted;
    ll lat_txn_finished;
    ll issued_req_cnt;
    ll recv_ack_cnt;
    ll recv_qry_cnt;
    ll lat_get_ts;
    ll rd_txn_finished;
    ll rmw_txn_finished;
    ll rd_intvl;
    ll wt_intvl;
    ll rmw_intvl;
    ll ctrl_cnt;
    ll ctrl_intvl;
    ll c2_cnt;
    ll c2_intvl;
    ll local_req;
    ll remote_req;

    ll key[10];

    void run();
    int exec_phase2( ll source_id, char* msg_buffer );

    Txn_manager *start_txn();
    void commit_txn( Txn_manager *mag );
    void abort_txn( Txn_manager *mag );
    void send_cmt_msg( Txn_manager *mag, bool s );
    void switch_txn_status( Txn_manager *mag );
    void return_read( ll key );
    void send_read_ack( QueryMessage *qmsg );
    void per_send_cmt_msg( Txn_manager *mag, bool s, Request* req, txn_item *item, ll o_ts);

    void deal_with_recv( ll id );

    void output();

    ll get_thread_timestamp();
};

class ClientThread : public MyThread{
public:
    ll worker_thd_id;
    ll req_cnt;
    ll req_issued_cnt;
    ll req_issued_tot;

    ll last_generate_id;

    Request* req_table;
    Request** req_tm_table;
    Queue *request_callback_queue;
    Queue *request_issue_queue;
    Queue *request_retry_queue;

    ll reqs[100];

    ll rd_txn_finished;
    ll wt_txn_finished;
    ll rmw_txn_finished;
    std::vector<ll> lat;
    std::vector<ll> abort_cnt;

//    void run();
//    ll generate_task();
//    ll generate_key();
};


class ConnectThread : public MyThread{
    void run();
};


#endif //CLOCK_MODEL_THREAD_H
