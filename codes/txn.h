//
// Created by prince_de_marcia on 2021/3/25.
//

#ifndef CLOCK_MODEL_TXN_H
#define CLOCK_MODEL_TXN_H

#include "config.h"
#include "table.h"


class Txn_manager{
public:
    ll write_flag[40];
    ll rmw_flag[40];
    ll status;
    ll txn_mg_id;
    ll txn_id;
    ll timestamp;
    ll start_timestamp;
    ll issued_timestamp;
    ll end_timestamp;

    ll first_complt_rd;
    ll first_complt_wt;
    ll first_complt_rmw;
    ll last_complt_rd;
    ll last_complt_wt;
    ll last_complt_rmw;

    //calculate for completed reqs
    ll rd_complt_cnt;
    ll wt_complt_cnt;
    ll rmw_complt_cnt;
//    ll qcnt;
//    ll ccnt;
    Request* req;

//    QueryMessage qbuf[10];
//    AckMessage ackbuf[10];
//    CommitMessage cbuf[10];

    //buffer req results, vectors for future
    ll rd_buffer;
    ll wt_buffer;

    ll sts[10][2];
    void start( ll id );
    void end( ll id );
    ll ret_res( ll id );

    void output();
};

class Txn{
public:
    ll node_id;
    ll worker_thd_id;
    ll client_thd_id;
    ll txn_table_cnt;
    Txn_manager* txn_table;
    Queue *txn_free_list;
    void init(ll _txn_table_cnt);
    ll get( ll txn_id );
    ll recycle( ll _txn_mg_id );
};

#endif //CLOCK_MODEL_TXN_H
