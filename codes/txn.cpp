//
// Created by prince_de_marcia on 2021/3/25.
//

#include "txn.h"
#include "util.h"

void Txn::init(ll _txn_table_cnt){
    txn_table_cnt = _txn_table_cnt;
    txn_table = (Txn_manager *)malloc( sizeof(Txn_manager)*txn_table_cnt );
    txn_free_list = (Queue *)malloc(sizeof(Queue));
    txn_free_list->init(txn_table_cnt);

    for( ll i = 0; i < txn_table_cnt; i ++ ){
        txn_table[i].status = 0;
        txn_table[i].txn_mg_id = i;
        txn_free_list->push(i);
    }
}

ll Txn::get( ll txn_id ){
    ll tmp = txn_free_list->get();
    txn_table[tmp].status = 1;
    txn_table[tmp].rd_complt_cnt = 0;
    txn_table[tmp].wt_complt_cnt = 0;
    txn_table[tmp].rmw_complt_cnt = 0;
    txn_table[tmp].first_complt_rd = -1;
    txn_table[tmp].first_complt_wt = -1;
    txn_table[tmp].first_complt_rmw = -1;
//    txn_table[tmp].qcnt = 0;
//    txn_table[tmp].ccnt = 0;
    txn_table[tmp].txn_id = txn_id;
    
    return tmp;
}

ll Txn::recycle(ll _txn_mg_id) {
    //printf("recycle T%lld mg %lld\n", worker_thd_id, _txn_mg_id);
    txn_table[_txn_mg_id].txn_id = -1;
    txn_table[_txn_mg_id].status = 0;
    txn_free_list->push( _txn_mg_id );
    return 0;
}

void Txn_manager::start(ll id) {
    sts[id][0] = get_timestamp();
}

void Txn_manager::end(ll id) {
    sts[id][1] = get_timestamp();
}

ll Txn_manager::ret_res(ll id) {
    if( sts[id][1]-sts[id][0] < 0 ){
        fprintf(stdout, "%lld id %lld statistics %lld error\n",
                txn_mg_id, txn_id, id);
    }
    return sts[id][1]-sts[id][0];
}

void Txn_manager::output() {
    printf("txn_mg_id %lld txn_id %lld status %lld rd %lld wt %lld\n",
            txn_mg_id, txn_id, status, rd_complt_cnt, wt_complt_cnt);
}