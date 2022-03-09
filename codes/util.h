//
// Created by prince_de_marcia on 2021/3/25.
//

#ifndef CLOCK_MODEL_UTIL_H
#define CLOCK_MODEL_UTIL_H

#include "config.h"

class Queue{
public:
    ll front;
    ll tail;
    ll cnt;
    int lock_flag;
    ll* qe;
    void init( ll _cnt );
    ll get();
    ll try_get();
    void push( ll id );
    ll lock_try_get();
    void lock_push(ll id);
};

class Linklist{
public:
    ll head;
    ll tail;
    ll cnt;
    ll sum;
    ll *next;
    ll *prev;
    ll padding[2];
    void init( ll _cnt );
    void init2( Linklist *lst );
    void insert( ll v, ll p );
    void delt( ll v );
    void output();
    void check();
};

class Statistics{
public:
    ll txn_finished;
    ll txn_aborted;
    ll txn_executed;
    ll req_aborted;
    ll lat_txn_finished;
    ll lat_get_ts;
    ll lat_txn_total;
    ll rd_intvl;
    ll wt_intvl;
    ll rmw_intvl;
    ll rd_txn_finished;
    ll rmw_txn_finished;
    std::vector<ll> lat_cdf;
    double avg_lat_txn_finished;
    double avg_lat_get_ts;
    double testing_time;
    void init();
};

class Zipfan_gen{
public:
    double factor;
    double zipf_v;
    ll keys;
    ll *mappings;
    void init( ll _keys );
    ll gen();
};

class Hotpoint{
public:
    ll ts[2];
    ll cnt[2];
    ll flag;
    ll sta;
    ll step;
    ll itvl;
    ll loc;
    ll delay_int;
    ll delay_cnt;
    ll xx[6];
    void init( ll _loc );
    bool check_hot();
    void update( ll ts );
};

class Pri_lock{
public:
    ll thd_num;
    int issued_token;
    int exec_token;
    ll priority[10];

    void init( ll _thd_num );
//    bool lock( ll thd_id, ll pri );
//    void unlock( ll thd_id );
    bool lock( ll thd_id, ll pri ){
        priority[thd_id] = pri;
        barrier();
        while( !__sync_bool_compare_and_swap( &exec_token, -1, thd_id ) ){
            if( exec_token == thd_id ) break;
        }
    }
    void unlock( ll thd_id ){
        priority[thd_id] = 0;
        ll min_ts = (1LL<<62), min_id = -1;
        for( ll i = 0; i < thd_num; i ++ ){
            if( priority[i] == 0 ) continue;
            barrier();
            if( priority[i] < min_ts ){
                min_ts = priority[i];
                min_id = i;
            }
        }
        exec_token = min_id;
    }
};

void parser(int argc, char * argv[]);
void read_config();
void bind_core(ll id);

#endif //CLOCK_MODEL_UTIL_H
