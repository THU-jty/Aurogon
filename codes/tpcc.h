//
// Created by prince_de_marcia on 2021/6/30.
//

#ifndef CLOCK_MODEL_TPCC_H
#define CLOCK_MODEL_TPCC_H

#include "config.h"

class Tpcc
{
public:
    ll num_keys;
    ll num_wh;
    ll num_district;
    ll num_custumor;
    ll num_stock;
    ll num_item;
    ll num_order;
    ll num_thd_buf;
    ll magic_cus;
    ll magic_item;
    std::vector<ll> *thd_wh;

    void init();
    void generate_req( WorkerThread *wthd );

    ll local_wh( ll w_id );
    ll local_dst( ll w_id, ll d_id );
    ll local_ctr( ll w_id, ll d_id, ll c_id );
    ll local_stk( ll w_id, ll s_id );
    ll local_itm( ll item_id );
    ll local_odr( ll thd_id, ll txn_mgr_id, ll id );
    ll gen_key( ll n_id, ll k_id );
    ll which_key( ll key );
};

#endif //CLOCK_MODEL_TPCC_H
