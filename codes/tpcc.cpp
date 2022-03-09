//
// Created by prince_de_marcia on 2021/6/30.
//

#include <time.h>
#include "tpcc.h"
#include "thread.h"

ll get_rand( ll ex, ll st, ll cnt );
ll NUrand( ll A, ll C, ll x, ll y );
//ll dst_per_wh = 10;

void Tpcc::init(){
    num_thd_buf = 5;

    num_wh = wh_pre_server;
    if( scale_factor != 0 ){
        num_wh = scale_factor*worker_per_server;
    }
    num_district = dst_per_wh*num_wh;
    num_custumor = 3000*dst_per_wh*num_wh;
    num_stock = num_wh*item_range;
    num_item = item_range;
    num_order = txn_per_worker*worker_per_server*num_thd_buf;
//    assert( num_wh%worker_per_server == 0 \
//    || worker_per_server%num_wh == 0 );
    num_keys = num_wh+num_district+num_custumor+num_stock+num_item+num_order;
    thd_wh = new std::vector<ll>[worker_per_server];
    magic_cus = 555; // [0,1024]
    magic_item = 6666; // [0,8192]

    if( num_wh >= worker_per_server ){
        for( ll i = 0; i < num_wh; i ++ ){
            thd_wh[ i%worker_per_server ].push_back(i);
        }
    }else{
        for( ll i = 0; i < worker_per_server; i ++ ){
            thd_wh[i].push_back( i%num_wh );
        }
    }
    assert( txn_size >= 16 );
    printf("Total warehouses this server %lld\n", num_wh);
}

void Tpcc::generate_req(WorkerThread *wthd) {
    Request **req_table = wthd->client.req_tm_table;
    ll thd_id = wthd->thd_id;

    for( ll k = 0; k < 1; k ++ ) {
        for (ll i = 0; i < prepared_req_per_thread; i++) {
            req_table[k][i].req_order = i;
            req_table[k][i].req_txnmger_id = k;
            Request *req = &req_table[k][i];
            ll type = rand() % 100;
            if (type < new_order_ratio) {// neworder transactions
                req->req_type = 1;
                ll req_cnt = 0;
                ll size;
//            size = rand()%10+5;
                size = 10;
                ll w_id, d_id, c_id;

                // w_tax = warehouse_table[OL_SUPPLY_W_ID].W_TAX
//                if (worker_per_server % num_wh == 0) {
//                    w_id = thd_id % num_wh;
//                } else if (num_wh % worker_per_server == 0) {
//                    w_id = (rand() * worker_per_server + thd_id) % num_wh;
//                } else {
//
//                }
                assert( thd_wh[thd_id].size() != 0 );
                w_id = thd_wh[thd_id][rand()%thd_wh[thd_id].size()];

                req->readreq.push_back(txn_item((ll) (my_node_id), \
            gen_key(my_node_id, local_wh(w_id)), rand(),\
            req_cnt++, 1));

                // d_tax = district_table[D_ID, W_ID].D_TAX
                // order_id = fetch_and_add(district_table[D_ID, W_ID].D_NEXT_O_ID, 1)
                d_id = rand() % dst_per_wh;
//            d_id = thd_id%dst_per_wh;
                req->rmwreq.push_back(txn_item((ll) (my_node_id), \
            gen_key(my_node_id, local_dst(w_id, d_id)), rand(),\
            req_cnt++, 1));

                // read customer table
                if( nu_rand )
                    c_id = NUrand( 1023, magic_cus, 0, 3000 );
                else c_id = rand() % 3000;
                req->readreq.push_back(txn_item((ll) (my_node_id), \
            gen_key( my_node_id, local_ctr(w_id, d_id, c_id)), rand(),\
            req_cnt++, 4));

                // write new-order table & order table
                //insert order-line table
                for( ll j = 0; j < 1; j ++ )
                    req->writereq.push_back(txn_item((ll) (my_node_id), \
            gen_key(my_node_id, local_odr(thd_id, k, j)), rand(),\
            req_cnt++, 4));

                ll tmp[20];
                for (ll j = 0; j < size; j++) {
                    ll x = rand() % 100;
                    ll local = 0;
                    if (x < neworder_local_ratio) {
                        local = 1;
                    }
                    ll remote_id = my_node_id;
                    ll tmp_w_id = w_id;
                    ll item_id;
                    while(1){
                        if( nu_rand )
                            item_id = NUrand( 8191, magic_item, 0, item_range );
                        else item_id = rand()%item_range;
                        int flag = 0;
                        for( ll t = 0; t < j; t ++ ){
                            if( tmp[t] == item_id ){
                                flag = 1;
                                break;
                            }
                        }
                        if( flag == 0 ) break;
                    }
                    tmp[j] = item_id;
                    if (local == 0) {
                        //select other node warehouse randomly
                        tmp_w_id = rand() % num_wh;
                        remote_id = get_rand(my_node_id, 0, node_cnt);
                        assert( remote_id != my_node_id );
                        //select other node warehouse only from local thread
//                    tmp_w_id = w_id;
//                    remote_id = get_rand( my_node_id, 0, node_cnt );
//                    //select other warehouse randomly
                    }
                    //check the stock
                    req->rmwreq.push_back(txn_item((ll) (remote_id), \
            gen_key( remote_id, local_stk(tmp_w_id, item_id)), rand(),\
            req_cnt++, 8));
//                    req->rmwreq.push_back(txn_item((ll) (my_node_id), \
//            gen_key( my_node_id, local_itm(item_id)), rand(), req_cnt++, 1));

                }
            } else {// payment transactions
                req->req_type = 2;
                ll req_cnt = 0;
                ll w_id, d_id;


//                if (worker_per_server % num_wh == 0) {
//                    w_id = thd_id % num_wh;
//                } else if (num_wh % worker_per_server == 0) {
//                    w_id = (rand() * worker_per_server + thd_id) % num_wh;
//                } else {
//
//                }
                w_id = thd_wh[thd_id][rand()%thd_wh[thd_id].size()];

                // Warehouse_table[W_ID].W_YTD += H_AMOUNT
                req->rmwreq.push_back(txn_item((ll) (my_node_id), \
            gen_key( my_node_id, local_wh(w_id)), rand(),\
            req_cnt++, 1));

                d_id = rand()%dst_per_wh;
                //District_table[W_ID, D_ID].D_YTD += H_AMOUNT
                req->rmwreq.push_back(txn_item((ll) (my_node_id), \
            gen_key( my_node_id, local_dst(w_id, d_id)), rand(),\
            req_cnt++, 1));

                ll x = rand() % 100;
                ll local = 0;
                if (x < payment_local_ratio) {
                    local = 1;
                }
                ll c_w_id = w_id, c_d_id = d_id, remote_id = my_node_id;
                ll c_id;
                if( nu_rand )
                    c_id = NUrand( 1023, magic_cus, 0, 3000 );
                else c_id = rand()%3000;
                if( local == 0 ){
                    remote_id = get_rand(my_node_id, 0, node_cnt);
                    c_w_id = rand()%num_wh;
                }
                req->rmwreq.push_back(txn_item((ll) (remote_id), \
            gen_key( remote_id, local_ctr(c_w_id, c_d_id, c_id)), rand(),\
            req_cnt++, 4));

                // insert into history table
                req->writereq.push_back(txn_item((ll) (my_node_id), \
            gen_key(my_node_id, local_odr(thd_id, k, 0)), rand(),\
            req_cnt++, 4));
            }
            req->tag();
        }
    }
}

ll Tpcc::local_wh( ll w_id ){
    assert( w_id >= 0 && w_id < num_wh );
    return w_id;
}

ll Tpcc::local_dst( ll w_id, ll d_id ){
    assert( w_id >= 0 && w_id < num_wh );
    assert( d_id >= 0 && d_id < dst_per_wh );
    return num_wh+w_id*dst_per_wh+d_id;
}

ll Tpcc::local_ctr( ll w_id, ll d_id, ll c_id ){
    assert( w_id >= 0 && w_id < num_wh );
    assert( d_id >= 0 && d_id < dst_per_wh );
    assert( c_id >= 0 && c_id < 3000 );
    return num_wh + num_district + (w_id*dst_per_wh+d_id)*3000 + c_id;
}

ll Tpcc::local_stk( ll w_id, ll s_id ){
    assert( w_id >= 0 && w_id < num_wh );
    assert( s_id >= 0 && s_id < item_range );
    return num_wh + num_district + num_custumor + w_id*item_range + s_id;

}
ll Tpcc::local_itm( ll item_id ){
    assert( item_id >= 0 && item_id < num_item );
    return num_wh + num_district + num_custumor + num_stock + item_id;
}

ll Tpcc::local_odr( ll thd_id, ll txn_mgr_id, ll id ){
    assert( thd_id >= 0 && thd_id < worker_per_server );
    assert( txn_mgr_id >= 0 && txn_mgr_id < txn_per_worker );
    return num_wh + num_district + num_custumor + num_stock + num_item + \
    thd_id*txn_per_worker*num_thd_buf + txn_mgr_id*num_thd_buf + id;
}

ll Tpcc::gen_key(ll n_id, ll k_id) {
    return n_id*num_keys + k_id;
}

ll Tpcc::which_key( ll key ){
    ll now_key = key-my_node_id*num_keys;
    assert( now_key >= 0 && now_key < num_keys );
    if( now_key < num_wh ) return 0;
    if( now_key < num_wh+num_district ) return 1;
    if( now_key < num_wh+num_district+num_custumor ) return 2;
    if( now_key < num_wh+num_district+num_custumor\
    +num_stock ) return 3;
    if( now_key < num_wh+num_district+num_custumor\
    +num_stock+num_item ) return 4;
    if( now_key < num_wh+num_district+num_custumor\
    +num_stock+num_item+num_order ) return 5;
}

ll get_rand( ll ex, ll st, ll cnt){
    ll x;
    while(1){
        x = rand()%cnt+st;
        if( x != ex ) break;
    }
    return x;
}

ll NUrand( ll A, ll C, ll x, ll y )
{
    return ( ( rand()%(A+1) | (rand()%(y-x)+x) ) + C ) % (y-x) + x ;
}