//
// Created by prince_de_marcia on 2021/3/25.
//

#include <time.h>
#include "table.h"
#include "txn.h"
#include "util.h"
#include "thread.h"
#include "rdma.h"

ll check( Linklist *lst, Queue *q );
ll gen_aux_ts( ll s_id, ll thd_id );
ll compare_ts( ll ts_1, ll aux_ts_1, ll ts_2, ll aux_ts_2 );

ll gen_txn_id( ll i, ll j, ll k ){
    return i*worker_per_server*txn_per_worker + j*txn_per_worker + k;
}

void Table::init(ll _row_cnt){
    row_cnt = _row_cnt;
//    rows = (row*)malloc( sizeof(row)*row_cnt );
    assert( sizeof(row)%64 == 0 );
    void *tmp_ptr;
    assert( posix_memalign( &tmp_ptr, 64, sizeof(row)*row_cnt ) == 0 );
    rows = (row*)tmp_ptr;
//    rows = (row*)malloc( sizeof(row)*row_cnt );
//    rows = new row[row_cnt];
//    srand((unsigned)time(NULL));
    for( ll i = 0; i < row_cnt; i ++ ){
        rows[i].lock = 0;
        rows[i].prilock.init( worker_per_server );
        rows[i].key = i+row_cnt*my_node_id;
//        rows[i].rd_lst = (Linklist *)malloc(sizeof(Linklist)*worker_per_server);

        void *tmp_ptr;
        assert( sizeof(Linklist)%64 == 0 );
        assert( posix_memalign( &tmp_ptr, 64, sizeof(Linklist)*worker_per_server ) == 0 );
        rows[i].rd_lst = (Linklist *)tmp_ptr;
//        for( ll j = 0; j < worker_per_server; j ++ )
//            rows[i].rd_lst[j].init(read_req_buffer_size);

        rows[i].v[0].wts = -1;
        rows[i].v[0].rts = -1;
        rows[i].v[0].value = rand();
        rows[i].update_cnt = 0;

        rows[i].reqs[0] = rows[i].aborts[0] = 0;

//        rows[i].read_q = (Queue*)malloc(sizeof(Queue)*worker_per_server);
//        for( ll j = 0; j < worker_per_server; j ++ ) {
//            rows[i].read_q[j].init(read_req_buffer_size);
//            for( ll k = 0; k < read_req_buffer_size; k ++ ){
//                rows[i].read_q[j].push(k);
//            }
//        }

        ll buf_size = get_buf_size(rows[i].key);

        rows[i].wt_lst.init(buf_size);
        rows[i].write_q.init(buf_size);
        for( ll j = 0; j < buf_size; j ++ ){
            rows[i].write_q.push(j);
        }
//        rows[i].write = (version *)malloc(buf_size*sizeof(version));

        assert( sizeof(version)%64 == 0 );
        assert( posix_memalign( &tmp_ptr, 64, buf_size*sizeof(version) ) == 0 );
        rows[i].write = (version *)tmp_ptr;
//        rows[i].read = (QueryMessage **)malloc(sizeof(QueryMessage*)*worker_per_server);
//        for( ll j = 0; j < worker_per_server; j ++ ) {
//            rows[i].read[j] = (QueryMessage *) malloc(read_req_buffer_size * sizeof(QueryMessage));
//        }

//        for( ll j = 0; j < 5; j ++ ) {
//            rows[i].aborts[j] = 0;
//            rows[i].reqs[j] = 0;
//        }
        rows[i].req_cnt = rows[i].tranverse_cnt = 0;
        rows[i].req_wt_cnt = rows[i].ins_cnt = 0;
        ll new_rq = rows[i].write_q.try_get();
        if( new_rq == -1 ){
            die("!!!\n");
        }
        rows[i].write[new_rq].timestamp = \
        rows[i].write[new_rq].wts = rows[i].write[new_rq].rts = 0;
        rows[i].write[new_rq].type = WRITE;
        rows[i].write[new_rq].value[0] = rand();
        rows[i].wt_lst.insert(new_rq, -1);
        rows[i].v_num = 1;
        rows[i].txn_ids = new int[node_cnt*worker_per_server*txn_per_worker];
        for( ll j = 0; j < node_cnt*worker_per_server*txn_per_worker; j ++ ) rows[i].txn_ids[j] = 0;
        rows[i].hot.init( i );

        mappings[rows[i].key] = i;
    }

//    zipf.init( row_cnt*node_cnt );
    zipf.init( row_cnt );

    max_ts = (ll **)malloc( sizeof(ll*)*node_cnt );
    for( ll i = 0; i < node_cnt; i ++ ) {
        max_ts[i] = (ll *) malloc(sizeof(ll) * worker_per_server);
        for( ll j = 0; j < worker_per_server; j ++ ) max_ts[i][j] = -1;
    }
//    max_ts = (std::atomic<ll> *)malloc( sizeof(std::atomic<ll>)*node_cnt );
//    for( ll i = 0; i < node_cnt; i ++ ) max_ts[i] = -1;
}

void QueryMessage::init( MT _msg, RW _type, ll _timestamp,
        ll _txn_mg_id, ll _txn_id, ll _key, ll _sz, ll _value,
        ll _req_id, ll _s_id, ll _thd_id, ll o_ts){
    message_type = _msg;
    type = _type;
    timestamp = _timestamp;
    txn_mg_id = _txn_mg_id;
    txn_id = _txn_id;
    key = _key;
    sz = _sz;
    value[0] = _value;
    req_id = _req_id;
    source_node_id = _s_id;
    source_thd_id = _thd_id;
    ordering_ts = o_ts;
}

void QueryMessage::copy(QueryMessage *msg) {
    message_type = msg->message_type;
    ordering_ts = msg->ordering_ts;
    type = msg->type;
    timestamp = msg->timestamp;
    txn_mg_id = msg->txn_mg_id;
    txn_id = msg->txn_id;
    key = msg->key;
    sz = msg->sz;
    req_id = msg->req_id;
    source_node_id = msg->source_node_id;
    source_thd_id = msg->source_thd_id;
    memcpy( value, msg->value, sizeof(ll)*msg->sz );
}

void version::init(QueryMessage *msg) {
    if( msg->type == WRITE || msg->type == RMW )
        type = PWRITE;
    else if( msg->type == READ )
        type = READ;
    timestamp = get_timestamp();
    txn_mg_id = msg->txn_mg_id;
    txn_id = msg->txn_id;
    s_id = msg->source_node_id;
    thd_id = msg->source_thd_id;
    key = msg->key;
    memcpy( value, msg->value, msg->sz );
//    value = msg->value;
    wts = rts = msg->timestamp;
}

void QueryMessage::output(){
//    printf("RW %d txn_id %lld txn_mag_id %lld k %lld v %lld tmsp %lld s_id %lld th_id %lld r_id %lld\n",
//          type, txn_id, txn_mg_id, key, value, timestamp, source_node_id, source_thd_id, req_id);
    DEBUG("RW %d txn_id %lld txn_mag_id %lld k %lld v %lld tmsp %lld s_id %lld r_id %lld\n",
           type, txn_id, txn_mg_id, key, value, timestamp, source_node_id, req_id);

}

void AckMessage::init( MT _msg, RW _type,
                         bool _s, ll _txn_mg_id,
                         ll _txn_id,
                         ll _key, ll _sz, ll _value, ll _req_id, ll o_ts ){
    message_type = _msg;
    type = _type;
    succeed = _s;
    txn_mg_id = _txn_mg_id;
    txn_id = _txn_id;
    key = _key;
    sz = _sz;
    value[0] = _value;
    req_id = _req_id;
    ordering_ts = o_ts;
}

void AckMessage::output(){
//    printf("RW %d sd %lld txn_id %lld txn_mag_id %lld k %lld v %lld tmsp %lld\n",
//          type, succeed, txn_id, txn_mg_id, key, value, timestamp);
    DEBUG("RW %d sd %lld txn_id %lld txn_mag_id %lld k %lld v %lld tmsp %lld\n",
           type, succeed, txn_id, txn_mg_id, key, value, timestamp);

}

void CommitMessage::output(){
//    printf("RW %d sd %lld txn_id %lld txn_mag_id %lld k %lld tmsp %lld\n",
//           type, succeed, txn_id, txn_mg_id, key, timestamp);
    DEBUG("RW %d sd %lld txn_id %lld txn_mag_id %lld k %lld tmsp %lld\n",
          type, succeed, txn_id, txn_mg_id, key, timestamp);
}

void CommitMessage::init( MT _msg, RW _type,
                          bool _s, ll _s_id, ll _thd_id, ll _txn_mg_id,
                          ll _txn_id, ll _key, ll _sz, ll _v,
                          ll _tsp, ll _req_id, ll _v_id, ll o_ts ){
    message_type = _msg;
    type = _type;
    succeed = _s;
    source_id = _s_id;
    thd_id = _thd_id;
    txn_mg_id = _txn_mg_id;
    txn_id = _txn_id;
    key = _key;
    sz = _sz;
    value[0] = _v;
    timestamp = _tsp;
    req_id = _req_id;
    version_id = _v_id;
    ordering_ts = o_ts;
}

void Table::read(QueryMessage *msg, AckMessage *ack_msg, ll thd_id) {
    ack_msg->message_type = ACK;
    ack_msg->type = msg->type;
    ack_msg->ordering_ts = 1;
    ack_msg->txn_mg_id = msg->txn_mg_id;
    ack_msg->txn_id = msg->txn_id;
    ack_msg->key = msg->key;
    ack_msg->sz = msg->sz;
    ack_msg->req_id = msg->req_id;
    ack_msg->wait = false;

    // to support concurrent indexing in the future
    ll loc = mapp(msg->key);

    if( mvcc_type == 0 ){

        if( use_pri_lock ) rows[loc].prilock.lock( thd_id, msg->timestamp );
        else lock(&rows[loc].lock);

#ifndef __NO_TS
        if (msg->timestamp < rows[loc].v[0].wts){
            ack_msg->timestamp = rows[loc].v[0].wts;

            //        rows[loc].aborts[ack_msg->target_node_id] ++;
//            if( record_flag )
//            rows[loc].sth.push_back(sta(msg->timestamp,
//                    rows[loc].v[0].wts,
//                    ack_msg->target_node_id));
            unlock(&rows[loc].lock);
            ack_msg->succeed = false;
            return ;
        }
#endif

        ack_msg->value[0] = rows[loc].v[0].value;
        rows[loc].v[0].wts = msg->timestamp;

//      rows[loc].reqs[ack_msg->target_node_id] ++;
//        if( record_flag )
//        rows[loc].sth.push_back(sta(msg->timestamp,
//                                    rows[loc].v[0].wts,
//                                    ack_msg->target_node_id));
        unlock(&rows[loc].lock);

        // to modify
        ack_msg->timestamp = msg->timestamp;
        ack_msg->succeed = true;

    }
    else if( mvcc_type == 1 ){

    }else if( mvcc_type == 2 ){
        mvcc_read( msg, ack_msg, thd_id );
        update_max_ts( ack_msg->target_node_id, thd_id, msg->timestamp );
    }
}

void Table::write(QueryMessage *msg, AckMessage *ack_msg, ll thd_id){
    ack_msg->message_type = ACK;
    ack_msg->type = msg->type;
    ack_msg->ordering_ts = 1;
    ack_msg->txn_mg_id = msg->txn_mg_id;
    ack_msg->txn_id = msg->txn_id;
    ack_msg->req_id = msg->req_id;
    ack_msg->key = msg->key;
    ack_msg->sz = msg->sz;
    ack_msg->wait = false;

    // to support concurrent indexing in the future
    ll loc = mapp(msg->key);

    if( mvcc_type == 0 ) {


        if( use_pri_lock ) rows[loc].prilock.lock( thd_id, msg->timestamp );
        else lock(&rows[loc].lock);

#ifndef __NO_TS
        if (msg->timestamp < rows[loc].v[0].wts) {
            ack_msg->timestamp = rows[loc].v[0].wts;
            unlock(&rows[loc].lock);
            ack_msg->succeed = false;
            return ;
        }
#endif
        rows[loc].v[0].value = ack_msg->value[0];
        rows[loc].v[0].wts = msg->timestamp;
        unlock(&rows[loc].lock);

        // to modify
        ack_msg->timestamp = msg->timestamp;

        ack_msg->succeed = true;

    }
    else if( mvcc_type == 1 ){

    }else if( mvcc_type == 2 ){
        mvcc_write(msg, ack_msg, thd_id);
        update_max_ts( ack_msg->target_node_id, thd_id, msg->timestamp );
    }
}

void Table::rmw( QueryMessage *msg, AckMessage *ack_msg, ll thd_id ){
    ack_msg->message_type = ACK;
    ack_msg->type = msg->type;
    ack_msg->ordering_ts = 1;
    ack_msg->txn_mg_id = msg->txn_mg_id;
    ack_msg->txn_id = msg->txn_id;
    ack_msg->req_id = msg->req_id;
    ack_msg->key = msg->key;
    ack_msg->sz = msg->sz;
    ack_msg->wait = false;

    ll loc = mapp(msg->key);
    ll delay = 0;
    if (key_delay == -1) {
        if( rows[loc].hot.check_hot() )
            delay = rows[loc].hot.delay_int;
    } else if (loc < key_delay) {
        delay = rows[loc].hot.delay_int;
    }
    ll hot_ts = get_timestamp();

    if( mvcc_type == 2 ){
        row *ro = &rows[loc];

        if( use_pri_lock )
        ro->prilock.lock( thd_id, msg->timestamp );
        else lock(&ro->lock);

        ro->reqs[0] ++;
        if(key_delay == -1)
            ro->hot.update(hot_ts);

        ll ptr, ins_version_id = -1;
        ll tmp_cnt = 0;
        int flag = 0;
        if (ro->wt_lst.tail != -1)
            ack_msg->timestamp = ro->write[ro->wt_lst.tail].wts;

        recycle_list( ro );

        ll con_id = -1;
        for (ptr = ro->wt_lst.tail; ptr != -1; ptr = ro->wt_lst.prev[ptr]) {
            tmp_cnt++;
            if( compare_ts( ro->write[ptr].wts,
                    gen_aux_ts( ro->write[ptr].s_id, ro->write[ptr].thd_id ),
                    msg->timestamp,
                    gen_aux_ts( msg->source_node_id, msg->source_thd_id ) ) == -1 ){
//            if (ro->write[ptr].wts < msg->timestamp) {
                if( ro->write[ptr].rts >= msg->timestamp ){
                    flag = 0;
                    con_id = ptr;
                }else{
                    if( delay > 0 && msg->timestamp-ro->write[ptr].rts >= lower_delay ){
                        flag = 3;
                    }else{
                        if (ro->write[ptr].rts < msg->timestamp)
                            ro->write[ptr].rts = msg->timestamp;
                        if( ro->write[ptr].type == PWRITE ){
                            flag = 2;
                        }
                        else{
                            flag = 1;
                            memcpy( ack_msg->value, ro->write[ptr].value, msg->sz );
                            //                        ack_msg->value = ro->write[ptr].value;
                        }
                    }


                    ll new_rq = ro->write_q.try_get();
                    if( new_rq == -1 ){
                        fprintf(stdout, "key%lld wrt pool is full! %lld\n", ro->key, ro->v_num);
//                        recycle_list( ro );
                        flag = 0;
                    }else{
                        ro->write[new_rq].init( msg );
                        ro->wt_lst.insert(new_rq, ptr);
                        ro->v_num ++;
                        ins_version_id = new_rq;
//                        ro->txn_ids[ gen_txn_id(msg->source_node_id, thd_id, msg->txn_mg_id) ] \
                        = msg->txn_id;
                    }
                    //push sth into queue

                }
                break;
            }else{
                if( stale_reads == 1 || immortal_writes == 1 ){
                    if( ro->write[ptr].type == WRITE ){
                        flag = 0;
                        break;
                    }
                }
            }
        }
//        msg->sz = flag;
//        ro->log4.push_back( *msg );
//        ro->log5.push_back( ro->write[ptr] );
//        msg->sz = 1;

        ro->req_cnt++;
        ro->tranverse_cnt += tmp_cnt;

        if( flag == 1 ){
            ack_msg->version_id = ins_version_id;
        }
        if( flag == 2 ){
            // need insert
//            ll new_id = ro->read_q[thd_id].try_get();
            ll new_id = worker_thds[thd_id].read_q.try_get();
            if( new_id != -1 ){
//                ro->read[thd_id][new_id].copy(msg);
//                ro->rd_lst[thd_id].insert(new_id, -1);

                worker_thds[thd_id].read[new_id].copy(msg);
                worker_thds[thd_id].read[new_id].version_id = ins_version_id;
                ro->rd_lst[thd_id].insert(new_id, -1);
            }else{
                printf("Thread%lld key%lld read buffer overload!\n", thd_id, ro->key);
            }
        }
        if( flag == 3 ){
            //push into queue
            ll new_id = worker_thds[thd_id].free_ins_read.try_get();
            assert( new_id != -1 );
            worker_thds[thd_id].read_req_q_pool[new_id].init(
                    ro->key, ins_version_id, ro->write[ins_version_id].wts,
                    msg->txn_mg_id, msg->txn_id, msg->sz, msg->req_id,
                    msg->source_node_id, get_timestamp()+delay*1000
                    );
            worker_thds[thd_id].ins_read.push(new_id);
//            printf("key = %lld thd_id %lld s_id %lld txn_id %lld v_id %lld q_id %lld\n",\
                    ro->key, thd_id, msg->source_node_id, msg->txn_id, ins_version_id, new_id);
        }

        if( flag == 0 && loc == 0 ){
//            ll ts = get_timestamp();
//            assert( con_id != -1 );
//            sta.push_back( probe(ts, ack_msg->target_node_id, \
//            ro->write[con_id].timestamp, ro->write[con_id].s_id) );
        }

        if( flag == 0 ){
            ro->aborts[0] ++;
            if( 1.0*ro->aborts[0]/ro->reqs[0] > 0.9 && ro->hot.delay_cnt > 0 ){
                ro->hot.delay_cnt --;
//                ro->hot.delay_int *= 2;
            }
        }

        if( use_pri_lock )
            ro->prilock.unlock( thd_id );
        else unlock(&ro->lock);

        if( flag == 0 ){
            ack_msg->succeed = false;
        }else if( flag == 1 ){
            ack_msg->succeed = true;
        }else if( flag == 2 || flag == 3 ){
            ack_msg->succeed = false;
            ack_msg->wait = true;
        }

    }
    update_max_ts( ack_msg->target_node_id, thd_id, msg->timestamp );
}

void Table::mvcc_read(QueryMessage *msg, AckMessage *ack_msg, ll thd_id){
    ll loc = mapp(msg->key);
    row *ro = &rows[loc];

    ll delay = 0;
    if (key_delay == -1) {
        if( rows[loc].hot.check_hot() )
            delay = rmw_delay;
    } else if (loc < key_delay) {
        delay = rmw_delay;
    }
    ll hot_ts = get_timestamp();

    if( use_pri_lock )
        ro->prilock.lock( thd_id, msg->timestamp );
    else lock(&ro->lock);

    if(key_delay == -1)
        ro->hot.update(hot_ts);

    if( commit_phase == 0 ) {
        ll ptr;
        ll tmp_cnt = 0;
        int flag = 0;

        if (ro->wt_lst.tail != -1)
            ack_msg->timestamp = ro->write[ro->wt_lst.tail].wts;

        for (ptr = ro->wt_lst.tail; ptr != -1; ptr = ro->wt_lst.prev[ptr]) {
            tmp_cnt++;
            if ( compare_ts( ro->write[ptr].wts,
                             gen_aux_ts( ro->write[ptr].s_id, ro->write[ptr].thd_id ),
                             msg->timestamp,
                             gen_aux_ts( msg->source_node_id, msg->source_thd_id ) ) == -1 ) {
//            if (ro->write[ptr].wts < msg->timestamp) {
                if (ro->write[ptr].rts < msg->timestamp)
                    ro->write[ptr].rts = msg->timestamp;
                flag = 1;
//                ack_msg->value = ro->write[ptr].value;
                memcpy( ack_msg->value, ro->write[ptr].value, msg->sz );
                break;
            }
        }

        ro->req_cnt++;
        ro->tranverse_cnt += tmp_cnt;

        if (flag) {
//        DEBUG("key%lld from %lld read w(%lld)r(%lld) rts %lld\n",
//              ro->key, ack_msg->target_node_id,
//              ro->write[ptr].wts, ro->write[ptr].rts, msg->timestamp);
        } else {
//        DEBUG("error key%lld read nothing!!! rts %lld\n", ro->key, msg->timestamp);

//        for( ll ptr = ro->wt_lst.head; ptr != -1; ptr = ro->wt_lst.next[ptr] ){
//            DEBUG("w(%lld)r(%lld) ", ro->write[ptr].wts, ro->write[ptr].rts);
//        }
//        DEBUG("\n");
        }


        if( use_pri_lock )
            ro->prilock.unlock( thd_id );
        else unlock(&ro->lock);

        if (!flag) {
            ack_msg->succeed = false;
        } else {
            ack_msg->succeed = true;
        }
    }else{
        ll ptr;
        ll tmp_cnt = 0;
        int flag = 0;

        if (ro->wt_lst.tail != -1)
            ack_msg->timestamp = ro->write[ro->wt_lst.tail].wts;

        if( delay > 0 ){
            flag = 3;
        }else {

            for (ptr = ro->wt_lst.tail; ptr != -1; ptr = ro->wt_lst.prev[ptr]) {
                tmp_cnt++;
                if (compare_ts(ro->write[ptr].wts,
                               gen_aux_ts(ro->write[ptr].s_id, ro->write[ptr].thd_id),
                               msg->timestamp,
                               gen_aux_ts(msg->source_node_id, msg->source_thd_id)) == -1) {
                    if (ro->write[ptr].rts < msg->timestamp)
                        ro->write[ptr].rts = msg->timestamp;
                    if (ro->write[ptr].type == PWRITE) {
                        flag = 2;
//                    flag = 1;
                    } else {
                        flag = 1;
//                    ack_msg->value = ro->write[ptr].value;
                        memcpy(ack_msg->value, ro->write[ptr].value, msg->sz);
                    }
                    break;
                } else {
                    if (stale_reads == 1) {
                        if (ro->write[ptr].type == WRITE) {
                            flag = 0;
                            break;
                        }
                    }
                }
            }
        }

        ro->req_cnt++;
        ro->tranverse_cnt += tmp_cnt;

        if( flag == 2 ){
            // need insert
//            ll new_id = ro->read_q[thd_id].try_get();
            ll new_id = worker_thds[thd_id].read_q.try_get();
            if( new_id != -1 ){
//                ro->read[thd_id][new_id].copy(msg);
//                ro->rd_lst[thd_id].insert(new_id, -1);

                worker_thds[thd_id].read[new_id].copy(msg);
                worker_thds[thd_id].read[new_id].version_id = -1;
                ro->rd_lst[thd_id].insert(new_id, -1);
            }else{
                printf("Thread%lld key%lld read buffer overload!\n", thd_id, ro->key);
            }
        }else if( flag == 3 ){
            ll new_id = worker_thds[thd_id].free_ins_read.try_get();
            assert( new_id != -1 );
            worker_thds[thd_id].read_req_q_pool[new_id].init(
                    ro->key, -2, msg->timestamp,
                    msg->txn_mg_id, msg->txn_id, msg->sz, msg->req_id,
                    msg->source_node_id, get_timestamp()+delay*1000
            );
            worker_thds[thd_id].ins_read.push(new_id);
//            printf("key = %lld thd_id %lld s_id %lld txn_id %lld v_id %lld q_id %lld\n",\
                    ro->key, thd_id, msg->source_node_id, msg->txn_id, ins_version_id, new_id);

        }

        if (flag) {
//        DEBUG("key%lld from %lld read w(%lld)r(%lld) rts %lld\n",
//              ro->key, ack_msg->target_node_id,
//              ro->write[ptr].wts, ro->write[ptr].rts, msg->timestamp);
        } else {
//        DEBUG("error key%lld read nothing!!! rts %lld\n", ro->key, msg->timestamp);

//        for( ll ptr = ro->wt_lst.head; ptr != -1; ptr = ro->wt_lst.next[ptr] ){
//            DEBUG("w(%lld)r(%lld) ", ro->write[ptr].wts, ro->write[ptr].rts);
//        }
//        DEBUG("\n");
        }

        if( use_pri_lock )
            ro->prilock.unlock( thd_id );
        else unlock(&ro->lock);

        if( flag == 0 ){
            ack_msg->succeed = false;
        }else if( flag == 1 ){
            ack_msg->succeed = true;
        }else if( flag == 2 || flag == 3 ){
            ack_msg->succeed = false;
            ack_msg->wait = true;
        }
    }

}

void Table::mvcc_write(QueryMessage *msg, AckMessage *ack_msg, ll thd_id){
    ll loc = mapp(msg->key), new_rq = -1;
    row *ro = &rows[loc];

    ll hot_ts = get_timestamp();

    if( use_pri_lock )
        ro->prilock.lock( thd_id, msg->timestamp );
    else
        lock(&ro->lock);

    if(key_delay == -1)
        ro->hot.update(hot_ts);

    recycle_list( ro );

    ll ptr;
    int flag = 0, fl = 0;

    if( ro->wt_lst.tail != -1 )
        ack_msg->timestamp = ro->write[ro->wt_lst.tail].wts;

    new_rq = ro->write_q.try_get();
    if( new_rq == -1 ) {
        fprintf(stdout, "key%lld wrt pool is full! %lld\n", ro->key, ro->v_num);
//        fprintf(stdout, "key%lld wrt pool is full! ts %lld %lld %lld cnt %lld\n",
//                ro->key, msg->timestamp,
//                max_ts[0][0]-msg->timestamp,
//                max_ts[1][0]-msg->timestamp, ro->v_num);

        if( use_pri_lock )
            ro->prilock.unlock( thd_id );
        else
            unlock(&ro->lock);

        ack_msg->succeed = false;
        return ;
    }
    ro->write[new_rq].init( msg );

    ll tmp_cnt = 0;
    ll ins_version_id = new_rq;
    for( ptr = ro->wt_lst.tail; ptr != -1; ptr = ro->wt_lst.prev[ptr] ){
        tmp_cnt ++;
//        if( ro->write[ptr].wts < msg->timestamp ){
        if( compare_ts( ro->write[ptr].wts,
                        gen_aux_ts( ro->write[ptr].s_id, ro->write[ptr].thd_id ),
                        msg->timestamp,
                        gen_aux_ts( msg->source_node_id, msg->source_thd_id ) ) == -1 ){
            if( ro->write[ptr].rts > msg->timestamp ){
                flag = -1;
                ro->write_q.push(new_rq);
            }else {
                ro->wt_lst.insert(new_rq, ptr);
                ro->v_num ++;
                flag = 1;
            }
            break;
        }else{
            if( immortal_writes == 1 ) {
                if( ro->write[ptr].type == WRITE ){
                    flag = -1;
                    ro->write_q.push(new_rq);
                    break;
                }
            }
        }
    }

    ro->req_wt_cnt ++;
    ro->ins_cnt += tmp_cnt;

//    DEBUG("key%lld ", ro->key);
//    for( ll ptr = ro->wt_lst.head; ptr != -1; ptr = ro->wt_lst.next[ptr] ){
//        DEBUG("w(%lld)r(%lld) ", ro->write[ptr].wts, ro->write[ptr].rts);
//    }
//    DEBUG("\n");

    if( flag == 0 ){
        ro->wt_lst.insert(new_rq, -1);
        ro->v_num ++;
    }

    if( flag != -1 ){
        ack_msg->version_id = ins_version_id;
//        ro->txn_ids[ gen_txn_id(msg->source_node_id, thd_id, msg->txn_mg_id) ] \
                        = msg->txn_id;
    }
    ro->update_cnt ++;
//    check(&ro->wt_lst, &ro->write_q);

    if( use_pri_lock )
        ro->prilock.unlock( thd_id );
    else
        unlock(&ro->lock);


    if( flag == -1 ){
        ack_msg->succeed = false;
    }else{
        ack_msg->succeed = true;
    }

}

void Table::mvcc_commit_write(CommitMessage *cmsg, ll thd_id, mvcc_commit_ret *ret ) {
    ll loc = mapp(cmsg->key);
    row *ro = &rows[loc];

    if( use_pri_lock )
        ro->prilock.lock( thd_id, 1LL<<61 );
    else
        lock(&ro->lock);

    ll ptr;

    // record ptr and directly commit

    if( cmsg->succeed == true ){
        int fl = 0;
        ll ptr = cmsg->version_id;
        assert( ptr != -1 );
        //make sure ptr is in chain
        ll in_chain = 0;
        if( ro->wt_lst.prev[ptr] != -1 || ro->wt_lst.next[ptr] != -1 )
            in_chain = 1;
        if( ro->wt_lst.tail == ptr )
            in_chain = 1;
        if( in_chain ) {
            if (ro->write[ptr].wts == cmsg->timestamp
            && ro->write[ptr].s_id == cmsg->source_id
            && ro->write[ptr].thd_id == cmsg->thd_id
            && ro->write[ptr].type == PWRITE) {
                ro->write[ptr].type = WRITE;
                fl = 1;
            }
        }
//        for( ptr = ro->wt_lst.tail; ptr != -1; ptr = ro->wt_lst.prev[ptr] ){
//            if( ro->write[ptr].wts == cmsg->timestamp && ro->write[ptr].type == PWRITE ){
//                ro->write[ptr].type = WRITE;
//                fl = 1;
//                break;
//            }
//        }


//        cmsg->sz = ptr;
//        cmsg->value[1] = fl;
//        cmsg->value[2] = get_timestamp();
//        ro->log.push_back( *cmsg );

    }else{
        int fl = 0;
        ll ptr = cmsg->version_id;
        ll tmp_cnt = 0;
//        assert( ptr != -1 );
        //a req comes if it doesn't fail
        if(1){
//        if( ptr == -1 ){
//            ll tmp = ro->txn_ids[ gen_txn_id(cmsg->source_id, thd_id, cmsg->txn_mg_id) ];
//            if( tmp >= cmsg->txn_id ){
                tmp_cnt = 0;
                for( ptr = ro->wt_lst.tail; ptr != -1; ptr = ro->wt_lst.prev[ptr] ){
                    tmp_cnt ++;
                    if( ro->write[ptr].wts == cmsg->timestamp
                    && ro->write[ptr].s_id == cmsg->source_id
                    && ro->write[ptr].thd_id == cmsg->thd_id
                    && ro->write[ptr].type == PWRITE ){
                        assert( ro->wt_lst.prev[ptr] != -1 );
                        if( ro->write[ptr].rts != ro->write[ptr].wts )
                            ro->write[ ro->wt_lst.prev[ptr] ].rts = ro->write[ptr].rts;
                        barrier();

                        ro->wt_lst.delt( ptr );
                        ro->v_num --;
                        ro->write_q.push(ptr);
                        fl = 1;
                        break;
                    }
                }
//            }
        }else {
            //make sure ptr is in chain
            ll in_chain = 0;
            if (ro->wt_lst.prev[ptr] != -1 || ro->wt_lst.next[ptr] != -1)
                in_chain = 1;
            if (ro->wt_lst.tail == ptr)
                in_chain = 1;
            if (in_chain) {
                if (ro->write[ptr].wts == cmsg->timestamp
                && ro->write[ptr].s_id == cmsg->source_id
                && ro->write[ptr].thd_id == cmsg->thd_id
                && ro->write[ptr].type == PWRITE) {
                    assert(ro->wt_lst.prev[ptr] != -1);
                    if (ro->write[ptr].rts > ro->write[ptr].wts)
                        ro->write[ro->wt_lst.prev[ptr]].rts = ro->write[ptr].rts;
                    barrier();

                    ro->wt_lst.delt(ptr);
                    ro->v_num--;
                    ro->write_q.push(ptr);
                    fl = 1;
                }
            }
        }

//        cmsg->sz = ptr;
//        cmsg->value[1] = fl;
//        cmsg->value[2] = tmp_cnt;
//        ro->log.push_back( *cmsg );
    }
    for( ll i = 0; i < worker_per_server; i ++ ){
        if( ro->rd_lst[i].tail != -1 )
            worker_thds[i].shared_upd_read->lock_push(ro->key);
    }


    if( use_pri_lock )
        ro->prilock.unlock( thd_id );
    else
        unlock(&ro->lock);
}

void Table::insert_read_req(ll thd_id, Read_req_item *item, ll item_id){
    ll key = item->key;
    ll loc = mapp(key);
    row *ro = &rows[loc];
    if( use_pri_lock )
        ro->prilock.lock( thd_id, 1 );
    else lock(&ro->lock);

    if( item->version_id == -2 ){
        ll ptr;
        ll flag;
        for (ptr = ro->wt_lst.tail; ptr != -1; ptr = ro->wt_lst.prev[ptr]) {
            if (compare_ts(ro->write[ptr].wts,
                           gen_aux_ts(ro->write[ptr].s_id, ro->write[ptr].thd_id),
                           item->timestamp,
                           gen_aux_ts(item->s_id, thd_id)) == -1) {
                if (ro->write[ptr].rts < item->timestamp)
                    ro->write[ptr].rts = item->timestamp;
                if (ro->write[ptr].type == PWRITE) {
                    flag = 2;
//                    flag = 1;
                } else {
                    flag = 1;
//                    ack_msg->value = ro->write[ptr].value;
//                    memcpy(ack_msg->value, ro->write[ptr].value, msg->sz);
                }
                break;
            } else {
                if (stale_reads == 1) {
                    if (ro->write[ptr].type == WRITE) {
                        flag = 0;
                        break;
                    }
                }
            }
        }
        ll ret_ts;
        if (ro->wt_lst.tail != -1)
            ret_ts = ro->write[ro->wt_lst.tail].wts;

        if( flag == 0 || flag == 1 ){

            if (use_pri_lock)
                ro->prilock.unlock(thd_id);
            else unlock(&ro->lock);

            bool _su;
            if( flag == 0 ){
                _su = false;
            }else if( flag == 1 ){
                _su = true;
            }
            ll target_node_id = item->s_id;
            if (target_node_id == my_node_id) {
                ll msg_id = worker_thds[thd_id].free_msg_q->get();
                AckMessage *ack_msg = (AckMessage *) &worker_thds[thd_id]\
.local_msg[msg_id * rdma_buffer_size];
                ack_msg->init(ACK, READ, _su, item->txn_mg_id,
                              item->txn_id, item->key, item->sz,
                              0, item->req_id, 0);
                ack_msg->timestamp = ret_ts;
//                ack_msg->version_id = item->version_id;
//            memcpy( ack_msg->value, qmsg->value, item->sz );
                worker_thds[thd_id].pending_msg_q->push(msg_id);
            } else {
                ll source_id = item->s_id;
                while (worker_thds[thd_id].send_cnt[source_id] <
                       worker_thds[thd_id].send_finished[source_id]);
                ll send_id = worker_thds[thd_id].send_cnt[source_id]++;
                // not ++ if read needs to be blocked

                ll send_tmp = send_id;
                send_id %= max_cq_num;
                char *pr = &worker_thds[thd_id].rdma->\
                rdma_send_region[source_id][send_id * rdma_buffer_size];
                AckMessage *ack_msg = (AckMessage *) pr;

                ack_msg->init(ACK, READ, _su, item->txn_mg_id,
                              item->txn_id, item->key, item->sz,
                              0, item->req_id, 0);
                ack_msg->timestamp = ret_ts;
//                ack_msg->version_id = item->version_id;
//            memcpy( ack_msg->value, qmsg->value, item->sz );
                worker_thds[thd_id].rdma->send(source_id,
                                               send_tmp * node_cnt + source_id,
                                               pr, sizeof(AckMessage), 0);
            }
        }else if( flag == 2 ){
            ll new_id = worker_thds[thd_id].read_q.try_get();
            if (new_id != -1) {
                worker_thds[thd_id].read[new_id].init(
                        QUERY, READ, item->timestamp, item->txn_mg_id,
                        item->txn_id, item->key, item->sz, 0,
                        item->req_id, item->s_id, thd_id, 0
                );
                worker_thds[thd_id].read[new_id].version_id = item->version_id;
                ro->rd_lst[thd_id].insert(new_id, -1);
            } else {
                printf("Thread%lld key%lld read buffer overload!\n", thd_id, ro->key);
            }
            if (use_pri_lock)
                ro->prilock.unlock(thd_id);
            else unlock(&ro->lock);
        }
    }else {
        version *vs = &ro->write[item->version_id];

        // if PWRITE is aborted
        ll in_chain = 0;
        if (ro->wt_lst.prev[item->version_id] != -1 || ro->wt_lst.next[item->version_id] != -1)
            in_chain = 1;
        if (ro->wt_lst.tail == item->version_id)
            in_chain = 1;

        if (in_chain == 0) {
            if (use_pri_lock)
                ro->prilock.unlock(thd_id);
            else unlock(&ro->lock);
        } else {

            assert(vs->wts == item->timestamp);
            assert(ro->wt_lst.prev[item->version_id] != -1);
            ro->write[ro->wt_lst.prev[item->version_id]].rts = item->timestamp;
        }
//    item->start_ts = get_timestamp();
//    item->sz = ro->wt_lst.prev[item->version_id];
//    if( in_chain == 0 ) item->sz = -1;
//    ro->log2.push_back( *item );
//    ro->log3.push_back( ro->write[ ro->wt_lst.prev[item->version_id] ] );
//    item->sz = 1;
        if (in_chain == 0) return;

        if (ro->write[ro->wt_lst.prev[item->version_id]].type == PWRITE) {
            // need to wait
            ll new_id = worker_thds[thd_id].read_q.try_get();
            if (new_id != -1) {
                worker_thds[thd_id].read[new_id].init(
                        QUERY, RMW, item->timestamp, item->txn_mg_id,
                        item->txn_id, item->key, item->sz, 0,
                        item->req_id, item->s_id, thd_id, 0
                );
                worker_thds[thd_id].read[new_id].version_id = item->version_id;
                ro->rd_lst[thd_id].insert(new_id, -1);
            } else {
                printf("Thread%lld key%lld read buffer overload!\n", thd_id, ro->key);
            }
            if (use_pri_lock)
                ro->prilock.unlock(thd_id);
            else unlock(&ro->lock);
        } else {
            // return directly
            ll ret_ts;
            if (ro->wt_lst.tail != -1)
                ret_ts = ro->write[ro->wt_lst.tail].wts;

            if (use_pri_lock)
                ro->prilock.unlock(thd_id);
            else unlock(&ro->lock);

            ll target_node_id = item->s_id;
            if (target_node_id == my_node_id) {
                ll msg_id = worker_thds[thd_id].free_msg_q->get();
                AckMessage *ack_msg = (AckMessage *) &worker_thds[thd_id]\
.local_msg[msg_id * rdma_buffer_size];
                ack_msg->init(ACK, RMW, true, item->txn_mg_id,
                              item->txn_id, item->key, item->sz,
                              0, item->req_id, 0);
                ack_msg->timestamp = ret_ts;
                ack_msg->version_id = item->version_id;
//            memcpy( ack_msg->value, qmsg->value, item->sz );
                worker_thds[thd_id].pending_msg_q->push(msg_id);
            } else {
                ll source_id = item->s_id;
                while (worker_thds[thd_id].send_cnt[source_id] <
                       worker_thds[thd_id].send_finished[source_id]);
                ll send_id = worker_thds[thd_id].send_cnt[source_id]++;
                // not ++ if read needs to be blocked

                ll send_tmp = send_id;
                send_id %= max_cq_num;
                char *pr = &worker_thds[thd_id].rdma->\
                rdma_send_region[source_id][send_id * rdma_buffer_size];
                AckMessage *ack_msg = (AckMessage *) pr;

                ack_msg->init(ACK, RMW, true, item->txn_mg_id,
                              item->txn_id, item->key, item->sz,
                              0, item->req_id, 0);
                ack_msg->timestamp = ret_ts;
                ack_msg->version_id = item->version_id;
//            memcpy( ack_msg->value, qmsg->value, item->sz );
                worker_thds[thd_id].rdma->send(source_id,
                                               send_tmp * node_cnt + source_id,
                                               pr, sizeof(AckMessage), 0);
            }
        }
    }
    worker_thds[thd_id].free_ins_read.push(item_id);
}

void Table::update_read_req(ll key, ll thd_id, mvcc_commit_ret *ret){
//    printf("begin\n");
//    for( ll ptr = ro->rd_lst.tail; ptr != -1; ptr = ro->rd_lst.prev[ptr] )
//        printf("%lld ", ptr);
//    printf("\n");
    ll loc = mapp(key);
    row *ro = &rows[loc];

//    if( use_pri_lock )
//    ro->prilock.lock( thd_id, 1LL<<61 );
//    else lock(&ro->lock);


    QueryMessage *read = worker_thds[thd_id].read;

    for( ll ptr = ro->rd_lst[thd_id].tail; ptr != -1;  ){
        ll flag = 0;
        QueryMessage *qmsg = &read[ptr];
//        ll in_chain = 0;
//        if( qmsg->version_id != -1 ) {
//            if (ro->wt_lst.prev[qmsg->version_id] != -1 || ro->wt_lst.next[qmsg->version_id] != -1)
//                in_chain = 1;
//            if (ro->wt_lst.tail == qmsg->version_id)
//                in_chain = 1;
//            if( in_chain == 0 ){
//                ll nxt = ro->rd_lst[thd_id].prev[ptr];
//                ro->rd_lst[thd_id].delt(ptr);
//                worker_thds[thd_id].read_q.push(ptr);
//                ptr = nxt;
//                continue;
//            }
//        }


        for( ll qtr = ro->wt_lst.tail; qtr != -1; qtr = ro->wt_lst.prev[qtr] ){
//            if (ro->write[qtr].wts < read[ptr].timestamp) {
            if ( compare_ts( ro->write[qtr].wts,\
                             gen_aux_ts( ro->write[qtr].s_id, ro->write[qtr].thd_id ),\
                             read[ptr].timestamp,\
                             gen_aux_ts( read[ptr].source_node_id, read[ptr].source_thd_id ) ) == -1 ) {
                if (ro->write[qtr].rts < read[ptr].timestamp)
                    ro->write[qtr].rts = read[ptr].timestamp;
                if( ro->write[qtr].type == PWRITE ) break;

                flag = 1;
//                ro->read[thd_id][ptr].value = ro->write[qtr].value;
                memcpy( read[ptr].value, ro->write[qtr].value, read[ptr].sz );
                break;
            }
        }
        if( flag == 1 ){
            if( read[ptr].source_thd_id != thd_id ){
//                Queue *q = worker_thds[ro->read[thd_id][ptr].source_thd_id].shared_upd_read;
//                lock(&q->lock_flag);
//                if( q->front-q->tail >= q->cnt ){
//                    assert(1==0);
//                }else{
//                    unlock(&q->lock_flag);
//                }

                printf("????????????????\n");
                assert(read[ptr].source_thd_id == thd_id);
                worker_thds[read[ptr].source_thd_id].shared_upd_read->lock_push(ro->key);
                ptr = ro->rd_lst[thd_id].prev[ptr];
            }else {
                if (ro->wt_lst.tail != -1)
                    read[ptr].timestamp = ro->write[ro->wt_lst.tail].wts;
                QueryMessage *qmsg = &read[ptr];
//            printf("return N%lld key%lld req_id %lld\n", qmsg->source_node_id, qmsg->key, qmsg->req_id);
                ll target_node_id = qmsg->source_node_id;
                if (target_node_id == my_node_id) {
                    ll msg_id = worker_thds[thd_id].free_msg_q->get();
                    AckMessage *ack_msg = (AckMessage *) &worker_thds[thd_id]\
                    .local_msg[msg_id * rdma_buffer_size];
                    ack_msg->init(ACK, qmsg->type, true, qmsg->txn_mg_id,
                                  qmsg->txn_id, qmsg->key, qmsg->sz, qmsg->value[0], qmsg->req_id, 1);
                    memcpy( ack_msg->value, qmsg->value, qmsg->sz );
                    ack_msg->version_id = qmsg->version_id;

//                    ack_msg->timestamp = qmsg->timestamp;

                    ret->local.push_back(msg_id);
                } else {
                    ll source_id = qmsg->source_node_id;
                    while (worker_thds[thd_id].send_cnt[source_id] <
                           worker_thds[thd_id].send_finished[source_id]);
                    ll send_id = worker_thds[thd_id].send_cnt[source_id]++;
                    // not ++ if read needs to be blocked

                    ll send_tmp = send_id;
                    send_id %= max_cq_num;
                    char *pr = &worker_thds[thd_id].rdma->\
                rdma_send_region[source_id][send_id * rdma_buffer_size];
                    AckMessage *ack_msg = (AckMessage *) pr;

                    ack_msg->init(ACK, qmsg->type, true, qmsg->txn_mg_id,
                                  qmsg->txn_id, qmsg->key, qmsg->sz, qmsg->value[0], qmsg->req_id, 1);
                    memcpy( ack_msg->value, qmsg->value, qmsg->sz );
                    ack_msg->version_id = qmsg->version_id;

//                    ack_msg->timestamp = qmsg->timestamp;


                    ret->remote.push_back(std::make_pair(source_id, send_tmp));
                }

                ll nxt = ro->rd_lst[thd_id].prev[ptr];
                ro->rd_lst[thd_id].delt(ptr);
                worker_thds[thd_id].read_q.push(ptr);
                ptr = nxt;
            }
        }else{
            ptr = ro->rd_lst[thd_id].prev[ptr];
        }
    }
//    if( use_pri_lock )
//    ro->prilock.unlock( thd_id );
//    else unlock(&ro->lock);
}

ll Table::mapp( ll key ){
    return mappings[key];
//    return key-my_node_id*row_cnt;
}

void Table::update_max_ts( ll node_id, ll thd_id, ll ts ){
    return;
}

void Table::recycle_list(row *ro) {
//    return ;

    // here to modify
    ll buf_size = get_buf_size(ro->key);

    if( ro->v_num < buf_size/2 ) return ;

    ll min_ts_tmp = (1LL<<62), cnt = 0;
    ll ptr;
    int flag = 0;

    for( ptr = ro->wt_lst.head; ptr != -1;  ) {
        if( ro->wt_lst.head == ro->wt_lst.tail ){
            if( ro->wt_lst.head == -1 ){
                fprintf(stdout, "key %lld no version.\n", ro->key);
            }
            break;
        }
        if( ro->write[ptr].type == PWRITE ){
            ptr = ro->wt_lst.next[ptr];
            continue;
        }
        if( ro->write[ptr].wts < min_ts_tmp && flag == 0 ){
            flag = 1;
            ptr = ro->wt_lst.next[ptr];
            continue;
        }
        if( ro->write[ptr].wts < min_ts_tmp ){
            ll pre = ro->wt_lst.prev[ptr];
            if( ro->write[ptr].rts > ro->write[ptr].wts )
                ro->write[pre].rts = ro->write[ptr].rts;

            ll nxt = ro->wt_lst.next[ptr];
            ro->wt_lst.delt( ptr );

            ro->v_num --;
            ro->write_q.push(ptr);
            ptr = nxt;
            cnt ++;
            // here to modify too
            if( cnt >= buf_size/5 ) break;
            continue;
        }
        else break;
    }
}

ll Table::get_buf_size( ll key ){
    if( workload == 3 ) {
        ll id = tpcc.which_key(key);
        if (id == 0) return wh_req_buffer;
        if (id == 1) return dst_req_buffer;
        return req_buffer_size;
    }else{
        if( key%row_cnt < ext_keys ) return ext_req_buffer_size;
        return req_buffer_size;
    }
}

void txn_item::output() {
//    DEBUG("k: %lld v: %lld\n", key, value);
}

ll gen_aux_ts( ll s_id, ll thd_id ){
    return s_id*worker_per_server+thd_id;
}

ll compare_ts( ll ts_1, ll aux_ts_1, ll ts_2, ll aux_ts_2 ){
    if( ts_1 > ts_2 ) return 1;
    if( ts_1 < ts_2 ) return -1;
    if( ts_1 == ts_2 ){
        if( aux_ts_1 > aux_ts_2 ) return 1;
        if( aux_ts_1 < aux_ts_2 ) return -1;
        if( aux_ts_1 == aux_ts_2 ) return 0;
    }
}

ll check( Linklist *lst, Queue *q ){
    std::vector<ll> v;
    ll cnt1 = 0, cnt2 = 0;
    for( ll i = q->tail; i < q->front; i ++ ){
        v.push_back(q->qe[i%q->cnt]);
        cnt1 ++;
    }
    for( ll ptr = lst->head; ptr != -1; ptr = lst->next[ptr] ){
        v.push_back(ptr);
        cnt2 ++;
    }
    std::sort( v.begin(), v.end() );
    ll sz1 = v.size();
//    std::unique(v.begin(), v.end());
    ll sz2 = v.size();
    int flag = 0;
    if( v[0] == -1 ){
        fprintf(stdout, "-1  ");
        flag = 1;
    }
    if( sz1 != sz2 ){
        flag = 1;
        fprintf(stdout, "dup %lld %lld %lld %lld ", sz1, sz2, cnt1, cnt2);
    }
    if( sz1 != lst->cnt ) {
        flag = 1;
        fprintf(stdout, "small %lld %lld %lld %lld",
                sz2, lst->cnt, cnt1, cnt2);
    }
    if( flag == 1 ) {
//        printf("q: ");
//        for( ll i = q->tail; i < q->front; i ++ ){
//            printf("%lld ", q->qe[i%q->cnt]);
//        }
//        printf("l: ");
//        for( ll ptr = lst->head; ptr != -1; ptr = lst->next[ptr] ) {
//            printf("%lld ", ptr);
//        }
        fprintf(stdout, "\n");
    }
}

void MessageControl::init() {
    free_q.init(500);
    my_q.init(500);
    for( ll i = 0; i < 500; i ++ )
        free_q.push(i);
    v = new MessageControlItem[500];
}

void MessageControl::push( char* ptr, ll id, bool local ){
    Message *msg = (Message *)ptr;
    ll ts;
    if( reordering == 1 )
        ts = msg->ordering_ts;
    else if( reordering == 0 ){
        ts = cnt ++;
    }

    if( reordering == 2 ) {
        ll m_id = free_q.try_get();
        assert( m_id != -1 );
        v[m_id].ts = ts;
        v[m_id].ptr = ptr;
        v[m_id].id = id;
        v[m_id].local = local;
//        if( msg->message_type == QUERY ){
//            tmp->type = 0;
//    //        qmsg->output();
//        }else if( msg->message_type == ACK ){
//            AckMessage *qmsg = (AckMessage *)msg;
//            tmp->type = 1;
//    //        qmsg->output();
//        }else if( msg->message_type == CMT ){
//            CommitMessage *qmsg = (CommitMessage *)msg;
//            tmp->type = 2;
//    //        qmsg->output();
//        }

        my_q.push(m_id);
    }else if( reordering == 0 || reordering == 1 ) {
        MessageControlItem tmp;
        tmp.ts = ts;
        tmp.ptr = ptr;
        tmp.id = id;
        tmp.local = local;
//        if (msg->message_type == QUERY) {
//            tmp.type = 0;
////        qmsg->output();
//        } else if (msg->message_type == ACK) {
//            AckMessage *qmsg = (AckMessage *) msg;
//            tmp.type = 1;
////        qmsg->output();
//        } else if (msg->message_type == CMT) {
//            CommitMessage *qmsg = (CommitMessage *) msg;
//            tmp.type = 2;
////        qmsg->output();
//        }

    q.push(tmp);
//        q_bp.push(tmp);
    }
}

void MessageControl::pop(MessageControlItem *p){
    if( reordering == 0 || reordering == 1 ) {
        if (q.empty()) {
            p->ts = -11;
            return;
        }
        MessageControlItem tmp = q.top();
        q.pop();
        p->ts = tmp.ts;
        p->ptr = tmp.ptr;
        p->id = tmp.id;
        p->local = tmp.local;
        return;
    }

    if( reordering == 0 ) {
        if (q_bp.empty()) {
            p->ts = -11;
            return;
        }
        MessageControlItem tmp = q_bp.front();
        q_bp.pop();
        p->ts = tmp.ts;
        p->ptr = tmp.ptr;
        p->id = tmp.id;
        p->local = tmp.local;
        return;
    }else if( reordering == 2 ) {

        ll m_id = my_q.try_get();
        if( m_id == -1 ){
            p->ts = -11;
            return;
        }
        MessageControlItem *tmp = &v[m_id];
        memcpy(p, tmp, sizeof(MessageControlItem));
        free_q.push(m_id);
        return;
    }
}

