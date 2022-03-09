//
// Created by prince_de_marcia on 2021/3/25.
//

#include <netdb.h>
#include <unistd.h>
#include <time.h>
#include "thread.h"
#include "table.h"
#include "rdma.h"
#include "txn.h"
#include "util.h"
#include "config.h"
#include "clock_sync.h"

void MyThread::setup(ll _node_id, ll _thd_id) {
    thd_id = _thd_id;
    node_id = _node_id;
}

void MyThread::run() {

}


void WorkerThread::run() {
    // bind core
    bind_core( bind_cores[thd_id] );

    client_thd_id = thd_id + worker_per_server;

    /*init txn table*/
    txn = (Txn *)malloc( sizeof(Txn) );
    txn->init( txn_per_worker );

    /*init rdma connection*/

    rdma = (RDMA *)malloc( sizeof(RDMA) );
    rdma->init(node_id, thd_id);

    send_cnt = (ll*)malloc(sizeof(ll)*node_cnt);
    for( ll i = 0; i < node_cnt; i ++ ) send_cnt[i] = 0;
    send_finished = (ll*)malloc(sizeof(ll)*node_cnt);
    for( ll i = 0; i < node_cnt; i ++ ) send_finished[i] = 0;
    recv_cnt = (ll*)malloc(sizeof(ll)*node_cnt);
    for( ll i = 0; i < node_cnt; i ++ ) recv_cnt[i] = 0;
    recv_finished = (ll*)malloc(sizeof(ll)*node_cnt);
    for( ll i = 0; i < node_cnt; i ++ ) recv_finished[i] = 0;

//    request_callback_queue = (Queue *)malloc(sizeof(Queue));
//    request_callback_queue->init(txn_per_worker*2);
//    client_thds[thd_id].request_callback_queue = request_callback_queue;
//    request_retry_queue = (Queue *)malloc(sizeof(Queue));
//    request_retry_queue->init(txn_per_worker*2);

    shared_upd_read = (Queue *)malloc(sizeof(Queue));
    shared_upd_read->init(50000);
    mctrl.init();
    ins_read.init(read_req_pending_size);
    free_ins_read.init(read_req_pending_size);
    for( ll i = 0; i < read_req_pending_size; i ++ ) free_ins_read.push(i);
    read_req_q_pool = (Read_req_item *)malloc( read_req_pending_size*sizeof(Read_req_item) );

    assert( max_cq_num <= qp_size );
//    assert( txn_size*sizeof(QueryMessage)*2 );

    for( ll j = 0; j < node_cnt; j ++ ) {
        if( j == my_node_id ) continue;
//        if( j == 0 && thd_id == 0 && show_sync_time == 1 ){
//            rdma->recv( j, 4396,NULL,0 );
//        }
        for (ll i = 0; i < max_cq_num; i++) {
            //DEBUG("post recv %lld %lld %p\n", j, i, rdma->rdma_recv_region[j]+i*rdma_buffer_size);
//            if( i == max_cq_num-1 ){
//                i = max_cq_num-1;
//            }
            rdma->recv( j, i*node_cnt+j,
                    rdma->rdma_recv_region[j]+i*rdma_buffer_size,
                    rdma_buffer_size );
        }
        recv_cnt[j] = max_cq_num;
        recv_finished[j] = 0;
    }


    free_msg_q = (Queue*)malloc(sizeof(Queue));
    pending_msg_q = (Queue*)malloc(sizeof(Queue));
    local_msg = (char *)malloc( 2000*rdma_buffer_size );
    free_msg_q->init(2000);
    pending_msg_q->init(2000);
    for( ll i = 0; i < 2000; i ++ ){
        free_msg_q->push(i);
    }
    client.init(thd_id);

    txn_cnt = 0;
    timestamp_offset = 0;
    ts_cnt = 0;

    /*init local buffer*/
    read_q.init(read_req_buffer_size);
    read_lst.init(read_req_buffer_size);
    for( ll i = 0; i < read_req_buffer_size; i ++ ) read_q.push(i);
    read = (QueryMessage *)malloc(sizeof(QueryMessage)*read_req_buffer_size);
    for( ll i = 0; i < table.row_cnt; i ++ ){
        table.rows[i].rd_lst[thd_id].init2( &read_lst );
    }

    struct ibv_wc* wc_array;
    wc_array = ( struct ibv_wc* ) malloc( sizeof(struct ibv_wc) * poll_size );

    /*statistics*/
    txn_finished = 0;
    txn_aborted = 0;
    txn_executed = 0;
    req_cnt = 0;
    req_aborted = 0;
    lat_txn_finished = 0;
    issued_req_cnt = 0;
    recv_ack_cnt = 0;
    recv_qry_cnt = 0;
    lat_get_ts = 0;
    rd_intvl = 0;
    wt_intvl = 0;
    rmw_intvl = 0;
    rd_txn_finished = 0;
    rmw_txn_finished = 0;
    ctrl_cnt = 0;
    ctrl_intvl = 0;
    c2_intvl = 0;
    c2_cnt = 0;
    local_req = 0;
    remote_req = 0;

//    struct ibv_qp_attr qp_attr;
//    struct ibv_qp_init_attr qp_init_attr;
//    for( ll i = 0; i < node_cnt; i ++ ){
//        if( i == my_node_id ) continue;
//        int ret = ibv_query_qp(rdma->qp[i], &qp_attr,
//                IBV_QP_STATE|IBV_QP_CUR_STATE|IBV_QP_DEST_QPN, &qp_init_attr);
//        //DEBUG("N%lldT%lld node%lld ret %lld\n", node_id,thd_id,i,ret);
//        DEBUG("xx %d %d %d\n", qp_attr.qp_state, qp_attr.cur_qp_state, qp_attr.dest_qp_num);
//    }


    ready_running = 1;
    //DEBUG("N%lldT%lld stop processing\n", node_id, thd_id);
    //while(!start_running);
//    printf("1 %d\n", stop_running);
    while(1){
        //DEBUG("N%lldT%lld %lld\n", node_id, thd_id, start_running);
//        printf("x %d\n", start_running);
        if( start_running ){
            break;
        }
        barrier();
//        else sched_yield();
    }
    barrier();
    MessageControlItem item;
#ifdef __gperf
    ProfilerStart("test2.prof");
#endif
//    printf("3 %d\n", stop_running);
    while(!stop_running){

        // collect request
        for( ll kk = 0; kk < repeat_poll_num; kk ++ ) {
            for( ll ss = 0; ss < remote_poll_num; ss ++ ) {
                ll cnt = 0;
                int num = ibv_poll_cq(rdma->recv_cq, poll_size, wc_array);
                if (num > 0) {
//            DEBUG("recv ack %d\n", num);
                    ctrl_intvl += num;
                    ctrl_cnt++;
                    for (int k = 0; k < num; k++) {
                        struct ibv_wc *wc = &wc_array[k];
                        if (wc->opcode == IBV_WC_RECV || wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
                            if (wc->status != IBV_WC_SUCCESS) {
                                fprintf(stdout, "N%lldT%lld recv error %lld!\n", node_id, thd_id, wc->wr_id);
                                continue;
                            }
                            //execute
                            ll _node_id = wc->wr_id % node_cnt, _recv_id = (wc->wr_id / node_cnt) % max_cq_num;
//                    DEBUG("recv %lld %lld %lld\n", wc->wr_id, _node_id, _recv_id);
                            if( batch_send ){
                                char *ptr = (char*)(rdma->rdma_recv_region[_node_id] + _recv_id * rdma_buffer_size);
                                ll txnum = 0;
                                char *now = ptr;
                                while(1){
                                    exec_phase2(_node_id, now);
                                    Message *Msg = (Message *)now;
                                    if( Msg->message_type == ACK ){
                                        now += sizeof(AckMessage);
                                        if( Msg->ordering_ts == 1 ) break;
                                    }else if( Msg->message_type == CMT ){
                                        now += sizeof(CommitMessage);
                                        if( Msg->ordering_ts == 1 ) break;
                                    }else if( Msg->message_type == QUERY ){
                                        now += sizeof(QueryMessage);
                                        if( Msg->ordering_ts == 1 ) break;
                                    }else{
                                        assert( 1 == 0 );
                                    }
                                    txnum ++;
                                    assert( txnum <= txn_size );
                                }
                                remote_req++;
                            }else {
                                exec_phase2(_node_id, rdma->rdma_recv_region[_node_id] + _recv_id * rdma_buffer_size);
                                remote_req++;
                            }
                            // post recv again
                            //deal_with_recv( wc->wr_id );
                            rdma->recv(_node_id, _recv_id * node_cnt + _node_id,
                                       rdma->rdma_recv_region[_node_id] + _recv_id * rdma_buffer_size,
                                       rdma_buffer_size);
                        }
                    }
                }else break;
            }


            ll tmp = 0;
            for (ll s = 0; s < local_poll_num; s++) {
                ll msg_id = pending_msg_q->try_get();
                if (msg_id != -1) {
                        exec_phase2(node_id, &local_msg[msg_id * rdma_buffer_size]);
                        free_msg_q->push(msg_id);
                        local_req ++;
                        tmp ++;
                } else break;

            }
            if( tmp != 0 ){
                c2_cnt ++;
                c2_intvl += tmp;
            }
            for(ll s = 0; s < ret_read_poll_num; s ++) {
                ll upd_rd_id = shared_upd_read->lock_try_get();
                if( upd_rd_id != -1 ){
                    return_read( upd_rd_id );
                }else break;
            }
            for( ll s = 0; s < ret_read_poll_num; s++ ){
                if( ins_read.front == ins_read.tail ) break;
                ll ts = get_timestamp();
                if( read_req_q_pool[ ins_read.qe[ ins_read.tail%ins_read.cnt ]].start_ts < ts ){
                    ll new_id = ins_read.get();
                    assert( new_id != -1 );
                    table.insert_read_req( thd_id, &read_req_q_pool[new_id], new_id );
                }else break;
            }
            while( client.req_issued_cnt < client.req_cnt
            && client.req_issued_tot < total_issued_cnt_per_thd ){
                start_txn();
            }
        }

        // execute querys
        if( reordering != 9 ) {
            MessageControlItem item;
            for (ll s = 0; s < 32; s++) {
                mctrl.pop(&item);
                if (item.ts == -11) break;
                if (item.local == true) {
                    exec_phase2(node_id, &local_msg[item.id * rdma_buffer_size]);
                    free_msg_q->push(item.id);
                } else {
                    ll _node_id = item.id % node_cnt, _recv_id = (item.id / node_cnt) % max_cq_num;
                    exec_phase2(_node_id, rdma->rdma_recv_region[_node_id] + _recv_id * rdma_buffer_size);
//                deal_with_recv( item.id );
                    rdma->recv(_node_id, _recv_id * node_cnt + _node_id,
                               rdma->rdma_recv_region[_node_id] + _recv_id * rdma_buffer_size,
                               rdma_buffer_size);

                }
            }
        }

        // other operations

        int num = ibv_poll_cq(rdma->send_cq, poll_size, wc_array);
        if(num > 0){
            //DEBUG("send ack %d\n", num);
            for( int k = 0; k < num; k ++ ){
                struct ibv_wc* wc = &wc_array[k];
                if( wc->opcode == IBV_WC_SEND ){
                    if( wc->status != IBV_WC_SUCCESS ){
                        fprintf(stdout, "N%lldT%lld send error %lld!\n", node_id, thd_id, wc->wr_id);
                        continue;
                    }
                    ll res = wc->wr_id;
                    ll id = res%node_cnt, cnt = res/node_cnt;

                    send_finished[id] = cnt;
                }
            }
        }

//        task_id = request_retry_queue->try_get();
//        if( task_id != -1 ){
//            DEBUG("N%lldT%lld retry req task %lld\n",
//                  node_id, thd_id, task_id);
//            start_txn( task_id );
//        }

    }
#ifdef __gperf
    ProfilerStop();
#endif
    //printf("%lld: %lld %lld %lld %lld\n", thd_id, key[0], key[1], key[2], key[3]);

}

Txn_manager * WorkerThread::start_txn(){
    ll txn_manager_id = txn->get(txn_cnt++ );// here txn_manager status -> 1
    Txn_manager * mag = &txn->txn_table[txn_manager_id];
    assert(mag->txn_id != -1);
    Request* rq = client.get_req( txn_manager_id );

    mag->req = rq;
//    mag->req = &req_table[task_id];

    if( record_flag ){txn_executed ++;}

    mag->start_timestamp = get_timestamp();
    if( mag->req->need_retry == false ){
        mag->req->st = mag->start_timestamp;
    }

    mag->start(0);
//    ll tsmp = 0;
    ll tsmp = get_thread_timestamp();
    mag->end(0);

    mag->timestamp = tsmp;
    mag->issued_timestamp = get_timestamp();


    for( ll i = 0; i < mag->req->writereq.size(); i ++ )
        mag->write_flag[i] = 0;
    for( ll i = 0; i < mag->req->rmwreq.size(); i ++ )
        mag->rmw_flag[i] = 0;
    DEBUG("Txn%lld get %lld\n", mag->txn_mg_id, mag->txn_id);

//    mag->start(1);
    switch_txn_status( mag );
}

//该函数用于在每个执行阶段的末尾，确定是否转入下一阶段，并开始做下一阶段要做的事情
void WorkerThread::switch_txn_status( Txn_manager *mag ){
    if( mag->txn_id == -1 ) return ;

    if( mag->status == 1 ){
        mag->status = 2;
        // deal with read reqs

        ll ordering_ts = mag->timestamp;

        char *node_begin[20], *node_now[20];
        ll req_cnt_cal[20], send_tmp_rec[20];
        if( batch_send ) {
            for (ll i = 0; i < node_cnt; i++) {
                node_begin[i] = node_now[i] = NULL;
                req_cnt_cal[i] = 0;
            }
        }

        for( ll i = 0; i < mag->req->readreq.size(); i ++ ){
            ll target_node_id = mag->req->readreq[i].node_id;
            assert( target_node_id < node_cnt );
            if( target_node_id == my_node_id ){
                DEBUG("N%lldT%lld Txn%lld %lld send local read req\n",
                      node_id, thd_id, mag->txn_mg_id, mag->txn_id);
//                mag->req->readreq[i].output();

                ll msg_id = free_msg_q->get();
                QueryMessage *qmsg = (QueryMessage *)(&local_msg[msg_id*rdma_buffer_size]);
                qmsg->init( QUERY, READ, mag->timestamp, mag->txn_mg_id,
                            mag->txn_id,
                            mag->req->readreq[i].key, mag->req->readreq[i].sz,
                            mag->req->readreq[i].value, i,
                            node_id, thd_id, 1 );

                pending_msg_q->push(msg_id);
                continue;
            }
            if( !batch_send ) {
                while (send_cnt[target_node_id] < send_finished[target_node_id]);
                ll send_id = send_cnt[target_node_id]++;
                ll send_tmp = send_id;
                send_id %= max_cq_num;
                char *ptr = &rdma->rdma_send_region[target_node_id][send_id * rdma_buffer_size];
                QueryMessage *msg = (QueryMessage *) ptr;
                msg->init(QUERY, READ, mag->timestamp, mag->txn_mg_id,
                          mag->txn_id,
                          mag->req->readreq[i].key, mag->req->readreq[i].sz,
                          mag->req->readreq[i].value, i,
                          node_id, thd_id, 0);
                DEBUG("N%lldT%lld Txn%lld %lld send remote read req to N%lld\n",
                      node_id, thd_id, mag->txn_mg_id, mag->txn_id, target_node_id);
//            mag->req->readreq[i].output();

                //DEBUG("send id %lld send fin %lld\n", send_id, send_finished[target_node_id]);

                rdma->send(target_node_id, send_tmp * node_cnt + target_node_id,
                           ptr, sizeof(QueryMessage), 0);
            }else{
                assert( req_cnt_cal[target_node_id] != -1 );
                if( node_begin[target_node_id] == NULL ){
                    while (send_cnt[target_node_id] < send_finished[target_node_id]);
                    ll send_id = send_cnt[target_node_id]++;
                    ll send_tmp = send_id;
                    send_tmp_rec[target_node_id] = send_tmp;
                    send_id %= max_cq_num;
                    char *ptr = &rdma->rdma_send_region[target_node_id][send_id * rdma_buffer_size];
                    node_begin[target_node_id] = ptr;
                    node_now[target_node_id] = node_begin[target_node_id];
                    QueryMessage *msg = (QueryMessage *) node_now[target_node_id];
                    msg->init(QUERY, READ, mag->timestamp, mag->txn_mg_id,
                              mag->txn_id,
                              mag->req->readreq[i].key, mag->req->readreq[i].sz,
                              mag->req->readreq[i].value, i,
                              node_id, thd_id, mag->req->readreq[i].last_one);
                    node_now[target_node_id] += sizeof(QueryMessage);
                    req_cnt_cal[target_node_id] ++;
                }else{
                    QueryMessage *msg = (QueryMessage *) node_now[target_node_id];
                    msg->init(QUERY, READ, mag->timestamp, mag->txn_mg_id,
                              mag->txn_id,
                              mag->req->readreq[i].key, mag->req->readreq[i].sz,
                              mag->req->readreq[i].value, i,
                              node_id, thd_id, mag->req->readreq[i].last_one);
                    node_now[target_node_id] += sizeof(QueryMessage);
                    req_cnt_cal[target_node_id] ++;

                }
                if( mag->req->readreq[i].last_one ){
                    rdma->send(target_node_id, send_tmp_rec[target_node_id] * node_cnt\
                        + target_node_id, node_begin[target_node_id],
                               req_cnt_cal[target_node_id]*sizeof(QueryMessage), 0);

                    req_cnt_cal[target_node_id] = -1;
                }
            }
            if( record_flag ){issued_req_cnt ++;}
        }

        // deal with rmw reqs
        // maybe need a new ordering ts

        if( batch_send ) {
            for (ll i = 0; i < node_cnt; i++) {
                node_begin[i] = node_now[i] = NULL;
                req_cnt_cal[i] = 0;
            }
        }
        for( ll i = 0; i < mag->req->rmwreq.size(); i ++ ){
            ll target_node_id = mag->req->rmwreq[i].node_id;
            assert( target_node_id < node_cnt );
            if( target_node_id == my_node_id ){
                DEBUG("N%lldT%lld Txn%lld %lld send local rmw req\n",
                      node_id, thd_id, mag->txn_mg_id, mag->txn_id);
//                mag->req->rmwreq[i].output();

                ll msg_id = free_msg_q->get();
                QueryMessage *qmsg = (QueryMessage *)(&local_msg[msg_id*rdma_buffer_size]);
                qmsg->init( QUERY, RMW, mag->timestamp, mag->txn_mg_id,
                            mag->txn_id,
                            mag->req->rmwreq[i].key, mag->req->rmwreq[i].sz,
                            mag->req->rmwreq[i].value, i,
                            node_id, thd_id, 1 );

                pending_msg_q->push(msg_id);
                continue;
            }
            if( !batch_send ) {
                while (send_cnt[target_node_id] < send_finished[target_node_id]);
                ll send_id = send_cnt[target_node_id]++;
                ll send_tmp = send_id;
                send_id %= max_cq_num;
                char *ptr = &rdma->rdma_send_region[target_node_id][send_id * rdma_buffer_size];
                QueryMessage *msg = (QueryMessage *) ptr;
                msg->init(QUERY, RMW, mag->timestamp, mag->txn_mg_id,
                          mag->txn_id,
                          mag->req->rmwreq[i].key, mag->req->rmwreq[i].sz,
                          mag->req->rmwreq[i].value, i,
                          node_id, thd_id, 0);
                DEBUG("N%lldT%lld Txn%lld %lld send remote rmw req to N%lld\n",
                      node_id, thd_id, mag->txn_mg_id, mag->txn_id, target_node_id);
//            mag->req->rmwreq[i].output();

                rdma->send(target_node_id, send_tmp * node_cnt + target_node_id,
                           ptr, sizeof(QueryMessage), 0);
            }else{
                assert( req_cnt_cal[target_node_id] != -1 );
                if( node_begin[target_node_id] == NULL ){
                    while (send_cnt[target_node_id] < send_finished[target_node_id]);
                    ll send_id = send_cnt[target_node_id]++;
                    ll send_tmp = send_id;
                    send_tmp_rec[target_node_id] = send_tmp;
                    send_id %= max_cq_num;
                    char *ptr = &rdma->rdma_send_region[target_node_id][send_id * rdma_buffer_size];
                    node_begin[target_node_id] = ptr;
                    node_now[target_node_id] = node_begin[target_node_id];
                    QueryMessage *msg = (QueryMessage *) node_now[target_node_id];
                    msg->init(QUERY, RMW, mag->timestamp, mag->txn_mg_id,
                              mag->txn_id,
                              mag->req->rmwreq[i].key, mag->req->rmwreq[i].sz,
                              mag->req->rmwreq[i].value, i,
                              node_id, thd_id, mag->req->rmwreq[i].last_one);
                    node_now[target_node_id] += sizeof(QueryMessage);
                    req_cnt_cal[target_node_id] ++;
                }else{
                    QueryMessage *msg = (QueryMessage *) node_now[target_node_id];
                    msg->init(QUERY, RMW, mag->timestamp, mag->txn_mg_id,
                              mag->txn_id,
                              mag->req->rmwreq[i].key, mag->req->rmwreq[i].sz,
                              mag->req->rmwreq[i].value, i,
                              node_id, thd_id, mag->req->rmwreq[i].last_one);
                    node_now[target_node_id] += sizeof(QueryMessage);
                    req_cnt_cal[target_node_id] ++;

                }
                if( mag->req->rmwreq[i].last_one ){
                    rdma->send(target_node_id, send_tmp_rec[target_node_id] * node_cnt\
                        + target_node_id, node_begin[target_node_id],
                               req_cnt_cal[target_node_id]*sizeof(QueryMessage), 0);

                    req_cnt_cal[target_node_id] = -1;
                }
            }
            if( record_flag ){issued_req_cnt ++;}
        }

        if( batch_send ){

        }


        if( mag->req->readreq.size() == 0 && mag->req->rmwreq.size() == 0 ){
            switch_txn_status( mag );
        }
    }else if( mag->status == 2 ){
        if( mag->rd_complt_cnt == mag->req->readreq.size() &&
                mag->rmw_complt_cnt == mag->req->rmwreq.size() ){
//            DEBUG("Txn%lld enter write phase\n", mag->txn_id);
            mag->status = 3;

            ll ordering_ts = mag->start_timestamp;

            char *node_begin[20], *node_now[20];
            ll req_cnt_cal[20], send_tmp_rec[20];
            if( batch_send ) {
                for (ll i = 0; i < node_cnt; i++) {
                    node_begin[i] = node_now[i] = NULL;
                    req_cnt_cal[i] = 0;
                }
            }
            for( ll i = 0; i < mag->req->writereq.size(); i ++ ){
                ll target_node_id = mag->req->writereq[i].node_id;
                assert( target_node_id < node_cnt );
                if( target_node_id == my_node_id ){
                    DEBUG("N%lldT%lld Txn%lld %lld send local write req\n",
                          node_id, thd_id, mag->txn_mg_id, mag->txn_id);
//                    mag->req->writereq[i].output();

                    ll msg_id = free_msg_q->get();
                    QueryMessage *qmsg = (QueryMessage *)(&local_msg[msg_id*rdma_buffer_size]);
                    qmsg->init( QUERY, WRITE, mag->timestamp, mag->txn_mg_id,
                                mag->txn_id,
                                mag->req->writereq[i].key, mag->req->writereq[i].sz,
                                mag->req->writereq[i].value, i,
                                node_id, thd_id, 1 );

                    pending_msg_q->push(msg_id);
                    continue;
                }
                if( !batch_send ) {
                    while (send_cnt[target_node_id] < send_finished[target_node_id]);
                    ll send_id = send_cnt[target_node_id]++;
                    ll send_tmp = send_id;
                    send_id %= max_cq_num;
                    char *ptr = &rdma->rdma_send_region[target_node_id][send_id * rdma_buffer_size];
                    QueryMessage *msg = (QueryMessage *) ptr;
                    msg->init(QUERY, WRITE, mag->timestamp, mag->txn_mg_id,
                              mag->txn_id,
                              mag->req->writereq[i].key, mag->req->writereq[i].sz,
                              mag->req->writereq[i].value, i,
                              node_id, thd_id, 1);

                    DEBUG("N%lldT%lld Txn%lld %lld send remote write req to N%lld\n",
                          node_id, thd_id, mag->txn_mg_id, mag->txn_id, target_node_id);
//                mag->req->writereq[i].output();

                    rdma->send(target_node_id, send_tmp * node_cnt + target_node_id,
                               ptr, sizeof(QueryMessage), 0);
                }else{
                    assert( req_cnt_cal[target_node_id] != -1 );
                    if( node_begin[target_node_id] == NULL ){
                        while (send_cnt[target_node_id] < send_finished[target_node_id]);
                        ll send_id = send_cnt[target_node_id]++;
                        ll send_tmp = send_id;
                        send_tmp_rec[target_node_id] = send_tmp;
                        send_id %= max_cq_num;
                        char *ptr = &rdma->rdma_send_region[target_node_id][send_id * rdma_buffer_size];
                        node_begin[target_node_id] = ptr;
                        node_now[target_node_id] = node_begin[target_node_id];
                        QueryMessage *msg = (QueryMessage *) node_now[target_node_id];
                        msg->init(QUERY, WRITE, mag->timestamp, mag->txn_mg_id,
                                  mag->txn_id,
                                  mag->req->writereq[i].key, mag->req->writereq[i].sz,
                                  mag->req->writereq[i].value, i,
                                  node_id, thd_id, mag->req->writereq[i].last_one);
                        node_now[target_node_id] += sizeof(QueryMessage);
                        req_cnt_cal[target_node_id] ++;
                    }else{
                        QueryMessage *msg = (QueryMessage *) node_now[target_node_id];
                        msg->init(QUERY, WRITE, mag->timestamp, mag->txn_mg_id,
                                  mag->txn_id,
                                  mag->req->writereq[i].key, mag->req->writereq[i].sz,
                                  mag->req->writereq[i].value, i,
                                  node_id, thd_id, mag->req->writereq[i].last_one);
                        node_now[target_node_id] += sizeof(QueryMessage);
                        req_cnt_cal[target_node_id] ++;

                    }
                    if( mag->req->writereq[i].last_one ){
                        ll *lltr = (ll *)node_begin[target_node_id];
                        (*lltr) = req_cnt_cal[target_node_id];
                        rdma->send(target_node_id, send_tmp_rec[target_node_id] * node_cnt\
                        + target_node_id, node_begin[target_node_id],
                                   req_cnt_cal[target_node_id]*sizeof(QueryMessage), 0);

                        req_cnt_cal[target_node_id] = -1;
                    }
                }
                if( record_flag ){issued_req_cnt ++;}
            }

            if( mag->req->writereq.size() == 0 ){
                switch_txn_status( mag );
            }
        }else{
            return ;
        }
    }else if( mag->status == 3 ){
        if( mag->wt_complt_cnt == mag->req->writereq.size() ){
//            DEBUG("Txn%lld enter commit phase\n", mag->txn_id);
            mag->status = 4;
            // time to commit
            // commit will not fail

            commit_txn(mag);
        }
    }else if( mag->status == 4 ){

    }
}

void WorkerThread::commit_txn( Txn_manager *mag ){
    send_cmt_msg( mag, true );
    // assume commit succeed
    // recycle resources
    if(record_flag) txn_finished++;

//    if( txn_finished%10000 == 0 ) printf("T%lld %lld\n", thd_id, txn_finished);

    mag->end_timestamp = get_timestamp();
    mag->req->ed = mag->end_timestamp;
    mag->req->lat = mag->req->ed-mag->req->st;
    mag->req->need_retry = false;
    DEBUG("N%lldT%lld Txn%lld id %lld commit!\n", node_id, thd_id, mag->txn_mg_id, mag->txn_id);
//            DEBUG("%lld %lld\n", txn->txn_table[txn_manager_id].issued_timestamp-txn->txn_table[txn_manager_id].start_timestamp,\
//                    txn->txn_table[txn_manager_id].end_timestamp-txn->txn_table[txn_manager_id].issued_timestamp);

    if(record_flag) lat_txn_finished += mag->end_timestamp-mag->start_timestamp;
    lat_get_ts += mag->ret_res(0);
    if( workload == 1 ){
        if( mag->req->req_type == 1 ){
            rd_txn_finished ++;
            rd_intvl += mag->last_complt_rd-mag->first_complt_rd;
        }else if( mag->req->req_type == 2 ){
            rmw_txn_finished ++;
            rmw_intvl += mag->last_complt_rmw-mag->first_complt_rmw;
        }
    }else if( workload == 2 ){
        if( mag->req->req_type == 1 ){
            rd_txn_finished ++;
            rd_intvl += mag->last_complt_rd-mag->first_complt_rd;
        }else if( mag->req->req_type == 2 ){
            rmw_txn_finished ++;
            rmw_intvl += mag->last_complt_rmw-mag->first_complt_rmw;
        }
    }

//    request_callback_queue->push(mag->req->req_order);
    client.recycle(mag->req);

    txn->recycle(mag->txn_mg_id);

}

void WorkerThread::abort_txn( Txn_manager *mag ) {
    send_cmt_msg( mag, false );
    DEBUG("N%lldT%lld Txn%lld id %lld abort!\n", node_id, thd_id,\
    mag->txn_mg_id, mag->txn_id);

    if( record_flag ){txn_aborted ++;}
    //to avoid deal with abort txns frequently
    mag->req->abort_cnt ++;
    mag->req->need_retry = true;

    //request_retry_queue->push(mag->req->req_id);
//    request_callback_queue->push(mag->req->req_order);
    client.recycle(mag->req);

    txn->recycle(mag->txn_mg_id);


}

void WorkerThread::send_cmt_msg( Txn_manager *mag, bool s ) {
    if( commit_phase == 0 ) return ;
    if( mvcc_type == 2 ){
//        ll ordering_ts = mag->start_timestamp+1000;
        ll ordering_ts = get_thread_timestamp();
        if( batch_send ) {
            char *node_begin[20], *node_now[20];
            ll req_cnt_cal[20], send_tmp_rec[20];
            for (ll i = 0; i < node_cnt; i++) {
                node_begin[i] = node_now[i] = NULL;
                req_cnt_cal[i] = 0;
            }
            for (ll i = 0; i < mag->req->writereq.size(); i++) {
                txn_item *item = &mag->req->writereq[i];
                ll target_node_id = item->node_id;
                assert( target_node_id < node_cnt );
                if( target_node_id == my_node_id ){
                    ll msg_id = free_msg_q->get();
                    CommitMessage *cmsg = (CommitMessage *)(&local_msg[msg_id*rdma_buffer_size]);

                    cmsg->init( CMT, WRITE, s, my_node_id,thd_id,
                                mag->txn_mg_id, mag->txn_id,
                                item->key, 1, -1,
                                mag->timestamp, item->req_id,
                                item->req_v_id, 1);

                    pending_msg_q->push(msg_id);
                    continue;
                }

                assert( req_cnt_cal[target_node_id] != -1 );
                if( node_begin[target_node_id] == NULL ){
                    while (send_cnt[target_node_id] < send_finished[target_node_id]);
                    ll send_id = send_cnt[target_node_id]++;
                    ll send_tmp = send_id;
                    send_tmp_rec[target_node_id] = send_tmp;
                    send_id %= max_cq_num;
                    char *ptr = &rdma->rdma_send_region[target_node_id][send_id * rdma_buffer_size];
                    node_begin[target_node_id] = ptr;
                    node_now[target_node_id] = node_begin[target_node_id];

                    CommitMessage *cmsg = (CommitMessage *)node_now[target_node_id];
                    cmsg->init( CMT, WRITE, s, my_node_id, thd_id,
                                mag->txn_mg_id, mag->txn_id,
                                item->key, 1, -1,
                                mag->timestamp, item->req_id,
                                item->req_v_id, mag->req->writereq[i].last_one);

                    node_now[target_node_id] += sizeof(CommitMessage);
                    req_cnt_cal[target_node_id] ++;
                }else{
                    CommitMessage *cmsg = (CommitMessage *)node_now[target_node_id];
                    cmsg->init( CMT, WRITE, s, my_node_id, thd_id,
                                mag->txn_mg_id, mag->txn_id,
                                item->key, 1, -1,
                                mag->timestamp, item->req_id,
                                item->req_v_id, mag->req->writereq[i].last_one);
                    node_now[target_node_id] += sizeof(CommitMessage);
                    req_cnt_cal[target_node_id] ++;

                }
                if( mag->req->writereq[i].last_one ){
                    rdma->send(target_node_id, send_tmp_rec[target_node_id] * node_cnt\
                        + target_node_id, node_begin[target_node_id],
                               req_cnt_cal[target_node_id]*sizeof(CommitMessage), 0);

                    req_cnt_cal[target_node_id] = -1;
                    if( s == true ) {
                        DEBUG("N%lldT%lld Txn%lld %lld send remote write commit to N%lld\n",
                              node_id, thd_id, mag->txn_mg_id, mag->txn_id, target_node_id);
                        //            mag->req->writereq[i].output();
                    }else{
                        DEBUG("N%lldT%lld Txn%lld %lld send remote write abort to N%lld\n",
                              node_id, thd_id, mag->txn_mg_id, mag->txn_id, target_node_id);
                        //            mag->req->writereq[i].output();
                    }
                }
            }
            for (ll i = 0; i < node_cnt; i++) {
                node_begin[i] = node_now[i] = NULL;
                req_cnt_cal[i] = 0;
            }
            for (ll i = 0; i < mag->req->rmwreq.size(); i++) {
                txn_item *item = &mag->req->rmwreq[i];
                ll target_node_id = item->node_id;
                assert( target_node_id < node_cnt );
                if( target_node_id == my_node_id ){
                    ll msg_id = free_msg_q->get();
                    CommitMessage *cmsg = (CommitMessage *)(&local_msg[msg_id*rdma_buffer_size]);

                    cmsg->init( CMT, WRITE, s, my_node_id,thd_id,
                                mag->txn_mg_id, mag->txn_id,
                                item->key, 1, -1,
                                mag->timestamp, item->req_id,
                                item->req_v_id, 0);

                    pending_msg_q->push(msg_id);
                    continue;
                }

                assert( req_cnt_cal[target_node_id] != -1 );
                if( node_begin[target_node_id] == NULL ){
                    while (send_cnt[target_node_id] < send_finished[target_node_id]);
                    ll send_id = send_cnt[target_node_id]++;
                    ll send_tmp = send_id;
                    send_tmp_rec[target_node_id] = send_tmp;
                    send_id %= max_cq_num;
                    char *ptr = &rdma->rdma_send_region[target_node_id][send_id * rdma_buffer_size];
                    node_begin[target_node_id] = ptr;
                    node_now[target_node_id] = node_begin[target_node_id];

                    CommitMessage *cmsg = (CommitMessage *)node_now[target_node_id];
                    cmsg->init( CMT, WRITE, s, my_node_id, thd_id,
                                mag->txn_mg_id, mag->txn_id,
                                item->key, 1, -1,
                                mag->timestamp, item->req_id,
                                item->req_v_id, mag->req->rmwreq[i].last_one);

                    node_now[target_node_id] += sizeof(CommitMessage);
                    req_cnt_cal[target_node_id] ++;
                }else{
                    CommitMessage *cmsg = (CommitMessage *)node_now[target_node_id];
                    cmsg->init( CMT, WRITE, s, my_node_id, thd_id,
                                mag->txn_mg_id, mag->txn_id,
                                item->key, 1, -1,
                                mag->timestamp, item->req_id,
                                item->req_v_id, mag->req->rmwreq[i].last_one);
                    node_now[target_node_id] += sizeof(CommitMessage);
                    req_cnt_cal[target_node_id] ++;

                }
                if( mag->req->rmwreq[i].last_one ){
                    rdma->send(target_node_id, send_tmp_rec[target_node_id] * node_cnt\
                        + target_node_id, node_begin[target_node_id],
                               req_cnt_cal[target_node_id]*sizeof(CommitMessage), 0);

                    req_cnt_cal[target_node_id] = -1;
                    if( s == true ) {
                        DEBUG("N%lldT%lld Txn%lld %lld send remote write commit to N%lld\n",
                              node_id, thd_id, mag->txn_mg_id, mag->txn_id, target_node_id);
                        //            mag->req->writereq[i].output();
                    }else{
                        DEBUG("N%lldT%lld Txn%lld %lld send remote write abort to N%lld\n",
                              node_id, thd_id, mag->txn_mg_id, mag->txn_id, target_node_id);
                        //            mag->req->writereq[i].output();
                    }
                }
            }


        }else {

            for (ll i = 0; i < mag->req->writereq.size(); i++) {
                if (mag->write_flag[i]) continue;
                per_send_cmt_msg(mag, s, mag->req, &mag->req->writereq[i], ordering_ts);
            }
            for (ll i = 0; i < mag->req->rmwreq.size(); i++) {
                if (mag->rmw_flag[i]) continue;
                per_send_cmt_msg(mag, s, mag->req, &mag->req->rmwreq[i], ordering_ts);
            }
        }
    }
}

void WorkerThread::per_send_cmt_msg( Txn_manager *mag, bool s, Request* req, txn_item *item, ll o_ts ){

    ll target_node_id = item->node_id;
    assert( target_node_id < node_cnt );
    if( target_node_id == my_node_id ){
        if( s == true ) {
            DEBUG("N%lldT%lld Txn%lld %lld send local write commit\n",
                  node_id, thd_id, mag->txn_mg_id, mag->txn_id);
        }else{
            DEBUG("N%lldT%lld Txn%lld %lld send local write abort\n",
                  node_id, thd_id, mag->txn_mg_id, mag->txn_id);
        }

        ll msg_id = free_msg_q->get();
        CommitMessage *cmsg = (CommitMessage *)(&local_msg[msg_id*rdma_buffer_size]);

        cmsg->init( CMT, WRITE, s, my_node_id,thd_id,
                    mag->txn_mg_id, mag->txn_id,
                    item->key, 1, -1,
                    mag->timestamp, item->req_id,
                    item->req_v_id, 1);

        pending_msg_q->push(msg_id);
//                table.mvcc_commit_write( cmsg, thd_id );

        return;
    }

    while( send_cnt[target_node_id] < send_finished[target_node_id] );
    ll send_id = send_cnt[target_node_id]++;
    ll send_tmp = send_id;
    send_id %= max_cq_num;
    char *ptr = &rdma->rdma_send_region[target_node_id][send_id*rdma_buffer_size];
    CommitMessage *cmsg = (CommitMessage *)ptr;
    cmsg->init( CMT, WRITE, s, my_node_id, thd_id,
                mag->txn_mg_id, mag->txn_id,
                item->key, 1, -1,
                mag->timestamp, item->req_id,
                item->req_v_id, 1);

    if( s == true ) {
        DEBUG("N%lldT%lld Txn%lld %lld send remote write commit to N%lld\n",
              node_id, thd_id, mag->txn_mg_id, mag->txn_id, target_node_id);
        //            mag->req->writereq[i].output();
    }else{
        DEBUG("N%lldT%lld Txn%lld %lld send remote write abort to N%lld\n",
              node_id, thd_id, mag->txn_mg_id, mag->txn_id, target_node_id);
        //            mag->req->writereq[i].output();
    }

    rdma->send( target_node_id, send_tmp*node_cnt+target_node_id,
                ptr, sizeof(CommitMessage), 0 );
//            INC(issued_req_cnt)
}

int WorkerThread::exec_phase2( ll source_id, char* msg_buffer ){
    Message *Msg = (Message *)msg_buffer;
    if( Msg->message_type == ACK ) {
        AckMessage *msg = (AckMessage *) msg_buffer;
        DEBUG("N%lldT%lld recv ACK from N%lld\n", node_id, thd_id, source_id);
        if( thd_id == 0 )
        msg->output();

        ll txn_manager_id = msg->txn_mg_id;
        Txn_manager * mag = &txn->txn_table[txn_manager_id];


        //deal with reply whose txn has been aborted
        if( msg->txn_id != mag->txn_id ){
//            if( my_node_id == source_id ) assert(1==0);
//            if( !msg->succeed )
//                INC(req_aborted)
            return -1;
        }

        if( msg->txn_id == mag->txn_id && msg->txn_id == -1 ){
            msg->output();
            mag->output();
            assert(1==0);
        }


//        DEBUG("N%lldT%lld recv ACK from N%lld\n", node_id, thd_id, source_id);
//        msg->output();

        // deal with failed ACK
        if( !msg->succeed ){
//            INC(req_aborted)

            if( msg->txn_id == mag->txn_id ) {
                //if( mvcc_type == 2 )
                if( msg->type == RMW )
                    mag->rmw_flag[msg->req_id] = 1;
                if( msg->type == WRITE )
                    mag->write_flag[msg->req_id] = 1;

                /*deal with abort*/
                abort_txn( mag );
            }
            return -1;
        }

        ll now_ts = get_timestamp();

        // deal with successful ACK
        if( mag->status == 2 ){
            if( msg->type == READ ) {
//                mag->rd_buffer = msg->value;

                if( mag->rd_complt_cnt == 0 )
                    mag->first_complt_rd = now_ts;
                mag->rd_complt_cnt ++;
                mag->req->readreq[ msg->req_id ].req_status = 1;
                if( mag->rd_complt_cnt == mag->req->readreq.size() )
                    mag->last_complt_rd = now_ts;
            }
            if( msg->type == RMW ){
//                mag->rd_buffer = msg->value;

                if( mag->rmw_complt_cnt == 0 )
                    mag->first_complt_rmw = now_ts;
                mag->rmw_complt_cnt ++;
                assert( msg->version_id != -1 );
                mag->req->rmwreq[ msg->req_id ].req_status = 1;
                mag->req->rmwreq[ msg->req_id ].req_v_id = msg->version_id;

                if( mag->rmw_complt_cnt == mag->req->rmwreq.size() )
                    mag->last_complt_rmw = now_ts;
            }
            switch_txn_status( mag );
        }else if( mag->status == 3 ){
            if( msg->type == WRITE ){
                if( mag->wt_complt_cnt == 0 )
                    mag->first_complt_wt = now_ts;

                mag->wt_complt_cnt ++;
                mag->req->writereq[ msg->req_id ].req_v_id = msg->version_id;

                if( mag->wt_complt_cnt == mag->req->writereq.size() )
                    mag->last_complt_wt = now_ts;
            }
            switch_txn_status( mag );
        }else if( mag->status == 4 ){
            // if we need commit ack, write here
        }
    }
    else if( Msg->message_type == QUERY ){
        QueryMessage *msg = (QueryMessage *) msg_buffer;

        DEBUG("RESPONDING N%lldT%lld recv RW %d query from N%lld ",
              node_id, thd_id, (int)msg->type, source_id);
//        DEBUG("RESPONDING N%lldT%lld recv RW %d query from N%lld\n",
//              node_id, thd_id, (int)msg->type, source_id);
//        if( thd_id == 0 )
        msg->output();

        if( msg->source_node_id == node_id ){
            ll ack_msg_id = free_msg_q->get();
            AckMessage *ack_msg = (AckMessage *)(&local_msg[ack_msg_id*rdma_buffer_size]);
            ack_msg->target_node_id = source_id;
            if( msg->type == READ ){
                table.read(msg, ack_msg, thd_id);
            }
            else if( msg->type == WRITE ){
                table.write(msg, ack_msg, thd_id);
            }else if( msg->type == RMW ) {
                table.rmw(msg, ack_msg, thd_id);
            }

            req_cnt ++;

            if( ( msg->type == READ || msg->type == RMW ) && ack_msg->wait == true ){
                //recycle
                DEBUG("\n");
                free_msg_q->push(ack_msg_id);
            }else {
                if( ack_msg->succeed == false ) req_aborted ++;
                // send
                DEBUG("return query above\n");
                pending_msg_q->push(ack_msg_id);
            }


        }else{
            while( send_cnt[source_id] < send_finished[source_id] );
            ll send_id = send_cnt[source_id];
            // not ++ if read needs to be blocked

            ll send_tmp = send_id;
            send_id %= max_cq_num;
            char *ptr = &rdma->rdma_send_region[source_id][send_id*rdma_buffer_size];
            AckMessage *ack_msg = (AckMessage *)ptr;

            ack_msg->target_node_id = source_id;



            if( msg->type == READ ){
                table.read(msg, ack_msg, thd_id);
            }
            else if( msg->type == WRITE ){
                table.write(msg, ack_msg, thd_id);
            }else if( msg->type == RMW ){
                table.rmw(msg, ack_msg, thd_id);
            }

            req_cnt ++;

            if( ( msg->type == READ || msg->type == RMW ) && ack_msg->wait == true ){
                DEBUG("\n");
            }else {
                if( ack_msg->succeed == false ) req_aborted ++;

                DEBUG("return query above\n");
                send_cnt[source_id] ++;

                rdma->send(source_id, send_tmp * node_cnt + source_id,
                           ptr, sizeof(AckMessage), 0);
            }
        }
        if( record_flag ){issued_req_cnt ++;}
    }
    else if( Msg->message_type == CMT ){
        CommitMessage *cmsg = (CommitMessage *) msg_buffer;
        DEBUG("N%lldT%lld recv CMT from N%lld\n", node_id, thd_id, source_id);
        cmsg->output();
        mvcc_commit_ret ret;
        table.mvcc_commit_write(cmsg, thd_id, &ret );
    }
    return 0;
}

void WorkerThread::return_read( ll key ){
    mvcc_commit_ret ret;
    table.update_read_req(key, thd_id, &ret );

    for( ll i = 0; i < ret.local.size(); i ++ ){
        pending_msg_q->push( ret.local[i] );
    }
    for( ll i = 0; i < ret.remote.size(); i ++ ){
        char *ptr = &rdma->rdma_send_region[ret.remote[i].first]\
            [ret.remote[i].second%max_cq_num*rdma_buffer_size];
        AckMessage *ack_msg = (AckMessage *)ptr;

        rdma->send(ret.remote[i].first, ret.remote[i].second * node_cnt + ret.remote[i].first,
                   ptr, sizeof(AckMessage), 0);
    }
}

void WorkerThread::deal_with_recv( ll id ){
    ll _node_id = id%node_cnt, recv_id = id/node_cnt;
    recv_finished[_node_id] = recv_id;
//    DEBUG("%lld: %lld  %lld\n", _node_id, recv_finished[_node_id], recv_cnt[_node_id] );
//    if( recv_cnt[_node_id]-recv_finished[_node_id] <= in_flight_recv_area ){
    if( recv_cnt[_node_id]-recv_finished[_node_id] <= in_flight_recv_area ){
        struct ibv_recv_wr *bad_wr = NULL;
        for( ll i = 0; i < one_epoch_recv_wr; i ++ ){
            rdma->wr[i].wr_id = recv_cnt[_node_id]*node_cnt+_node_id;
            rdma->wr[i].sg_list = &rdma->sge[i];
            rdma->wr[i].num_sge = 1;
            if( i != one_epoch_recv_wr-1 ){
                rdma->wr[i].next = &rdma->wr[i+1];
            }
            else rdma->wr[i].next = NULL;

            rdma->sge[i].addr = (uint64_t)( rdma->rdma_recv_region[_node_id]\
            +(recv_cnt[_node_id]%max_cq_num)*rdma_buffer_size );
            rdma->sge[i].length = rdma_buffer_size;
            rdma->sge[i].lkey = rdma->rdma_recv_mr[_node_id]->lkey;

//            DEBUG("N%lldT%lld s%lld post recv %lld\n",\
//            node_id, thd_id, _node_id, recv_cnt[_node_id]%max_cq_num);
            recv_cnt[_node_id] ++;
        }
        int ret = ibv_post_recv(rdma->qp[_node_id], &rdma->wr[0], &bad_wr);
        if( ret ) assert( 0 == 1 );
    }
}

void WorkerThread::output() {
    fprintf(stdout, "N%lldT%lld Txn finished %lld aborted %lld avg_lat %.2lf ", \
    node_id, thd_id, txn_finished, txn_aborted, (double)lat_txn_finished/txn_finished/1e3);
//    fprintf(stdout, "max_rd_intvl %.2lf max_rmw_intvl %.2lf %lld %lld\n", \
//    1.0*rd_intvl/rd_txn_finished/1e3, 1.0*rmw_intvl/rmw_txn_finished/1e3, rd_txn_finished, rmw_txn_finished );
//    fprintf(stdout, "avg ctrl %.2lf ctrl2 %.2lf\n", \
//    1.0*ctrl_intvl/ctrl_cnt, 1.0*c2_intvl/c2_cnt );
//    fprintf(stdout, "avg local_req %lld remote_req %lld local_ratio %.2lf %% \n", \
    local_req, remote_req, 100.0*local_req/(local_req+remote_req) );
    fprintf(stdout, "req_cnt %lld req_abt %lld ratio %.2lf %% \n", \
    req_cnt, req_aborted, 100.0*req_aborted/req_cnt );
    //    fprintf(stdout, "issued_req_cnt %lld recv_ack_cnt %lld recv_qry_cnt %lld\n", issued_req_cnt, recv_ack_cnt, recv_qry_cnt);
}

ll ClientGen::generate_task( ll txn_manager_id ) {
    ll k = 0;
    ll cnt = 0;
    for( ll i = 1; i <= prepared_req_per_thread; i ++ ) {
        if( req_tm_table[k][(i+last_generate_id)%prepared_req_per_thread].in_flight == 0 && (
        retry_txns == 0 ||
                req_tm_table[k][(i+last_generate_id)%prepared_req_per_thread].need_retry == 0 )
        ){
            req_tm_table[k][(i+last_generate_id)%prepared_req_per_thread].in_flight = 1;
            last_generate_id = (i+last_generate_id)%prepared_req_per_thread;
            return last_generate_id;
        }
        cnt ++;
        if( cnt >= prepared_req_per_thread/2 ){
            printf("find new reqs hard! need more\n");
        }
    }
    return -1;
}

ll WorkerThread::get_thread_timestamp(){
    return gt.get_timestamp();
}

ll ClientGen::generate_key()
{
    ll key;
    ll distributed = rand()%100;
    if( local_req_ratio == -1 ){
        key = table.zipf.gen();
//        assert( key != 0 );
        return key;
    }
    while(1){
        key = rand();
        //printf("%lld %lld %lld\n", thd_id, key, key%key_range);
        key = key%key_range;
        //                key = cnt%key_range; cnt ++;
        if( local_req_ratio != -1 ) {
            if (key / key_partition_range == my_node_id
                && distributed >= local_req_ratio)
                continue;
            if (key / key_partition_range != my_node_id
                && distributed < local_req_ratio)
                continue;
        }
        break;
    }
    return key;
}

void ClientGen::init( ll _thd_id ){
    req_cnt = txn_per_worker;
    req_issued_cnt = 0;
    req_issued_tot = 0;
    last_generate_id = 0;
    thd_id = _thd_id;

//    req_table = new Request[prepared_req_per_thread];
    retry_queue = new std::queue< std::pair<ll,ll> >;
    lat = new std::vector<ll>;
    abort_cnt = new std::vector<ll>;
    req_tm_table = (Request**)malloc( sizeof(Request *)*txn_per_worker );
    for( ll i = 0; i < txn_per_worker; i ++ ){
        req_tm_table[i] = new Request[prepared_req_per_thread];
    }

    for( ll j = 0; j < txn_per_worker; j ++ ){
        for( ll i = 0; i < prepared_req_per_thread; i ++ ) {
            req_tm_table[j][i].need_retry = false;
            req_tm_table[j][i].abort_cnt = 0;
        }
    }

    if( workload == 0 ){
        for( ll k = 0; k < 1; k ++ )
        for( ll i = 0; i < prepared_req_per_thread; i ++ ){
            req_tm_table[k][i].req_order = i;
            for( ll j = 0; j < txn_size; j ++ ){
                /*distributed guaranteed*/
                ll key = generate_key();
                ll value = rand();
                ll rw_ratio = rand()%100;
                if( rw_ratio < read_req_ratio )
                    req_tm_table[k][i].readreq.push_back(txn_item((ll)(key/key_partition_range), key, value, j, 1) );
                else
                    req_tm_table[k][i].writereq.push_back(txn_item((ll)(key/key_partition_range), key, value, j, 1) );
            }
        }
    }else if( workload == 1 ){
        for( ll k = 0; k < 1; k ++ ) {
            for (ll i = 0; i < prepared_req_per_thread; i++) {
                req_tm_table[k][i].req_order = i;
                Request *req = &req_tm_table[k][i];
                ll type = rand() % 100;
                if (type < read_snapshot_ratio) {
                    req->req_type = 1;
                    for (ll j = 0; j < txn_size; j++) {
                        /*distributed guaranteed*/
                        ll value = rand();
                        ll key;
                        while (1) {
                            key = generate_key();
                            int fl = 0;
                            for (ll k = 0; k < req->readreq.size(); k++) {
                                if (req->readreq[k].key == key) {
                                    fl = 1;
                                    break;
                                }
                            }
                            if (fl) continue;
                            break;
                        }
                        req->readreq.push_back(txn_item((ll) (key / key_partition_range), key, value, j, 1));
                    }
                } else {
                    req->req_type = 2;
                    for (ll j = 0; j < txn_size; j++) {
                        /*distributed guaranteed*/
                        ll value = rand();
                        ll key;
                        while (1) {
                            key = generate_key();
                            int fl = 0;
                            for (ll k = 0; k < req->readreq.size(); k++) {
                                if (req->readreq[k].key == key) {
                                    fl = 1;
                                    break;
                                }
                            }
                            if (fl) continue;
                            break;
                        }
                        req->readreq.push_back(txn_item((ll) (key / key_partition_range), key, value, j, 1));
                        req->writereq.push_back(txn_item((ll) (key / key_partition_range), key, value, j, 1));
                    }
                }
                if( order_request ){
                    std::sort( req->readreq.begin(), req->readreq.end(), cmp );
                    std::sort( req->writereq.begin(), req->writereq.end(), cmp );
                    std::sort( req->rmwreq.begin(), req->rmwreq.end(), cmp );
                }
                req->tag();
            }
        }
    }else if( workload == 2 ){
        for( ll k = 0; k < 1; k ++ ) {
            for (ll i = 0; i < prepared_req_per_thread; i++) {
                req_tm_table[k][i].req_order = i;
                Request *req = &req_tm_table[k][i];
                ll type = rand() % 100;
                if (type < read_snapshot_ratio) {
                    req->req_type = 1;
                    for (ll j = 0; j < txn_size; j++) {
                        /*distributed guaranteed*/
                        ll value = rand();
                        ll key;
                        while (1) {
                            key = generate_key();
                            int fl = 0;
                            for (ll k = 0; k < req->readreq.size(); k++) {
                                if (req->readreq[k].key == key) {
                                    fl = 1;
                                    break;
                                }
                            }
                            if (fl) continue;
                            break;
                        }
                        req->readreq.push_back(txn_item((ll) (key / key_partition_range), key, value, j, 1));
                    }
                } else {
                    req->req_type = 2;
                    for (ll j = 0; j < txn_size; j++) {
                        /*distributed guaranteed*/
                        ll value = rand();
                        ll key;
                        while (1) {
                            key = generate_key();
                            int fl = 0;
                            for (ll k = 0; k < req->rmwreq.size(); k++) {
                                if (req->rmwreq[k].key == key) {
                                    fl = 1;
                                    break;
                                }
                            }
                            if (fl) continue;
                            break;
                        }
                        req->rmwreq.push_back(txn_item((ll) (key / key_partition_range), key, value, j, 1));
                    }
                }
                if( order_request ){
                    std::sort( req->readreq.begin(), req->readreq.end(), cmp );
                    std::sort( req->writereq.begin(), req->writereq.end(), cmp );
                    std::sort( req->rmwreq.begin(), req->rmwreq.end(), cmp );
                }
                req->tag();
            }
        }
    }else if( workload == 3 ){
        tpcc.generate_req(&worker_thds[thd_id]);
    }else if( workload == 4 ){
        for( ll k = 0; k < 1; k ++ ) {
            for (ll i = 0; i < prepared_req_per_thread; i++) {
                req_tm_table[k][i].req_order = i;
                Request *req = &req_tm_table[k][i];
                for (ll j = 0; j < txn_size; j++) {
                    ll type = rand() % 100;
                    std::vector<txn_item> *vec;
                    if (type < read_snapshot_ratio) {
                        vec = &req->readreq;
                    } else {
                        vec = &req->rmwreq;
                    }
                    ll value = rand();
                    ll key;
                    while (1) {
                        key = generate_key();
                        int fl = 0;
                        for (ll k = 0; k < vec->size(); k++) {
                            if (vec->at(k).key == key) {
                                fl = 1;
                                break;
                            }
                        }
                        if (fl) continue;
                        break;
                    }
                    vec->push_back(txn_item((ll) (key / key_partition_range), key, value, j, 1));
                }
                if( order_request ){
                    std::sort( req->readreq.begin(), req->readreq.end(), cmp );
                    std::sort( req->writereq.begin(), req->writereq.end(), cmp );
                    std::sort( req->rmwreq.begin(), req->rmwreq.end(), cmp );
                }
                req->tag();
            }
        }
    }
}

Request *ClientGen::get_req( ll txn_manager_id ){
    ll task_id;
    if( retry_txns == 1 ) {
        if( retry_queue->empty() == true ){
            task_id = generate_task(txn_manager_id);
        }else {
            ll now = get_timestamp();
            std::pair<ll, ll> tmp = retry_queue->front();
            if (now > tmp.second) {
                retry_queue->pop();
                task_id = tmp.first;
                assert(task_id >= 0 && task_id < prepared_req_per_thread);
            } else {
                task_id = generate_task(txn_manager_id);
            }
        }
    }else{
        task_id = generate_task(txn_manager_id);
    }

    assert( task_id != -1 );

    req_issued_cnt ++;
    req_issued_tot ++;

    req_tm_table[0][task_id].init();
    return &req_tm_table[0][task_id];
}

void ClientGen::recycle(Request *req) {
    req->in_flight = 0;
    if( retry_txns == 0 ) {
        if( req->need_retry == false ){
            if( record_flag )
                lat->push_back(req->lat);
        }else{
            req->need_retry = false;
            req->abort_cnt = 0;
        }
    }else {
        if (req->need_retry == true) {
//            assert( req->req_order >= 0 && req->req_order < prepared_req_per_thread );
            retry_queue->push( std::make_pair(req->req_order,\
            get_timestamp()+retry_interval*1000) );
//            printf("push %lld\n", req->req_order);
        } else {

            lat->push_back(req->lat);
            abort_cnt->push_back(req->abort_cnt);

            req->lat = 0;
            req->abort_cnt = 0;
        }
    }
    req_issued_cnt --;
}