#include <iostream>
#include "thread.h"
#include "util.h"
#include "config.h"
#include "table.h"
#include "txn.h"
#include "rdma.h"
#include "tpcc.h"
#include "clock_sync.h"

void * run_thread(void * id) {
    MyThread * thd = (MyThread *) id;
    thd->run();
    return NULL;
}

void manul_sync_time();
void start_clock();
void end_clock();
void probe_debug(ll start_ts);
void signalhandler( int signum );

int probe = 0;
Statistics sta;

int main(int argc, char* argv[]) {
//    bind_core(0);
    printf("row %d\n", sizeof(row));
    printf("List %d\n", sizeof(Linklist));
    printf("version %d\n", sizeof(version));
//    return 0;
    read_config();
    parser(argc, argv);
    signal( SIGSEGV, signalhandler );

    start_clock();

    printf("gt time %lld\n", gt.get_timestamp());
    printf("Gonna set done\n");

    stop_running = 0;
    start_running = 0;

    if( workload == 3 ){
        tpcc.init();
        table.init( tpcc.num_keys );
    }else {
        table.init(key_partition_range);
    }

    worker_thds = new WorkerThread[worker_per_server];
    client_thds = new ClientThread[client_per_server];


    pthread_t * pthds =
            (pthread_t *) malloc(sizeof(pthread_t)*
            (worker_per_server
            + client_per_server));
    ll thd_cnt = 0;

    for( ll i = 0; i < worker_per_server; i ++ ){
        worker_thds[i].setup(my_node_id, i);
        pthread_create(&pthds[thd_cnt++], NULL,
                run_thread, (void *)&worker_thds[i]);
    }

//    for( ll i = 0; i < client_per_server; i ++ ){
//        client_thds[i].setup(my_node_id, thd_cnt);
//        pthread_create(&pthds[thd_cnt++], NULL,
//                       run_thread, (void *)&client_thds[i]);
//    }
    while(1){
        int flag = 0;
        for( ll i = 0; i < worker_per_server; i ++ ){
            if( worker_thds[i].ready_running == 0 ){
                flag = 1;
                break;
            }
        }
//        for( ll i = 0; i < client_per_server; i ++ ){
//            if( client_thds[i].ready_running == 0 ){
//                flag = 1;
//                break;
//            }
//        }
        if( flag ){
            sched_yield();
            continue;
        }
        else break;
    }
    if( show_sync_time ) manul_sync_time();


    ll start_ts = get_timestamp();
    ll end_ts;
    start_running = 1;
    barrier();
    fprintf(stdout, "start running %lld\n", get_timestamp());
    //sleep(100);
    while(1){
        end_ts = get_timestamp();
        if( end_ts-start_ts >= warmup_time && record_flag == 0 )
            record_flag = 1;
        if( end_ts-start_ts < running_time+warmup_time ){
//            sched_yield();
            usleep(1000);
        }
        else break;
    }
    stop_running = 1;

    sleep(1);
    printf("test ends!!!\n");

    end_clock();

    for( ll i = 0; i < worker_per_server; i ++ ){
        worker_thds[i].output();
        sta.txn_finished += worker_thds[i].txn_finished;
        sta.txn_aborted += worker_thds[i].txn_aborted;
        sta.txn_executed += worker_thds[i].txn_executed;
        sta.req_aborted += worker_thds[i].req_aborted;
        sta.lat_txn_finished += worker_thds[i].lat_txn_finished;
        sta.lat_get_ts += worker_thds[i].lat_get_ts;
        sta.rd_intvl += worker_thds[i].rd_intvl;
        sta.wt_intvl += worker_thds[i].wt_intvl;
        sta.rmw_intvl += worker_thds[i].rmw_intvl;
        sta.rd_txn_finished += worker_thds[i].rd_txn_finished;
        sta.rmw_txn_finished += worker_thds[i].rmw_txn_finished;
    }


    for( ll i = 0; i < worker_per_server; i ++ ){
        for( ll j = 0; j < worker_thds[i].client.lat->size(); j ++ ) {
            sta.lat_cdf.push_back(worker_thds[i].client.lat->at(j));
            sta.lat_txn_total += worker_thds[i].client.lat->at(j);
        }
    }
    std::sort(sta.lat_cdf.begin(), sta.lat_cdf.end());


    sta.avg_lat_txn_finished = (double)sta.lat_txn_finished/sta.txn_finished;
    sta.avg_lat_get_ts = (double)sta.lat_get_ts/sta.txn_finished;
    sta.testing_time = end_ts-start_ts-warmup_time;
    sta.testing_time /= 1e9;
    fprintf(stdout, "Total finished: %lld Total abort: %lld\n", sta.txn_finished, sta.txn_aborted);
    fprintf(stdout, "Testing time: %lf s\n", sta.testing_time);
    fprintf(stdout, "Txn thpt: %.2lf Abort ratio %.2lf %%\n", sta.txn_finished/sta.testing_time, \
                     100.0*sta.txn_aborted/(sta.txn_aborted+sta.txn_finished));
    fprintf(stdout, "Txn avg lat: %lf us\n", sta.avg_lat_txn_finished/1e3);
    fprintf(stdout, "Txn avg lat(including abort): %.2lf "
                    "med %.2lf p99 %.2lf p999 %.2lf max %.2lf\n", sta.lat_txn_total/sta.lat_cdf.size()/1e3,
                    sta.lat_cdf[sta.lat_cdf.size()/2]/1e3,
                    sta.lat_cdf[sta.lat_cdf.size()-sta.lat_cdf.size()/100]/1e3,
                    sta.lat_cdf[sta.lat_cdf.size()-sta.lat_cdf.size()/1000]/1e3,
                    sta.lat_cdf[sta.lat_cdf.size()-1]/1e3
                    );
    fprintf(stdout, "Txn_intvl avg lat rd: %lf us rmw: %lf\n", \
                    1.0*sta.rd_intvl/sta.rd_txn_finished/1e3, 1.0*sta.rmw_intvl/sta.rmw_txn_finished/1e3 );
    fprintf(stdout, "Get_ts avg lat: %lf us\n", sta.avg_lat_get_ts/1e3);
}

void manul_sync_time(){
    if( my_node_id == 0 ){
        sleep(2);
        for( ll i = 1; i < node_cnt; i ++ ){
            ll tmp = gt.get_timestamp();
            memcpy(worker_thds[0].rdma->rdma_send_region[i], &tmp, sizeof(ll));
            worker_thds[0].rdma->send(i, 0,
                    worker_thds[0].rdma->rdma_send_region[i],
                    sizeof(ll), 0);
        }
        ll cnt = 0;
        printf("node%lld time: %lld\n", my_node_id, get_timestamp());
    }
    else{
        ll cnt = 0;
        while(1){
            struct ibv_wc wc_array[10];
            int num = ibv_poll_cq(worker_thds[0].rdma->recv_cq, 1, wc_array);
            if(num > 0) {
                //DEBUG("send ack %d\n", num);
                ll ts = gt.get_timestamp();
                for (int k = 0; k < num; k++) {
                    struct ibv_wc *wc = &wc_array[k];
                    if( wc->opcode == IBV_WC_RECV || wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM ){
                        if( wc->status == IBV_WC_SUCCESS ){
                            if( wc->wr_id != 4396 ){
                                printf("nonononononono  %lld\n", wc->wr_id);
                            }
                            ll _node_id = wc->wr_id%node_cnt, _recv_id = (wc->wr_id/node_cnt)%max_cq_num;
                            ll tmp;
                            memcpy( &tmp, worker_thds[0].rdma->rdma_recv_region[_node_id]\
                            +_recv_id*rdma_buffer_size, sizeof(ll) );
                            base_time_offset = tmp-ts+1000;
                            printf("node%lld time: %lld offset %lld\n",
                                    my_node_id, get_timestamp(), base_time_offset);
                            cnt ++;
                            break;
                        }
                    }
                }
            }
            if( cnt == 1 ) break;
        }
    }
    sleep(3);
}

void start_clock(){
    gt.init(hp_connect_port, my_node_id);
    gt.run();
    while(!gt.is_warmup_done()) sched_yield();
}

void end_clock(){
    gt.set_done();
    gt.finalize();
}



void print_txn_mag(){
    for( ll i = 0; i < worker_per_server; i ++ ){
        for( ll j = 0; j < txn_per_worker; j ++ ){
            Txn_manager *mag = &worker_thds[i].txn->txn_table[j];
            printf("T%lldM%lld txn_id %6lld ", i, j, mag->txn_id);
            printf("RMW: ");
            for( ll k = 0; k < mag->req->rmwreq.size(); k ++ ){
                printf("%5lld %lld ", mag->req->rmwreq[k].key, mag->req->rmwreq[k].req_status);
            }
            printf("READ: ");
            for( ll k = 0; k < mag->req->readreq.size(); k ++ ){
                printf("%5lld %lld ", mag->req->readreq[k].key, mag->req->readreq[k].req_status);
            }
            printf("\n");
        }
    }
}

void print_version( ll loc ){
    assert( loc < key_partition_range );
    row *ro = &table.rows[loc];
    for( ll ptr = ro->wt_lst.head; ptr != -1; ptr = ro->wt_lst.next[ptr] ){
        version *v = &table.rows[loc].write[ptr];
        printf("%4lld %d %lld %lld id %lld mg %lld s %lld\n", \
        ptr, v->type, v->wts, v->rts, v->txn_id, v->txn_mg_id, v->s_id);
    }
}

void print_read_buffer( ll loc ) {
    assert( loc < key_partition_range );
    row *ro = &table.rows[loc];
    for( ll i = 0; i < worker_per_server; i ++ ){
        printf("T%lld:\n", i);
        for( ll ptr = ro->rd_lst[i].tail; ptr != -1; ptr = ro->rd_lst[i].prev[ptr] ) {
            QueryMessage *qmsg = &worker_thds[i].read[ptr];
            printf("RW %d txn_id %lld txn_mag_id %lld k %lld v %lld tmsp %lld s_id %lld th_id %lld r_id %lld\n",
          qmsg->type, qmsg->txn_id, qmsg->txn_mg_id, qmsg->key, qmsg->value, qmsg->timestamp, qmsg->source_node_id, qmsg->source_thd_id, qmsg->req_id);

        }
    }
}

void print_read_pending() {
    for( ll i = 0; i < worker_per_server; i ++ ){
        printf("T%lld:\n", i);
        for( ll j = worker_thds[i].ins_read.tail; j < worker_thds[i].ins_read.front; j ++ ) {
            ll pos = worker_thds[i].ins_read.qe[j%worker_thds[i].ins_read.cnt];
            Read_req_item *rd = &worker_thds[i].read_req_q_pool[pos];
            printf("k %4lld v %4lld ts %lld mg %lld id %lld req %lld s %lld\n",
                    rd->key, rd->version_id, rd->timestamp, rd->txn_mg_id, rd->txn_id, rd->req_id, rd->s_id);
        }
    }
}

void probe_debug(ll start_ts){
    ll now_ts = get_timestamp();
    ll unit = 1000000000LL;
    if( now_ts-start_ts >= 1*unit && probe == 0 ){
        ll sum = 0;
        for( ll i = 0; i < worker_per_server; i ++ ){
            sum += worker_thds[i].txn_cnt;
        }
        if( sum > 10000 ){
            printf("no bug\n");
            probe = 1;
            return ;
        }
        for(ll i = 0; i < worker_per_server; i ++)
            for( ll j = 0; j < txn_per_worker; j ++ ) {
                printf("T%lld ", i);
                worker_thds[i].txn->txn_table[j].output();
            }
        probe = 1;
    }else if( now_ts-start_ts >= 2*unit && probe == 1 ){
        ll sum = 0;
        for( ll i = 0; i < worker_per_server; i ++ ){
            sum += worker_thds[i].txn_cnt;
        }
        if( sum > 10000 ){
            printf("no bug\n");
            probe = 2;
            return ;
        }
        for (ll i = 0; i < key_partition_range; i++){
            ll key = i + my_node_id * key_partition_range;
            ll loc = table.mappings[key];
            row *ro = &table.rows[loc];
        }
        for(ll i = 0; i < worker_per_server; i ++)
            for( ll j = 0; j < txn_per_worker; j ++ ) {
                printf("T%lld ", i);
                worker_thds[i].txn->txn_table[j].output();
            }
        probe = 2;
    }
}

void signalhandler( int signum ){
    printf("programm exit wrong!!!\n");
    exit(0);
}
