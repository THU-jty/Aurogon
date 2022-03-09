//
// Created by prince_de_marcia on 2021/3/25.
//

#include "util.h"

void Queue::init( ll _cnt ){
    cnt = _cnt;
    front = tail = 0;
    lock_flag = 0;
    qe = (ll*)malloc( sizeof(ll)*cnt );
}

ll Queue::get(){
    while(front <= tail){
//        sched_yield();
    }
    ll ret = qe[tail%cnt];
    barrier();
    tail ++;
    return ret;
}

ll Queue::try_get(){
    if(front <= tail){
        return -1;
    }
    ll ret = qe[tail%cnt];
    tail ++;
    return ret;
}

void Queue::push( ll id ){
    while( front-tail >= cnt ){
//        sched_yield();
    }
    qe[front%cnt] = id;
    barrier();
    front ++;
    return ;
}

ll Queue::lock_try_get(){
    lock(&lock_flag);
    if(front <= tail){
        unlock(&lock_flag);
        return -1;
    }
    ll ret = qe[tail%cnt];
    tail ++;
    unlock(&lock_flag);
    return ret;
}

void Queue::lock_push( ll id ){
    lock(&lock_flag);
    while( front-tail >= cnt ){
//        sched_yield();
    }
    qe[front%cnt] = id;
    front ++;
    unlock(&lock_flag);
    return ;
}

void Linklist::init( ll _cnt ){
    cnt = _cnt;
    sum = 0;
    next = (ll *)malloc( sizeof(ll)*cnt );
    prev = (ll *)malloc( sizeof(ll)*cnt );
    for( ll i = 0; i < cnt; i ++ )
        next[i] = prev[i] = -1;
    head = tail = -1;
}

void Linklist::init2( Linklist *lst ){
    cnt = lst->cnt;
    sum = 0;
    next = lst->next;
    prev = lst->prev;
    head = tail = -1;
}


// insert v after position p, p == -1 for insert at head
void Linklist::insert( ll v, ll p ){
    if( v == -1 ) exit(-1);

//    check();

    sum ++;
    if( p == -1 ){
        next[v] = head;
        prev[v] = -1;
        barrier();
        if( head != -1 ){
            prev[head] = v;
        }
        else{
            tail = v;
        }
        head = v;
        return ;
    }
    next[v] = next[p];
    prev[v] = p;
    barrier();
    if( next[p] != -1 ){
        prev[next[p]] = v;
    }
    else{
        tail = v;
    }
    next[p] = v;
}

void Linklist::delt( ll v ){
    if( v == -1 ) exit(-1);

//    check();

    sum --;
    if( next[v] != -1 ){
        prev[ next[v] ] = prev[v];
    }else{
        tail = prev[v];
    }
    barrier();
    if( prev[v] != -1 ){
        next[ prev[v] ] = next[v];
    }else{
        head = next[v];
    }
    barrier();
    next[v] = prev[v] = -1;
}

void Linklist::check(){
    ll r0 = 0, r1 = 0;
    for( ll ptr = head; ptr != -1; ptr = next[ptr] ){
        r0 ++;
    }
    for( ll ptr = tail; ptr != -1; ptr = prev[ptr] ){
        r1 ++;
    }
    if( r0 != r1 || r0 != sum || r1 != sum ){
        printf("check wrong!\n");
    }
//    else printf("ok\n");
}

void Linklist::output(){
    for( ll ptr = head; ptr != -1; ptr = next[ptr] ){
        printf("%lld ", ptr);
    }
    printf("\n");
}

void Statistics::init(){
    txn_finished = 0;
    txn_aborted = 0;
    txn_executed = 0;
    req_aborted = 0;
    lat_txn_finished = 0;
    lat_txn_total = 0;
    rd_intvl = 0;
    wt_intvl = 0;
    rmw_intvl = 0;
    rd_txn_finished = 0;
    rmw_txn_finished = 0;
}

void Hotpoint::init( ll _loc )
{
    loc = _loc;
    for( ll i = 0; i < 2; i ++ ) ts[i] = cnt[i] = 0;
    flag = sta = step = 0;
    itvl = hotpoint_sta_range;
    delay_int = rmw_delay;
    delay_cnt = 2;
}

bool Hotpoint::check_hot(){
//    if( loc < 10 ) return true;
//    else return false;
    if( flag == 1 ){
        return true;
    }
    else return false;
}

void Hotpoint::update(ll tsp) {
    ll id = tsp/itvl;
    ll i = (id&1LL);
//    assert( tsp/itvl >= ts[id] );
    assert( i == 0 || i == 1 );
    if( id > ts[i] ){
        cnt[i] = 1;
        ts[i] = id;
        if (cnt[1 - i] >= hotpoint_num) {
            flag = 1;
            sta++;
//            printf("cnt %lld %lld ts %lld %lld id %lld now %lld flag %lld sta %lld\n", \
                cnt[0], cnt[1], ts[0], ts[1], id, tsp, flag, sta);
        } else {
            flag = -1;
        }
    }else{
        cnt[i] ++;
    }

}

void Zipfan_gen::init( ll _keys ){
    keys = _keys/zip_group;
    assert( _keys%zip_group == 0 );
//    mappings = new ll[node_cnt*2];
    factor = zipfan;
    zipf_v = 0.0;
    for( ll i = 0; i < keys; i ++ ){
        zipf_v += pow( 1.0/(i+1.0), factor );
    }
}


ll Zipfan_gen::gen(){
    ll ret;
    double alpha = 1 / (1 - factor);
    double u = (double)(rand() % 10000000) / 10000000;
    double uz = u * zipf_v;
    double zeta_2_theta = 1 + pow(1.0 / 2, factor);
    double eta = (1 - pow(2.0 / keys, 1 - factor)) /
    (1 - zeta_2_theta / zipf_v);
    if (uz < 1) ret = 0;
    else if (uz < 1 + pow(0.5, factor)) ret = 1;
    else {
        ret = (uint32_t) (keys * pow(eta * u - eta + 1, alpha));
    }
    ll a = ret*zip_group + rand()%zip_group;
    ll b = rand()%node_cnt;
    return a+b*keys*zip_group;
//    ll big = keys/node_cnt*mappings[ret%(node_cnt*2)] + ret/(node_cnt*2)*2 + ret%(node_cnt*2)/node_cnt;
//    ll small = rand()%zip_group;
//    return big*zip_group+small;
}

/*
 * original zipfan
void Zipfan_gen::init( ll _keys ){
    keys = _keys/zip_group;
    assert( _keys%zip_group == 0 );
    assert( keys%node_cnt == 0 );
    mappings = new ll[node_cnt*2];
    factor = zipfan;
    zipf_v = 0.0;
    for( ll i = 0; i < keys; i ++ ){
        zipf_v += pow( 1.0/(i+1.0), factor );
    }
    for( ll i = 0; i < node_cnt; i ++ )
        mappings[i] = mappings[node_cnt*2-i-1] = i;
}


ll Zipfan_gen::gen(){
    ll ret;
    double alpha = 1 / (1 - factor);
    double u = (double)(rand() % 10000000) / 10000000;
    double uz = u * zipf_v;
    double zeta_2_theta = 1 + pow(1.0 / 2, factor);
    double eta = (1 - pow(2.0 / keys, 1 - factor)) /
                 (1 - zeta_2_theta / zipf_v);
    if (uz < 1) ret = 0;
    else if (uz < 1 + pow(0.5, factor)) ret = 1;
    else {
        ret = (uint32_t) (keys * pow(eta * u - eta + 1, alpha));
    }
    ll big = keys/node_cnt*mappings[ret%(node_cnt*2)] + ret/(node_cnt*2)*2 + ret%(node_cnt*2)/node_cnt;
    ll small = rand()%zip_group;
    return big*zip_group+small;
}
*/

void Pri_lock::init( ll _thd_num ){
    issued_token = exec_token = -1;
    thd_num = _thd_num;
    for( int i = 0; i < thd_num; i ++ ){
        priority[i] = 0;
    }
}

//__sync_fetch_and_add
//bool Pri_lock::lock( ll thd_id, ll pri ){
//    priority[thd_id] = pri;
//    barrier();
//    while( !__sync_bool_compare_and_swap( &exec_token, -1, thd_id ) ){
//        if( exec_token == thd_id ) break;
//    }
//}
//
//void Pri_lock::unlock( ll thd_id ){
//    priority[thd_id] = 0;
//    ll min_ts = (1LL<<62), min_id = -1;
//    for( ll i = 0; i < thd_num; i ++ ){
//        if( priority[i] == 0 ) continue;
//        barrier();
//        if( priority[i] < min_ts ){
//            min_ts = priority[i];
//            min_id = i;
//        }
//    }
//    exec_token = min_id;
//}





void parser(int argc, char * argv[]) {
    assert( argc%2 != 0 );
    for (int i = 1; i < argc; i+=2) {
        assert(argv[i][0] == '-');
        if( argv[i][1] == 'n' && argv[i][2] == 'i' && argv[i][3] == 'd' )
            // nid
            my_node_id = atoi(&argv[i+1][0]);
        else if( argv[i][1] == 'n' && argv[i][2] == 'c' && argv[i][3] == 'n' && argv[i][4] == 't' )
            // ncnt
            node_cnt = atoi(&argv[i+1][0]);
        else if( argv[i][1] == 'w' && argv[i][2] == 't' && argv[i][3] == 'h' && argv[i][4] == 'd' )
            // wthd
            worker_per_server = atoi(&argv[i+1][0]);
        else if( argv[i][1] == 'c' && argv[i][2] == 't' && argv[i][3] == 'h' && argv[i][4] == 'd' )
            // cthd
            client_per_server = atoi(&argv[i+1][0]);
//        else if( argv[i][1] == 'z' ) {
//            // local_ip
//            std::ostringstream s;
//            s << argv[i + 1];
//            MemcacheAddr = s.str();
//        }
        else if( argv[i][1] == 'r' && argv[i][2] == 'u' && argv[i][3] == 'n' )
            // run
            running_time = atoi(&argv[i+1][0])*1000000000LL;
        else if( strcmp( argv[i], "-txn_size" ) == 0 ){
            txn_size = atoi(&argv[i+1][0]);
        }else if( strcmp( argv[i], "-recycle_entry_once" ) == 0 ){
            recycle_entry_once = atoi(&argv[i+1][0]);
        }else if( strcmp( argv[i], "-poll_size" ) == 0 ){
            poll_size = atoi(&argv[i+1][0]);
        }else if( strcmp( argv[i], "-local_poll_num" ) == 0 ){
            local_poll_num = atoi(&argv[i+1][0]);
        }else if( strcmp( argv[i], "-mvcc_type" ) == 0 ){
            mvcc_type = atoi(&argv[i+1][0]);
        }else if( strcmp( argv[i], "-workload" ) == 0 ){
            workload = atoi(&argv[i+1][0]);
        }else if( strcmp( argv[i], "-commit_phase" ) == 0 ){
            commit_phase = atoi(&argv[i+1][0]);
        }else if( strcmp( argv[i], "-immortal_writes" ) == 0 ){
            immortal_writes = atoi(&argv[i+1][0]);
        }else if( strcmp( argv[i], "-stale_reads" ) == 0 ){
            stale_reads = atoi(&argv[i+1][0]);
        }else if( strcmp( argv[i], "-retry_txns" ) == 0 ){
            retry_txns = atoi(&argv[i+1][0]);
        }else if( strcmp( argv[i], "-key_range" ) == 0 ){
            key_range = atoi(&argv[i+1][0]);
        }else if( strcmp( argv[i], "-read_req_ratio" ) == 0 ){
            read_req_ratio = atoi(&argv[i+1][0]);
        }else if( strcmp( argv[i], "-local_req_ratio" ) == 0 ){
            local_req_ratio = atoi(&argv[i+1][0]);
        }else if( strcmp( argv[i], "-read_snapshot_ratio" ) == 0 ){
            read_snapshot_ratio = atoi(&argv[i+1][0]);
        }
    }
    client_per_server = worker_per_server;
//    max_cq_num = txn_per_worker*txn_size*64;
    max_cq_num = 2000;
    key_partition_range = key_range/node_cnt;
    one_epoch_recv_wr = max_cq_num/2;
    in_flight_recv_area = max_cq_num-one_epoch_recv_wr;
    read_req_buffer_size = txn_size*worker_per_server*node_cnt*txn_per_worker*10;
    recycle_entry_once = req_buffer_size/5;
}

void read_config(){
    //return ;

    FILE *fp = NULL;
    fp = fopen("arg", "r");
    if( fp == NULL ){
        printf("Cannot open arg file\n");
        return ;
    }
    char s[100];
    while( fscanf(fp, "%s", s) != EOF ){
        ll x;
        fscanf(fp, "%lld", &x);
        int flag = 0;
        for( int i = 0; i < strlen(s); i ++ ){
            if( s[i] == '.' ){
                flag = 1;
                break;
            }
        }
        if( flag == 1 ){
            std::stringstream ss;
            ss << s;
            MemcacheAddr = ss.str();
            MemcachePort = x;
        }
        else if( strcmp( s, "txn_per_worker" ) == 0 ) txn_per_worker = x;
        else if( strcmp( s, "prepared_req_per_thread" ) == 0 ) prepared_req_per_thread = x;
        else if( strcmp( s, "node_cnt" ) == 0 ) node_cnt = x;
        else if( strcmp( s, "worker_per_server" ) == 0 ) worker_per_server = x;
        else if( strcmp( s, "txn_size" ) == 0 ) txn_size = x;
        else if( strcmp( s, "read_req_ratio" ) == 0 ) read_req_ratio = x;
        else if( strcmp( s, "local_req_ratio" ) == 0 ) local_req_ratio = x;
        else if( strcmp( s, "key_range" ) == 0 ) key_range = x;
        else if( strcmp( s, "running_time" ) == 0 ) running_time = x*1000000000LL;
        else if( strcmp( s, "warmup_time" ) == 0 ) warmup_time = x*1000000000LL;
        else if( strcmp( s, "req_buffer_size" ) == 0 ) req_buffer_size = x;
        else if( strcmp( s, "show_sync_time" ) == 0 ) show_sync_time = x;
        else if( strcmp( s, "mvcc_type" ) == 0 ) mvcc_type = x;
        else if( strcmp( s, "workload" ) == 0 ) workload = x;
        else if( strcmp( s, "read_snapshot_ratio" ) == 0 ) read_snapshot_ratio = x;
        else if( strcmp( s, "commit_phase" ) == 0 ) commit_phase = x;
        else if( strcmp( s, "immortal_writes" ) == 0 ) immortal_writes = x;
        else if( strcmp( s, "stale_reads" ) == 0 ) stale_reads = x;
        else if( strcmp( s, "retry_txns" ) == 0 ) retry_txns = x;
        else if( strcmp( s, "reordering" ) == 0 ) reordering = x;
        else if( strcmp( s, "wh_pre_server" ) == 0 ) wh_pre_server = x;
        else if( strcmp( s, "item_range" ) == 0 ) item_range = x;
        else if( strcmp( s, "new_order_ratio" ) == 0 ) new_order_ratio = x;
        else if( strcmp( s, "neworder_local_ratio" ) == 0 ) neworder_local_ratio = x;
        else if( strcmp( s, "payment_local_ratio" ) == 0 ) payment_local_ratio = x;
        else if( strcmp( s, "local_poll_num" ) == 0 ) local_poll_num = x;
        else if( strcmp( s, "remote_poll_num" ) == 0 ) remote_poll_num = x;
        else if( strcmp( s, "ret_read_poll_num" ) == 0 ) ret_read_poll_num = x;
        else if( strcmp( s, "repeat_poll_num" ) == 0 ) repeat_poll_num = x;
        else if( strcmp( s, "order_request" ) == 0 ) order_request = x;
        else if( strcmp( s, "retry_interval" ) == 0 ) retry_interval = x;
        else if( strcmp( s, "insert_skew" ) == 0 ) insert_skew = x;
        else if( strcmp( s, "use_pri_lock" ) == 0 ) use_pri_lock = x;
        else if( strcmp( s, "rmw_delay" ) == 0 ) rmw_delay = x;
        else if( strcmp( s, "key_delay" ) == 0 ) key_delay = x;
        else if( strcmp( s, "hotpoint_num" ) == 0 ) hotpoint_num = x;
        else if( strcmp( s, "zip_group" ) == 0 ) zip_group = x;
        else if( strcmp( s, "lower_delay" ) == 0 ) lower_delay = x*1000LL;
        else if( strcmp( s, "nu_rand" ) == 0 ) nu_rand = x;
        else if( strcmp( s, "scale_factor" ) == 0 ) scale_factor = x;
        else if( strcmp( s, "batch_send" ) == 0 ) batch_send = x;
        else if( strcmp( s, "zipfan" ) == 0 ){
            zipfan = x/100.0;
        }
        else{
            printf("%s\n", s);
            assert(0);
        }
        //printf("%s %lld\n", s, x);
    }
    fclose(fp);
}

void bind_core( ll id ){
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(id, &mask);
    if (pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask) < 0) {
        fprintf(stderr, "set thread affinity failed\n");
    }
}