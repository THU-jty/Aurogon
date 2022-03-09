#include"clock_sync.h"

//////////////
// DataList //
//////////////

std::string my_output_dir = std::string("./data/");


void DataList::init(int id){

    // printf("Gonna malloc %lu\n", DL_len*sizeof(ts_t));
    node_id = id;
    printf("The node id is %d\n", node_id);

    this->timestamp = (ts_t*)malloc(DL_len*sizeof(ts_t));
    this->upper_dot = (ts_t*)malloc(DL_len*sizeof(ts_t));
    this->lower_dot = (ts_t*)malloc(DL_len*sizeof(ts_t));
    this->state = (char*)malloc(DL_len*sizeof(char));

    assert(timestamp&&upper_dot&&lower_dot&&state);

    for(int i=0; i<DL_len; i++){
        state[i] = PROBE_INV;
        upper_dot[i] = 0;
        lower_dot[i] = 0;
    } 

    start_ts = 0;
    idx_start = 0;
    idx_end = 0;
    fitting_flag = 0;

    // open the files to output data
    #ifdef OUTPUT_MODE
    char path_buffer[128];
    sprintf(path_buffer, "%supper_%d.txt", my_output_dir.c_str(), node_id);
    ofs_upper.open(path_buffer, std::ios::out);
    sprintf(path_buffer, "%slower_%d.txt", my_output_dir.c_str(), node_id);
    ofs_lower.open(path_buffer, std::ios::out);
    sprintf(path_buffer, "%sinfo_%d.txt", my_output_dir.c_str(), node_id);
    ofs_info.open(path_buffer, std::ios::out);
    sprintf(path_buffer, "%sdot_%d.txt", my_output_dir.c_str(), node_id);
    cns_dot.open(path_buffer, std::ios::out);
    sprintf(path_buffer, "%scns_info_%d.txt", my_output_dir.c_str(), node_id);
    cns_info.open(path_buffer, std::ios::out);

    if(!ofs_upper || !ofs_lower || !ofs_info || !cns_dot){
        fprintf(stderr, "Open file failed\n");
        assert(false);
    }
    #endif

    // temp
    svm_id = 0;
    for(int i=0; i<60; i++)
        svm_duration[i] = -1;


};

void DataList::finalize(){

    // close the files
    #ifdef OUTPUT_MODE
    ofs_upper.close();
    ofs_lower.close();
    ofs_info.close();
    cns_dot.close();
    cns_info.close();
    #endif

    free(timestamp);
    free(upper_dot);
    free(lower_dot);
    free(state);

    for(int i=0; i<60; i++){
        ts_t duration = svm_duration[i];
        if(duration==-1)
            break;
        //printf("%lld ms\n", duration/1000000);
    }

}

void DataList::InsertLocalProbe(int request_id, ts_t ts, ts_t upper, ts_t lower){

    // we assume local probe inserts continuously with regard to request ID
    int idx = request_id%DL_len;
    if(state[idx]!=PROBE_INV){
        printf("Warning: overwriting valid data %d.\n", idx);
        state[idx] = PROBE_INV;
        assert(false);
    }
    
    timestamp[idx] = ts;
    upper_dot[idx] = upper;
    lower_dot[idx] = lower;
    
    __asm__ __volatile__("" ::: "memory");
    
    state[idx] |= PROBE_LR;

}

// we assume identical request ID in a short run(long enough to fill an int variable), so this step doesn't consider concurrency issue regarding request ID.
void DataList::InsertRemoteUpper(int request_id, ts_t upper){

    int idx = request_id%DL_len;  
    assert(state[idx]&PROBE_LR);
    // printf("Insert remote upper %d\n", idx);
    // assert(!(state[idx]&PROBE_UPR));
    if(state[idx]&PROBE_UPR){
        printf("PROBE UPPER failed in %d\n", idx);
    }

    upper_dot[idx] = upper - upper_dot[idx];

    // insert the dot into file
    #ifdef OUTPUT_MODE
    ofs_upper<<timestamp[idx]<<" "<<upper_dot[idx]<<std::endl;
    #endif

    __asm__ __volatile__("" ::: "memory");
    state[idx] |= PROBE_UPR;

    if(state[idx]==PROBE_OK)
        tick(idx);

}

void DataList::InsertRemoteLower(int request_id, ts_t lower){

    int idx = request_id%DL_len;  
    assert(state[idx]&PROBE_LR);
    // printf("Insert remote lower %d\n", idx);
    // assert(!(state[idx]&PROBE_LWR));
    if(state[idx]&PROBE_LWR){
        printf("PROBE LOWER failed in %d\n", idx);
    }    

    lower_dot[idx] = lower - lower_dot[idx];

    // insert the dot into file
    #ifdef OUTPUT_MODE
    ofs_lower<<timestamp[idx]<<" "<<lower_dot[idx]<<std::endl;
    #endif

    __asm__ __volatile__("" ::: "memory");
    state[idx] |= PROBE_LWR;

    if(state[idx]==PROBE_OK)
        tick(idx);

}

void* SVM_fitting(void* argv){

    // cpu_set_t mask;
    // CPU_ZERO(&mask);
    // CPU_SET(20, &mask);
    // sched_setaffinity(0, sizeof(mask), &mask);

    DataList* dl = (DataList*)argv;
    GlobalTimer* gtimer = (GlobalTimer*)dl->gtimer;
    int start = dl->idx_start;
    int end = dl->idx_end;
    if(end<start)
        end += DL_len;

    int iddd = dl->svm_id;
    dl->svm_id++;

    // printf("start SVM fitting with start is %d, end is %d\n", start, end);
    // start SVM fitting
    svm my_svm(dl->timestamp[start], dl->lower_dot[start]);

    #ifdef OUTPUT_MODE
    ts_t last_ts = dl->timestamp[start];
    bool valid;
    #endif

    for(int i=start; i<=end; i++){  // insert the dot
        int idx = i%DL_len;
        #ifdef OUTPUT_MODE
        valid=false;
        #endif
        if(dl->state[idx]&PROBE_UPR){
            my_svm.insert_dot(dl->timestamp[idx], dl->upper_dot[idx], 0);
            #ifdef OUTPUT_MODE
            valid = true;
            #endif
        }
        else
            printf("Upper probe %d not ready\n", idx);
        if(dl->state[idx]&PROBE_LWR){
            my_svm.insert_dot(dl->timestamp[idx], dl->lower_dot[idx], 1);
            #ifdef OUTPUT_MODE
            valid = true;
            #endif
        }
        else
            printf("Lower probe %d not ready\n", idx);
        // drop the data
        #ifdef OUTPUT_MODE
        if(valid)
            last_ts = dl->timestamp[idx];
        #endif
        __asm__ __volatile__("":::"memory");
        dl->state[idx] = PROBE_INV;
    }

    // remark the flag
    __asm__ __volatile__("":::"memory");
    dl->idx_start = (dl->idx_end+1)%DL_len;
    dl->fitting_flag = 0;

    // get SVM results
    double k, b, distance;  // k without unit, b in ns
    ts_t start_time = get_real_clock();
	my_svm.get_result(&k, &b, &distance);
    ts_t end_time = get_real_clock();
    
    // b+=114;  // this is the compensation for RC/UD difference

    if(iddd<60){
    assert(dl->svm_duration[iddd]==-1);
    dl->svm_duration[iddd] = end_time-start_time;
    }
    
    // printf("SVM calculation %lld ms\n", (end_time-start_time)/1000000);
    ts_t base_x =dl->timestamp[start];
    ts_t x_mid = base_x + (fit_interval/2)*BILLION;
    ts_t y_mid = k*(fit_interval/2)*BILLION + b;

    // output
    #ifdef OUTPUT_MODE
    dl->ofs_info<<std::scientific<<k<<" "<<std::fixed<<b<<" "<<distance<<" "<<my_svm.get_init_x()<<" "<<my_svm.get_init_y()<<" "<<last_ts<<std::endl;
    #endif

    // set Master node's new dot
    // printf("Goona insert dot %lld, %lld\n", x_mid, y_mid);
    gtimer->clockmanager->InsertResDot(x_mid, y_mid);

    return NULL;

}

void* LSVM_fitting(void* argv){

    printf("Error: empry method.\n");
    assert(false);
    return NULL;

}

void DataList::tick(int idx){

    if(!fitting_flag){  // make sure no fitting is in process
        // if the boundary arrives
        if(timestamp[idx]<timestamp[idx_start]+fit_interval*(int)1e9)
            return;
        // modify the flag. Note that we insert dot in a serial method, so there is no need for concurrency control
        fitting_flag = 1;
        idx_end = idx;
        // printf("Gonna do SVM when idx is %d\n", idx_end);

        #if(fitting_method==FM_SVM)
        pthread_create(&fitting_thread, NULL, SVM_fitting, (void*)this);
        #elif(fitting_method==FM_LSVM)
        pthread_create(&fitting_thread, NULL, LSVM_fitting, (void*)this);
        #else
        printf("Warning: Not fitting method available.\n");
        #endif
    }
}

// This function do GC by mark data in [id_start, id_end] as invalid
void DataList::DropProbeData(int id_start, int id_end){

    printf("Gonna Drop data [%d, %d]\n", id_start, id_end);
    if(id_end<id_start)
        id_end+=DL_len;

    for(int i=idx_start; i<=idx_end; i++){
        int idx = i%DL_len;
        state[idx] = PROBE_INV;
    }

}

//////////////////
// ClockManager //
//////////////////

void ClockManager::init(GlobalTimer* timer_pointer, int type){

    for(int i=0; i<RECORD_MAX; i++){
        edge_valid[i] = false;
        mid_valid[i] = false;
    }

    this->gtimer = timer_pointer;
    // get the device list for clock info
    struct ibv_device** dev_list = ibv_get_device_list(NULL);
    struct ibv_device* ib_dev = *dev_list;
    ctx = ibv_open_device(ib_dev);
    memset(&vex, 0, sizeof(ibv_values_ex));
	mlx5dv_get_clock_info(ctx, &clock_info);
    vex.comp_mask =  IBV_VALUES_MASK_RAW_CLOCK;

    #ifdef OUTPUT_MODE
    char path_buffer[128];
    if(type==0)
        sprintf(path_buffer, "%scns_edge_%d.txt", my_output_dir.c_str(), gtimer->node_id);
    else if(type==1)
        sprintf(path_buffer, "%snns_edge_%d.txt", my_output_dir.c_str(), gtimer->node_id);
    else
        assert(false);

    ofs_edge.open(path_buffer, std::ios::out);
    #endif

    // get start clock
    this->type = type;
    if(type==0){
        _interval = CNS_interval*1000000000;
        start_x = get_cpu_time();
        start_y = get_nic_time();

    }
    else if(type==1){
        start_x = get_nic_time();
        start_y = 0;  // typically, delta won't be so large, so we set it 0 
        _interval = fit_interval*1000000000;
    }
    else
        assert(false);

    delete_idx = RECORD_MAX/2;

    // printf("[Debug] type %d, start_x is %lld, start_y is %lld\n", type, start_x, start_y);

}

void ClockManager::finalize(){
    #ifdef OUTPUT_MODE
    ofs_edge.close();
    #endif
}

void ClockManager::InsertResDot(ts_t x, ts_t y){

    #ifdef OUTPUT_MODE
    ofs_edge<<x<<" "<<y<<" ";
    #endif

    // get the index
    double x_ = (double)(x-start_x);
    assert(x_>0);
    double y_ = (double)(y-start_y);

    // if(type==1)
    //     printf("[Debug] %f = %lld - %lld\n", y_, y, start_y);

    int idx = ((int)(x_/_interval))%RECORD_MAX;  // index for "mid array"


    // assert(!mid_valid[idx]);
    if(mid_valid[idx]){
        printf("Error in slot %d, previous ts is %lld, now ts is %lld\n", idx, (ts_t)mid_x[idx]+start_x, x);
        // assert(false);
    }

    // we try at most 5 history count in case that the previous result is not ready yet due to spike SVM calculation time
    int idx1;
    bool valid=false;

    for(int i=0; i<5; i++){
        idx1 = (idx - i + RECORD_MAX)%RECORD_MAX;
        if(mid_valid[idx1]){
            valid=true;
            break;
        }
    }

    if(!valid&&gtimer->is_warmup_done()){
        printf("Previous TS %d is not ready yet\n", idx);
        assert(false);
    }

    mid_x[idx] = x_;
    mid_y[idx] = y_;
    mid_valid[idx] = true;
    mid_valid[delete_idx] = false;
    delete_idx = (delete_idx+1)%RECORD_MAX;

    double xp = mid_x[idx1];
    double yp = mid_y[idx1];

    #ifdef OUTPUT_MODE
    ofs_edge<<(ts_t)x_+start_x<<" "<<(ts_t)y_+start_y<<" ";
    #endif

    // put a new dot into the NIC array. Different method may behave differently
    #if fitting_method==FM_SVM
    double delta_x = (SVM_delay+0.5)*_interval;
    assert(delta_x>0);
    double target_x = delta_x + x_;
    double target_y = (y_-yp)*delta_x/(x_-xp)+y_;
    // insert the dot
    idx = ((int)(target_x/_interval))%RECORD_MAX;

    edge_x[idx] = target_x;
    edge_y[idx] = target_y;

    #ifdef OUTPUT_MODE
    ofs_edge<<(ts_t)target_x+start_x<<" "<<(ts_t)target_y+start_y<<std::endl;
    #endif

    __asm__ __volatile__("" ::: "memory");
    edge_valid[idx] = true;
    edge_valid[(idx+RECORD_MAX/2)%RECORD_MAX] = false;  // periodically GC

    #elif fitting_method==FM_LSVM
    assert(false);
    #else
    assert(false);
    #endif

}

ts_t ClockManager::GetModifiedValue(ts_t timestamp){

    bool channel2 = false;
    if(type==1 && !gtimer->rcc_master)
        channel2 = true;

    // note that "timestamp" should already be the NIC ts
    double delta = (double)(timestamp-start_x);
    assert(delta>0);
    int index = ((int)(delta/_interval))%RECORD_MAX;

    int idx1, idx2;
    int valid_cnt = 0;

      for(int i=0; i<5; i++){

        if(valid_cnt==1){
            idx2 = (index - i + RECORD_MAX)%RECORD_MAX;
            if(channel2 || edge_valid[idx2]){
                valid_cnt++;
                break;                
            }
        }
        else{
            idx1 = (index - i + RECORD_MAX)%RECORD_MAX;
            if(channel2 || edge_valid[idx1])
                valid_cnt++;
        }

    }
    if(valid_cnt!=2){
        printf("In TS: target idx %d valid count %d less than 2 after 5 try!\n", index, valid_cnt);
        return 0;  
    }

    double x1 = edge_x[idx1%RECORD_MAX];
    double x2 = edge_x[idx2%RECORD_MAX];

    double y1 = edge_y[idx1%RECORD_MAX];
    double y2 = edge_y[idx2%RECORD_MAX];

    double target_y = (delta-x2)*(y2-y1)/(x2-x1) + y2;

    ts_t result = (ts_t)target_y + start_y;

    if(channel2)
        result = 0;

    return result;

}

ts_t ClockManager::GetRealTime(ts_t timestamp){

    return timestamp+GetModifiedValue(timestamp);

}


ts_t ClockManager::get_nic_time(){

    assert(!ibv_query_rt_values_ex(ctx, &vex));
    return mlx5dv_ts_to_ns(&clock_info, vex.raw_clock.tv_nsec);

}

ts_t ClockManager::get_cpu_time(){

	struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_nsec + 1000000000*ts.tv_sec;

}

/////////////////
// GlobalTimer //
/////////////////

typedef struct{
    tsocket_context* ctx;
    char ip_addr[128];
    int port;
}master_info;

typedef struct{
    tsocket_context_ud* ctx;
    char ip_addr[128];
    int port;
}master_info_ud;

void* master_run_rc(void* argv){

    // this function enable asyn connection of master function
    master_info* info = (master_info*)argv;
    assert(!tsocket_connect(info->ctx, info->ip_addr, info->port));
    return NULL;

}

void* master_run_ud(void* argv){

    // this function enable asyn connection of master function
    master_info_ud* info = (master_info_ud*)argv;
    assert(tsocket_connect_ud(info->ctx, info->ip_addr, info->port)!=-1);
    return NULL;

}

void GlobalTimer::init(int local_port, int id){

    port = local_port;
    SlaveCount = 0;
    _is_done = false;
    node_id = id;
    CNS_warmup_done = false;
    local_warmup_done = false;
    global_warmup_done = false;
    datalist = new DataList;
    datalist->init(node_id);
    datalist->gtimer = this;
    clockmanager = new ClockManager;
    clockmanager->init(this, 1);
    clockmanager_cpu = new ClockManager;
    clockmanager_cpu->init(this, 0);

    // step 1: create RDMA socket
    rcc_center = create_rdma();  // create socket with center
    udc = create_tsocket_ud();
    rcc_list = NULL;
    rcc_master = NULL;

    // step 2: build RDMA connection with center

    assert(!rdma_bind(rcc_center, (int)(local_port+ts_port_delta)));

    // step 3: get new-work structure from center node
    // post recv
    cent2master msg;
    int rc = 1;
    while(rc)
        rc = rdma_recv(rcc_center, (char*)(&msg), sizeof(cent2master), NULL);
    printf("Got network topologies from the Center.\n");

    // build connection with the other node. We fix that master use "connect" while slave use "bind" function

    // step 4: Master nodes asynchronously connect to the Slave nodes
    master_info rc_info;
    master_info_ud ud_info;
    if(msg.master_port!=-1){
        rcc_master = create_tsocket();
        strcpy(rc_info.ip_addr, msg.master_ip);
        strcpy(ud_info.ip_addr, msg.master_ip);
        rc_info.port = msg.master_port;
        ud_info.port = msg.master_port+UD_port_delta;
        rc_info.ctx = rcc_master;
        ud_info.ctx = udc;
        printf("Gonna connect to %s,%d\n", rc_info.ip_addr, rc_info.port);
        fflush(stdout);
        pthread_create(&master_rc, NULL, master_run_rc, &rc_info);
        pthread_create(&master_ud, NULL, master_run_ud, &ud_info);
    }
    
    // step 5: Slave nodes synchronously bind the ports
    
    if(msg.slave_port_num){
        rcc_list = (tsocket_context**) malloc(sizeof(tsocket_context*)*msg.slave_port_num);
        SlaveCount = msg.slave_port_num;
    }

    int col=0;
    while(msg.slave_port[col]!=-1){

        printf("Gonna bind on %d\n", msg.slave_port[col]);
        // build RC connection
        rcc_list[col] = create_tsocket();
        assert(!tsocket_bind(rcc_list[col], msg.slave_port[col]));
        // build UD connection
        int ud_qp_idx = tsocket_bind_ud(udc, msg.slave_port[col]+UD_port_delta);
        assert((ud_qp_idx-1)==col);  // here should also minus one
        col++;

    }
    assert(col==SlaveCount);

    assert(ud_batch>=(lcu_batch*SlaveCount));

    fprintf(stdout, "Sync connection built with other nodes.\n");

    // wait for the master connection finish
    if(msg.master_port!=-1){
        pthread_join(master_rc, NULL);
        pthread_join(master_ud, NULL);
    }

}

typedef struct{

    DataList* datalist;
    char* buffer;

}extract_s;

void* extract_data(void* argv){

    // we don't want extraction to block probing
    extract_s* es = (extract_s*)argv;
    DataList* datalist = es->datalist;
    batch_data* bd = (batch_data*)es->buffer;
    for(int i=0; i<bd->data_num; i++){
        int id = bd->request_id[i];
        ts_t timestamp = bd->timestamp[i];
        bool is_rc = bd->is_rc_data[i];
        if(is_rc){
            datalist->InsertRemoteLower(id, timestamp);
        }
            
        else{
            datalist->InsertRemoteUpper(id, timestamp);
        }
            
    }
    return NULL;

}

void* master_run(void* argv){

    GlobalTimer* gtimer = (GlobalTimer*) argv;

    // master side starts the probing, collects ts
    ts_t ts_rc, probe_start, probe_end;
    // ts_t epoch_ts, now_ts;

    int request_id = 0;  // this id use locally, so we don't do concurrency control
    int max_request_id = 2147483640;  // be a little smaller
    char buffer[lcu_buffer_size];
    ts_t ts_array[ud_batch];
    int qp_idx_array[ud_batch];

    DataList* datalist = gtimer->datalist;

    extract_s extract_struct;
    extract_struct.buffer = buffer;
    extract_struct.datalist = datalist;

    // post recv for large request
    tsocket_post_recv(gtimer->rcc_master, 1, 8);

    int epoch_count = 0;
    int time_count = 0;

    printf("Master thread runs.\n");

    // int upper_idx = 0, lower_idx=0;

    while(!gtimer->is_done()){

        time_count++;
        if(time_count==dot_num){
            epoch_count++;
            time_count = 0;
            if(epoch_count>warmup_epoch)
                gtimer->local_warmup_done = true;
        }

        probe_start = get_real_clock();

        // RC send & poll
        tsocket_send(gtimer->rcc_master, request_id, buffer, 0, &ts_rc);

        // UD send & poll
        tsocket_send_ud(gtimer->udc, 0, request_id, buffer, 0);

        int poll_num = tsocket_poll_send(gtimer->udc, ts_array, qp_idx_array);
        assert(poll_num==1&&qp_idx_array[0]==0);

        // insert the starting probe data
        datalist->InsertLocalProbe(request_id, ts_rc, ts_array[0], ts_rc);  // we let the UD ts to be the upper

        // poll the recv oppotunistically. small batch should use sync way, while large batch should use async way
        if(!tsocket_recv(gtimer->rcc_master, buffer, lcu_buffer_size)) // large chunk arrives
        // async way
        {
            pthread_create(&gtimer->extract_thread, NULL, extract_data, &extract_struct);
        }
            
        // sync way for small request
        // {
        //     DataList* datalist = gtimer->datalist;
        //     batch_data* bd = (batch_data*)buffer;
        //     for(int i=0; i<bd->data_num; i++){
        //         int id = bd->request_id[i];
        //         ts_t timestamp = bd->timestamp[i];
        //         bool is_rc = bd->is_rc_data[i];
        //         if(is_rc){
        //             // printf("Lower %d\n", id);
        //             datalist->InsertRemoteLower(id, timestamp);
        //         }
                    
        //         else{
        //             // printf("Upper %d\n", id);
        //             datalist->InsertRemoteUpper(id, timestamp);
        //         }
                    
        //     }
        // }


        // wait for the duration passes
        probe_end = get_real_clock();
        
        int delta = interval - (int)(probe_end-probe_start)/1000; // in us, denoting the remaining time
        if(delta<0){
            fprintf(stdout, "Warning: sampling Frequency too large, exceeding %d us.\n", -delta);
        }
        else{
            // assert(delta>=0&&delta<=1000);
            if((delta<0 || delta>interval)){
                printf("Error delta: The delta is %d\n", delta);
                // assert(false);
                delta = 0;
            }
            usleep(delta);
        }

        request_id = (request_id+1)%max_request_id;

    }

    // send a final msg
    // we use -2 to denote end of the probing
    tsocket_send(gtimer->rcc_master, -2, buffer, 0, NULL);
    printf("Master thread quits\n");

    return NULL;

}

void* slave_run(void* argv){

    printf("Slave thread runs.\n");

    GlobalTimer* gtimer = (GlobalTimer*) argv;

    // the buffer array to do batch send
    batch_data buffer_array[MAX_PAIR];
    for(int i=0; i<MAX_PAIR; i++)
        buffer_array[i].data_num = 0;
    // receive related
    ts_t ts_array[ud_batch];
    int qp_idx_array[ud_batch];
    int request_id_array[ud_batch];

    int slave_count = gtimer->SlaveCount;

    // post recveiver for RC
    for(int i=0; i<slave_count; i++)
        tsocket_post_recv(gtimer->rcc_list[i], 0, ud_batch);
    // we don't need to allocate receiver for UD here, as it's already done in its creation function

    tsocket_context** slave_list = gtimer->rcc_list;
    tsocket_context_ud* udc = gtimer->udc;
    
    assert(slave_count==udc->opp_qpn-1);
    int finish_count=0;  // mark for finish
    bool is_end[MAX_PAIR];
    for(int i=0; i<MAX_PAIR; i++)
        is_end[i] = false;

    while(finish_count<slave_count){

        ts_t start = get_real_clock();
        // batch recv RC request
        for(int i=0; i<slave_count; i++){
            if(is_end[i])
                continue;
            // batch poll request from a single node
            int request_count = tsocket_poll_recv_rc(slave_list[i], ts_array, request_id_array);
            // put the message into the array
            for(int j=0; j<request_count; j++){
                // first see if remote send end message
                int request_id = request_id_array[j];
                if(request_id==-2){
                    assert(!is_end[i]);
                    is_end[i] = true;
                    finish_count++;
                    break;
                }
                buffer_array[i].request_id[buffer_array[i].data_num] = request_id;
                buffer_array[i].timestamp[buffer_array[i].data_num] = ts_array[j];
                buffer_array[i].is_rc_data[buffer_array[i].data_num] = true;
                buffer_array[i].data_num++;
                // if(i==0)
                //     printf("Insert lower %d\n", request_id);
                if(buffer_array[i].data_num==lcu_batch){
                    // a batch full, send the data to remote
                    tsocket_send(slave_list[i], -1, (char*)&buffer_array[i], sizeof(batch_data), NULL);
                    buffer_array[i].data_num = 0;
                }
            }
            if(!is_end[i])
                tsocket_post_recv(slave_list[i], 0, request_count);
        }

        // batch recv UD request
        int request_count = tsocket_poll_recv(udc, ts_array, qp_idx_array, request_id_array, slave_count);
        // put the message into the array
        for(int j=0; j<request_count; j++){
            // first see if remote send end message
            int request_id = request_id_array[j];
            int i = qp_idx_array[j]-1; // mind that this mapping exists
            if(is_end[i])  // this node has already close
                continue;
            buffer_array[i].request_id[buffer_array[i].data_num] = request_id;
            buffer_array[i].timestamp[buffer_array[i].data_num] = ts_array[j];
            buffer_array[i].is_rc_data[buffer_array[i].data_num] = false;
            buffer_array[i].data_num++;
            // if(i==0)
            //     printf("Insert upper %d\n", request_id);
            if(buffer_array[i].data_num==lcu_batch){
                // a batch full, send the data to remote
                tsocket_send(slave_list[i], -1, (char*)&buffer_array[i], sizeof(batch_data), NULL);
                buffer_array[i].data_num = 0;
            }
        }
        tsocket_recv_ud(udc, request_count);
        
        ts_t end = get_real_clock();
        
        ts_t res = slave_batch_time - (end-start)/1000;
        if(res>0)
            usleep(res);
    }

    printf("Slave thread quits\n");
    
    return NULL;
}

void* cpu_nic_cs_thread(void* argv){

    // bind the core in offline test
    cpu_set_t cpu;
    CPU_ZERO(&cpu);
    int cpu_id = 8;
    CPU_SET(cpu_id, &cpu);
    sched_setaffinity(0, sizeof(cpu), &cpu);

    // this thread periodically synchronizes cpu time and nic ts counter
    GlobalTimer* gtimer = (GlobalTimer*) argv;
    ClockManager* manager = gtimer->clockmanager_cpu;
    // step 1: do get nic test to get an upper bound of filter
    /* use fixed value now, to be done in the future */
    int ts_filter_bound = 2000;
    double ts_filter_percentage = 0.5;
    ts_t cpu_save[recalculation_epoch];
    ts_t NIC_save[recalculation_epoch];

    // step 2: when sync is not done, poll nic ts in certain interval
    ts_t cpu_ts, nic_ts, cpu_start, cpu_end;
    // some counting number
    int epoch_count = 0;
    int epoch_round = 0;
    long long int try_count = 0;
    long long int x_sum=0, y_sum=0;
    ts_t x_start = manager->get_cpu_time();
    ts_t y_start = manager->get_nic_time();

    while(1){
re_sample:
        epoch_count++;
        ts_t start_ts = get_real_clock();
        // find a probe small enough
        while(1){
            try_count++;
            // probe
            cpu_start = manager->get_cpu_time();
            nic_ts = manager->get_nic_time();
            cpu_end = manager->get_cpu_time();
            cpu_ts = (cpu_end + cpu_start)/2;

            #ifdef OUTPUT_MODE
            gtimer->datalist->cns_dot<<cpu_start<<" "<<cpu_end<<" "<<nic_ts<<std::endl;
            #endif

            #ifdef CNS_FILTER
            // int try_cc = 0;
            // if((cpu_end - cpu_start)<ts_filter_bound || try_cc>5){
            //     try_cc = 0;
            //     break;
            // }
            // try_cc++;
            if((cpu_end - cpu_start)<ts_filter_bound){
                break;
            }
            #else
            break;
            #endif
        }


        if(!gtimer->CNS_warmup_done && epoch_round>=CNS_warmup_epoch)
            gtimer->CNS_warmup_done = true;

        // add this dot
        x_sum += (cpu_ts - x_start);
        y_sum += (nic_ts - y_start);

        if(epoch_count%recalculation_epoch==0){

            epoch_round++;
            ts_t mid_x = x_sum/recalculation_epoch + x_start;
            ts_t mid_y = y_sum/recalculation_epoch + y_start;

            // add this point to GlobalTimer
            // printf("[CNS] Gonna insert dot %lld, %lld\n", mid_x, mid_y);
            // printf("CNS insert idx is %\n", ((int)((mid_x-manager->start_ts)/(CNS_interval*(int)1e9)))%RECORD_MAX);
            manager->InsertResDot(mid_x, mid_y);
            
            #ifdef OUTPUT_MODE
            gtimer->datalist->cns_info<<mid_x<<" "<<mid_y<<std::endl;
            gtimer->datalist->cns_dot<<"DEADBEAF"<<std::endl;
            #endif

            if(gtimer->is_done())
                break;

            x_sum = 0;
            y_sum = 0;

        }

        int dur = (int)(get_real_clock() - start_ts);
        int res = CNS_gap - dur/1000;
        if(res<0){
            fprintf(stdout, "Warning: CNS sampling Frequency too large, exceeding %d us.\n", -res);
        }
        else{
            // assert(delta>=0&&delta<=1000);
            if(res<0 || res>CNS_gap){
                printf("Error delta: The delta is %d\n", res);
                // assert(false);
                res = 0;
            }
            usleep(res);
        }
    }
    
    fprintf(stdout, "Epoch count %d, average try count %f\n", epoch_count, (double)(1.0*try_count/epoch_count));
    fprintf(stdout, "Nic-CPU-sync thread quits.\n");

    return NULL;

}

void* test_run(void* argv){

    GlobalTimer* ptr = (GlobalTimer*) argv;

    while(!ptr->is_warmup_done())
        sched_yield();

    char buff[1024];

    

    printf("Timestamp test runs\n");

    // define as inline function
    
    ts_t local_ts;
    ts_t send_ts;

    int rc;
    for(int i=0; i<TEST_TIMES; i++){
        rc = 1;
        while(rc)
            rc = rdma_recv(ptr->rcc_center, buff, 0, &local_ts);
        ts_t timestamp = ptr->get_timestamp(); // test software ts
        rdma_send(ptr->rcc_center, (char*)buff, 0, &send_ts);
        ts_t modified_ts = ptr->get_modify_ts(local_ts);
        msg2cent msg;
        msg.ts = modified_ts;
        msg.soft_ts = timestamp; // minus the poll
        msg.send_ts = send_ts;
        msg.recv_ts = local_ts;

        rdma_send(ptr->rcc_center, (char*)(&msg), sizeof(msg2cent), NULL);
    }

    printf("Timestamp test quits.\n");

    return NULL;

}

void GlobalTimer::run(){

    if(rcc_master)
        pthread_create(&master_thread, NULL, master_run, (void*)this);

    if(SlaveCount!=0)
        pthread_create(&slave_thread, NULL, slave_run, (void*)this);

    pthread_create(&cpu_thread, NULL, cpu_nic_cs_thread, (void*)this);

    #ifdef TEST_MODE
    pthread_create(&test_thread, NULL, test_run, (void*)this);
    #endif

    wait_warmup_done();

}

bool GlobalTimer::is_local_warmup_done(){

    if(!rcc_master)
        local_warmup_done = true;
    return local_warmup_done&&CNS_warmup_done;

}

bool GlobalTimer::is_warmup_done(){

    return global_warmup_done;

}

void GlobalTimer::wait_warmup_done(){

    // ts_t tp1 = get_real_clock();
    while(!is_local_warmup_done())
        sched_yield();
    // ts_t tp2 = get_real_clock();
    printf("local warmup done.\n");
    // send msg to center
    msg2cent msg;
    msg.is_warmup_done = true;
    msg.msg_id = 111;
    // send_to_center(&msg);
    send_to_center(&msg);
    // ts_t tp3 = get_real_clock();
    // recv msg from center
    msg2master msg2;
    recv_from_center(&msg2);
    // assert(msg2.msg_id==222);
    if(msg2.msg_id!=222){
        printf("Wrong msg id %d\n", msg2.msg_id);
        assert(false);
    }

    global_warmup_done = true;
    printf("global warmup done.\n");
    // ts_t tp4 = get_real_clock();

    // printf("[Inside Info] time duration are: %lld, %lld, %lld ms\n", (tp2-tp1)/1000000, (tp3-tp2)/1000000, (tp4-tp3)/1000000);

}

void GlobalTimer::finalize(){

    // join all sync connection
    if(rcc_master)
        pthread_join(master_thread, NULL);
        
    if(SlaveCount!=0)
        pthread_join(slave_thread, NULL);

    pthread_join(cpu_thread, NULL);

    #ifdef TEST_MODE
    pthread_join(test_thread, NULL);
    #endif

    // finalize sockets
    printf("Gonna finalize.\n");
    assert(!finalize_rdma(rcc_center));
    assert(finalize_tsocket_ud(udc)!=-1);
    if(rcc_master)
        assert(!finalize_tsocket(rcc_master));
    for(int i=0; i<SlaveCount; i++)
        assert(!finalize_tsocket(rcc_list[i]));

    if(rcc_list)
        free(rcc_list);

    datalist->finalize();
    delete datalist;

    clockmanager->finalize();
    delete clockmanager;

    clockmanager_cpu->finalize();
    delete clockmanager_cpu;

}

void GlobalTimer::set_done(){

    _is_done = true;

}

bool GlobalTimer::is_done(){

    return _is_done;

}

void GlobalTimer::send_to_center(msg2cent* msg){

    rdma_send(rcc_center, (char*)msg, sizeof(msg2cent), NULL);

}

void GlobalTimer::recv_from_center(msg2master* msg){

    int rc = 1;
    while(rc)
        rc = rdma_recv(rcc_center, (char*)msg, sizeof(msg2master), NULL);

}

// get real clock from CPU time
ts_t GlobalTimer::get_timestamp(){

    // step 1: get local cpu time
    ts_t cpu_time = clockmanager_cpu->get_cpu_time();
    // step 2: convert local cpu time to local NIC time
    ts_t nic_time = clockmanager_cpu->GetModifiedValue(cpu_time); 
    // step 3: convert local NIC time to remote NIC time
    ts_t timestamp = nic_time + clockmanager->GetModifiedValue(nic_time);

    return timestamp;

}

void GlobalTimer::get_ts_map(ts_t* x, ts_t* y){

    // step 1: get local cpu time
    ts_t cpu_time = clockmanager_cpu->get_cpu_time();
    // step 2: convert local cpu time to local NIC time
    ts_t nic_time = clockmanager_cpu->GetModifiedValue(cpu_time); 
    // step 3: convert local NIC time to remote NIC time
    ts_t timestamp = nic_time + clockmanager->GetModifiedValue(nic_time);

    *x = cpu_time;
    *y = timestamp;
    
}

void GlobalTimer::get_nic_ts_map(ts_t* x, ts_t* y){

    ts_t cpu_time = clockmanager_cpu->get_cpu_time();
    ts_t nic_time = clockmanager_cpu->GetModifiedValue(cpu_time); 
    ts_t timestamp = clockmanager->GetModifiedValue(nic_time);

    // we just want (NIC-local) and (NIC-remote - NIC-local)
    *x = nic_time;
    *y = timestamp;

}

// get real clock from NIC time
ts_t GlobalTimer::get_timestamp_nic(){

    ts_t nic_time = clockmanager->get_nic_time();
    return get_modify_ts(nic_time);

}

// get modified ts from a certain NIC time
ts_t GlobalTimer::get_modify_ts(ts_t input){

    return clockmanager->GetRealTime(input);

}

ts_t GlobalTimer::get_nic_time(){

    return clockmanager->get_nic_time();

}

//////////////
// GTCenter //
//////////////


GTCenter::GTCenter(char** addr_list, int* port_list, uint32_t addr_num){

    // the addr_list should not be free before connection built

    if(addr_num>MAX_SERVERS){
        
        fprintf(stderr, "Too many servers, please modify the constant MAX_SERVERS.\n");
        return;
    
    }

    server_num = addr_num;
    ip_list = addr_list;

    // build ts connection
    context_array = (rdma_context**) malloc(sizeof(rdma_context*)*server_num);

    for(uint32_t i=0; i<server_num; i++){
        int port = (int)(port_list[i]+ts_port_delta);
        printf("Connect to %s:%d ... ", addr_list[i], port);
        fflush(stdout);
        context_array[i] = create_rdma();
        assert(!rdma_connect(context_array[i], addr_list[i], port));
        printf("finish\n");
    }    
    // inform finish
    fprintf(stdout, "Connection built with %d servers.\n", addr_num);

}

void GTCenter::finalize(){

    for(uint32_t idx=0; idx<server_num; idx++){
        
        // delete sock_array[idx];

        if(finalize_rdma(context_array[idx]))

            printf("Error in releasing resource!!\n"); 
    
    }
    free(context_array);
    
    #ifdef TEST_MODE
    ts_file.close();
    #endif

}

void GTCenter::build_connection_config(int layout){

    // this function decide the layout of network and send msg to all master nodes.


    // center layout
    switch (layout){
    
    case CENTER_LAYOUT:
        // every node connects to master node 0
        for(unsigned int idx=0; idx<server_num; idx++){

            if(idx==0){

                int cnt = 0;
                for(unsigned int col=1; col<server_num; col++){
                    connection_slave[idx][cnt++] = col;
                }
                connection_slave[idx][cnt] = -1;

                connection_master[idx][0] = -1;
            }

            else{

                for(unsigned int row=1; row<server_num; row++){

                    connection_master[idx][0] = 0;
                    connection_master[idx][1] = -1;
                    connection_slave[idx][0] = -1;

                }

            }

        }
        break;
    
    case LINE_LAYOUT:
        // construct a line
        for(unsigned int idx=0; idx<server_num; idx++){

            if(idx!=server_num-1){
                connection_slave[idx][0] = (int)idx+1;
                connection_slave[idx][1] = -1;
            }
            else
                connection_slave[idx][0] = -1;


            if(idx!=0){
                connection_master[idx][0] = (int)idx-1;
                connection_master[idx][1] = -1;
            }
            else
                connection_master[idx][0] = -1;
 
        }        

        break;

    default:
        fprintf(stderr, "Net-work layout not recognizable.\n");
        return;
    }

    // send layout to master
    send_layout_to_master();

}

void GTCenter::send_layout_to_master(){

    char buff[1024];
    cent2master* msg = (cent2master*) buff;
    printf("Gonna send layout\n");
    // this function send layout to master according to the constructed layout
    for(uint32_t idx=0; idx<server_num; idx++){
        
        // rdma::socket* sock = sock_array[idx];
        rdma_context* _ctx = context_array[idx];

        // slave should hand out the ports

        int col = 0;
        while(connection_slave[idx][col]!=-1){

            int master_id = connection_slave[idx][col];
            msg->slave_port[col++] = GTCenter_base_port + master_id*server_num + idx;

        }
        msg->slave_port[col] = -1;
        msg->slave_port_num = col;

        // send master, this is simple as it could only be one
        if(connection_master[idx][0]!=-1){

            int slave_id = connection_master[idx][0];
            int use_port = GTCenter_base_port + idx*server_num + slave_id;
            strcpy(msg->master_ip, ip_list[slave_id]);
            msg->master_port = use_port;

        }
        else
            msg->master_port = -1;

        rdma_send(_ctx, (char*)buff, sizeof(cent2master), NULL);

        fprintf(stdout, "Send connection message to %s finish.\n", ip_list[idx]);

    }

}

void GTCenter::print_layout(){

    fprintf(stdout, "---- print layout ----\n");
    // slave layout
    for(unsigned int i=0; i<server_num; i++){
        fprintf(stdout, "%s: ", ip_list[i]);
        for(unsigned int j=0; j<server_num; j++){
            if(connection_slave[i][j]!=-1){
                fprintf(stdout, "<---|%s ", ip_list[connection_slave[i][j]]);
            }
            else
                break;
        }
        fprintf(stdout, "\n");
    }
    fprintf(stdout, "-------------\n");
    // master layout
    for(unsigned int i=0; i<server_num; i++){
        fprintf(stdout, "%s: ", ip_list[i]);
        for(unsigned int j=0; j<server_num; j++){
            if(connection_master[i][j]!=-1){
                fprintf(stdout, "|--->%s ", ip_list[connection_master[i][j]]);
            }
            else
                break;
        }
        fprintf(stdout, "\n");
    }
}

void GTCenter::collect_info(msg2cent* msg_array){

    for(uint32_t i=0; i<server_num; i++){
        int rc = 1;
        while(rc)
            rc = rdma_recv(context_array[i], (char*)(msg_array+i), sizeof(msg2cent), NULL);
    }

}

void GTCenter::broadcast_info(msg2master* msg_array){

    for(uint32_t i=0; i<server_num; i++){
        rdma_send(context_array[i], (char*)(msg_array+i), sizeof(msg2master), NULL);
    }

}

void GTCenter::test_hw_ts(long long int& hard_ts_sum, long long int& soft_ts_sum){
    
    ts_t send_ts[server_num];
    ts_t recv_ts[server_num];
    ts_t result_hw[server_num];
    ts_t result_sw[server_num];

    char buff[8];
    for(uint32_t i=0; i<server_num; i++){
        rdma_send(context_array[i], buff, 0, &send_ts[i]);
    }

    for(uint32_t i=0; i<server_num; i++){
        int rc=1;
        while(rc)
            rc = rdma_recv(context_array[i], buff, 0, &recv_ts[i]);
    }
    
    msg2cent msg_array[server_num];

    for(uint32_t i=0; i<server_num; i++){
        rdma_recv(context_array[i], (char*)(&msg_array[i]), sizeof(msg2cent), NULL);
    }

    // the start ts diff
    ts_t hw_ts, sw_ts;
    ts_t owd[server_num];
    for(uint32_t i=0; i<server_num; i++){

        // test hardware ts
        hw_ts = msg_array[i].ts;  
        // test software ts
        sw_ts = msg_array[i].soft_ts;
        // printf("***\n%lld\n%lld\n%lld\n%lld\n***\n", recv_ts[i], msg_array[i].send_ts, msg_array[i].recv_ts, send_ts[i]);
        owd[i] = -(recv_ts[i]-msg_array[i].send_ts+msg_array[i].recv_ts-send_ts[i])/2;
        // printf("OWD1 %lld, OWD2 %lld, ", msg_array[i].recv_ts-send_ts[i], msg_array[i].send_ts-recv_ts[i]);
        result_hw[i] = hw_ts - send_ts[i] - owd[i];
        result_sw[i] = sw_ts - send_ts[i] - owd[i];

    }
    // print result
    // printf("-------\n");
    for(uint32_t i=1; i<server_num; i++){
        // stat_ts_array[i-1][stat_pointer++] = res;
        // printf("(%lld, %lld)", result_hw[i]-result_hw[0], result_sw[i]-result_sw[0]);
        ts_t hw_err = result_hw[i] - result_hw[0];
        ts_t sw_err = result_sw[i] - result_sw[0];
        #ifdef OUTPUT_MODE
        ts_file<<hw_err<<" "<<sw_err<<std::endl;
        #endif
        hard_ts_sum += ts_abs(hw_err);
        soft_ts_sum += ts_abs(sw_err);
    }

}
