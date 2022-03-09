//
// Created by prince_de_marcia on 2021/3/25.
//

#include "rdma.h"

int modify_qp_to_init(struct  ibv_qp* qp);
int modify_qp_to_rtr(struct ibv_qp *qp,	uint32_t remote_qpn,
                     uint16_t dlid, uint8_t *remoteGid);
void fillAhAttr(ibv_ah_attr *attr, uint32_t remoteLid,
                uint8_t *remoteGid);
int modify_qp_to_rts(struct ibv_qp *qp);

void RDMA::send( ll nid, ll tid, char* ptr, int size, int imm_data ) {
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));

    wr.wr_id = tid;

    if (imm_data != 0)
        wr.opcode = IBV_WR_SEND_WITH_IMM;
    else wr.opcode = IBV_WR_SEND;
    wr.sg_list = &sge;

    // first for sync clock
    send_signal_cnt++;
    if (send_signal_cnt == 10) {
        wr.send_flags = IBV_SEND_SIGNALED;
        send_signal_cnt = 0;
    }

    if( imm_data != 0 )  wr.imm_data = imm_data;
    wr.num_sge = 1;

    sge.addr = (uint64_t)ptr;
    sge.length = size;
    sge.lkey = rdma_send_mr[nid]->lkey;

    if(   ptr < rdma_send_region[nid]
          ||  ptr+size > rdma_send_region[nid]+max_cq_num*rdma_buffer_size )
        printf("xxxxxxxxxxxxxxxxxxxxxx\n");

    if( ibv_post_send(qp[nid], &wr, &bad_wr) == -1 ){
        printf("T%lld: %p %d rg %p %d\n", ptr, size,
                rdma_send_region[nid], max_cq_num*rdma_buffer_size);
    }
//    TEST_NZ(ibv_post_send(qp[nid], &wr, &bad_wr));
}

void RDMA::recv( ll nid, ll tid, char* ptr, int size ){
    struct ibv_recv_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));

    wr.wr_id = tid;

    wr.sg_list = &sge;

    wr.num_sge = 1;

    sge.addr = (uint64_t)ptr;
    sge.length = size;
    sge.lkey = rdma_recv_mr[nid]->lkey;

    int ret = ibv_post_recv(qp[nid], &wr, &bad_wr);
    if( ret ){
        assert(0==1);
    }
//    TEST_NZ();
}

void RDMA::destroy(){

}

void RDMA::init( ll _node_id, ll _thd_id ) {
    node_id = _node_id;
    thd_id = _thd_id;

    send_signal_cnt = 0;

    struct ibv_device **dev_list = NULL;
    int num_devices;
    struct ibv_device *ib_dev = NULL;
    char dev_name[] = "mlx5_0";

    dev_list = ibv_get_device_list(&num_devices);
    if (!dev_list) {
        fprintf(stdout, "N%lldT%lld: failed to get IB devices list\n", node_id, thd_id);
        assert(false);
    }

    /* if there isn't any IB device in host */
    if (!num_devices) {
        fprintf(stderr, "found %d device(s)\n", num_devices);
        assert(false);
    }

    int device_index;
    for (device_index = 0; device_index < num_devices; device_index++) {
        ib_dev = dev_list[device_index];
        //printf("N%lldT%lld:  %s\n", node_id, thd_id, ibv_get_device_name(dev_list[device_index]) );
        if( strcmp( ibv_get_device_name(dev_list[device_index]), dev_name ) ) continue;

        /* get device handle */
        ctx = ibv_open_device(ib_dev);
        if (!ctx) {
            fprintf(stdout, "N%lldT%lld: failed to open device %s\n", node_id, thd_id, dev_name);
            continue;
        }

        /* query port properties  */
        if (ibv_query_port(ctx, ib_port, &port_attr)) {
            fprintf(stdout, "N%lldT%lld: ibv_query_port on port %lld failed\n", node_id, thd_id, ib_port);
            continue;
        }


        if (port_attr.state == IBV_PORT_ACTIVE) {
            break;
        }
    }

    ibv_free_device_list(dev_list);

    gidIndex = GID;
    if (ibv_query_gid(ctx, ib_port, GID, &gid)) {
        fprintf(stderr, "ibv_query_gid on port %lld gid %lld failed\n", ib_port, GID);
        assert(false);
    }

    pd = ibv_alloc_pd(ctx);
    TEST_Z(pd);

    /*init RDMA memory region*/
    send_cq = ibv_create_cq(ctx, cq_size, NULL, NULL, 0);
    recv_cq = ibv_create_cq(ctx, cq_size, NULL, NULL, 0);

    rdma_recv_region = (char **)malloc( sizeof(char*)*node_cnt );
    rdma_send_region = (char **)malloc( sizeof(char*)*node_cnt );

    wr = (struct ibv_recv_wr *)malloc(sizeof(struct ibv_recv_wr)*one_epoch_recv_wr);
    sge = (struct ibv_sge *)malloc(sizeof(struct ibv_sge)*one_epoch_recv_wr);

    for( ll i = 0; i < node_cnt; i ++ ) {
        rdma_recv_region[i] = (char *) malloc(rdma_buffer_size * max_cq_num);
        rdma_send_region[i] = (char *) malloc(rdma_buffer_size * max_cq_num);
        //DEBUG("node %lld send %p size %lld\n",\
                i, rdma_send_region[i], rdma_buffer_size * max_cq_num);
    }

    rdma_recv_mr = (struct ibv_mr **)malloc( sizeof(struct ibv_mr *)*node_cnt );
    rdma_send_mr = (struct ibv_mr **)malloc( sizeof(struct ibv_mr *)*node_cnt );

    for( ll i = 0; i < node_cnt; i ++ ){
        rdma_recv_mr[i] = ibv_reg_mr(pd, rdma_recv_region[i], rdma_buffer_size * max_cq_num,
                                  IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE
                                  | IBV_ACCESS_REMOTE_READ);
        rdma_send_mr[i] = ibv_reg_mr(pd, rdma_send_region[i], rdma_buffer_size * max_cq_num,
                                  IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE
                                  | IBV_ACCESS_REMOTE_READ);
    }

    /*init RDMA qp*/
    struct ibv_qp_init_attr *qp_attr;
    qp_attr = (struct ibv_qp_init_attr *) malloc(sizeof(struct ibv_qp_init_attr));
    memset(qp_attr, 0, sizeof(*qp_attr));
    qp_attr->qp_type = IBV_QPT_RC;
    qp_attr->send_cq = send_cq;
    qp_attr->recv_cq = recv_cq;

    qp_attr->cap.max_send_wr = qp_size;
    qp_attr->cap.max_recv_wr = qp_size;
    qp_attr->cap.max_send_sge = 20;
    qp_attr->cap.max_recv_sge = 20;
    qp_attr->cap.max_inline_data = 200; // max size in byte of inline data on the send queue


    qp_attr->sq_sig_all = 0; // set as 1 to generate CQE from all WQ

    qp = (struct ibv_qp **)malloc( sizeof(struct ibv_qp *)*node_cnt );

    memcached_st *client = NULL;
    memcached_return cache_return;
    memcached_server_st *server = NULL;
    client = memcached_create(NULL);
    server = memcached_server_list_append(server,
                                          MemcacheAddr.c_str(), MemcachePort, &cache_return);
    cache_return = memcached_server_push(client, server);

    for( ll i = 0; i < node_cnt; i ++ ){
        if( i == my_node_id ) continue;
        qp[i] = ibv_create_qp(pd, qp_attr);
        int rc;
        rc = modify_qp_to_init(qp[i]);
        if (rc) {
            fprintf(stderr, "change QP state to INIT failed\n");
            assert(false);
        }

        struct cm_con_data_t local_con_data;

        local_con_data.qp_num = qp[i]->qp_num;
        local_con_data.lid = port_attr.lid;
        memcpy(local_con_data.remoteGid, gid.raw, 16 * sizeof(uint8_t));


        if(cache_return != MEMCACHED_SUCCESS){
            fprintf(stdout, "N%lldT%lld: memcached server establish failed!\n", node_id, thd_id);
            return;
        }

        int expiration = 0;
        uint32_t flags = 0;

        std::string key;
        std::ostringstream s1, s2, s3;
        s1 << node_id;
        s2 << thd_id;
        s3 << i;
        key = s1.str()
              +"_"+s2.str()
              +"_"+s3.str();
//        DEBUG("N%lldT%lld: set string %s\n", node_id, thd_id, key.c_str());

        char* value_p = (char *)(&local_con_data);

        cache_return = memcached_set(client, key.c_str(), key.length(),
                                     value_p, sizeof(struct cm_con_data_t),
                                     expiration, flags);

        if(cache_return != MEMCACHED_SUCCESS){
            fprintf(stdout, "N%lldT%lld: memcached server set failed!\n", node_id, thd_id);
            return;
        }

//        DEBUG("N%lldT%lld: rdma%lld initphase1 completed\n", node_id, thd_id, i);
    }

    for( ll i = 0; i < node_cnt; i ++ ) {
        if( i == my_node_id ) continue;
        struct cm_con_data_t remote_con_data;
        struct cm_con_data_t* tmp_con_data;

        std::string key;
        char* value_p;
        size_t get_size;
        uint32_t flags = 0;

        std::ostringstream s1, s2, s3;
        s1 << node_id;
        s2 << thd_id;
        s3 << i;
        key = s3.str()
              +"_"+s2.str()
              +"_"+s1.str();

        //DEBUG("N%lldT%lld: get string %s\n", node_id, thd_id, key.c_str());
        while(1) {
            value_p = memcached_get(client, key.c_str(), key.length(),
                                    &get_size, &flags, &cache_return);
            if( cache_return == MEMCACHED_SUCCESS ) break;
            else{
                sched_yield();
            }
        }

        tmp_con_data = (struct cm_con_data_t *)value_p;

        //DEBUG("N%lldT%lldZ%lld get qp %d lid %d\n", node_id, thd_id, i\
                tmp_con_data->qp_num, tmp_con_data->lid);

        remote_con_data.qp_num = tmp_con_data->qp_num;
        remote_con_data.lid    = tmp_con_data->lid;
        memcpy( remote_con_data.remoteGid, tmp_con_data->remoteGid, 16*sizeof(uint8_t) );

        int rc;
        rc = modify_qp_to_rtr(qp[i], remote_con_data.qp_num,
                              remote_con_data.lid, remote_con_data.remoteGid);
        if (rc) {
            fprintf(stderr, "failed to modify QP state from RESET to RTS\n");
            assert(false);
        }


        rc = modify_qp_to_rts(qp[i]);
        if (rc) {
            fprintf(stderr, "failed to modify QP state from RESET to RTS\n");
            assert(false);
        }

        fprintf(stdout, "N%lldT%lld: qp to %lld connect.\n", node_id, thd_id, i);
    }
}

int modify_qp_to_init(struct  ibv_qp* qp){

    struct ibv_qp_attr 	attr;
    int 			flags;
    int 			rc;

    /* do the following QP transition: RESET -> INIT */
    memset(&attr, 0, sizeof(attr));

    attr.qp_state 	= IBV_QPS_INIT;
    attr.port_num 	= ib_port;
    attr.pkey_index = 0;

    /* we don't do any RDMA operation, so remote operation is not permitted */
    attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE  |
            IBV_ACCESS_REMOTE_READ  |
            IBV_ACCESS_REMOTE_ATOMIC ;

    flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX
            | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;

    rc = ibv_modify_qp(qp, &attr, flags);
    if (rc) {
        fprintf(stderr, "failed to modify QP state to INIT\n");
        return rc;
    }
    return 0;
}

int modify_qp_to_rtr(struct ibv_qp *qp,	uint32_t remote_qpn,
        uint16_t dlid, uint8_t *remoteGid){

    struct ibv_qp_attr 	attr;
    int 			flags;
    int 			rc;

    /* do the following QP transition: INIT -> RTR */
    memset(&attr, 0, sizeof(attr));

    attr.qp_state 			= IBV_QPS_RTR;
    attr.path_mtu 			= IBV_MTU_256;
    attr.dest_qp_num 		= remote_qpn;
    attr.rq_psn 			= 0;
    attr.max_dest_rd_atomic 	= 1;
    attr.min_rnr_timer 		= 0x12;


    fillAhAttr(&attr.ah_attr, dlid, remoteGid);

    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
            IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    rc = ibv_modify_qp(qp, &attr, flags);
    if (rc) {
        fprintf(stderr, "failed to modify QP state to RTR %d \n", rc);
        printf("%s\n", strerror(rc));
        return rc;
    }

    return 0;
}

void fillAhAttr(ibv_ah_attr *attr, uint32_t remoteLid,
        uint8_t *remoteGid){

    memset(attr, 0, sizeof(ibv_ah_attr));
    attr->dlid = remoteLid;
    attr->sl = 0;
    attr->src_path_bits = 0;
    attr->port_num = ib_port;

    //attr->is_global = 0;

    // fill ah_attr with GRH

    attr->is_global = 1;
    memcpy(&attr->grh.dgid, remoteGid, 16);
    attr->grh.flow_label = 0;
    attr->grh.hop_limit = 1;
    attr->grh.sgid_index = GID;
    attr->grh.traffic_class = 0;
}

int modify_qp_to_rts(struct ibv_qp *qp){

    struct ibv_qp_attr 	attr;
    int 			flags;
    int 			rc;


    /* do the following QP transition: RTR -> RTS */
    memset(&attr, 0, sizeof(attr));

    attr.qp_state 		= IBV_QPS_RTS;
    attr.timeout 		= 0x12;
    attr.retry_cnt 		= 7;
    attr.rnr_retry 		= 7;
    attr.sq_psn 		= 0;
    attr.max_rd_atomic 	= 1;

    flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
            IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;

    rc = ibv_modify_qp(qp, &attr, flags);
    if (rc) {
        fprintf(stderr, "failed to modify QP state to RTS\n");
        return rc;
    }

    return 0;
}