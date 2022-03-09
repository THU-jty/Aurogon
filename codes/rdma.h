//
// Created by prince_de_marcia on 2021/3/25.
//

#ifndef CLOCK_MODEL_RDMA_H
#define CLOCK_MODEL_RDMA_H

#include "config.h"



struct cm_con_data_t {
    uint32_t 			qp_num;		/* QP number */
    uint16_t 			lid;		/* LID of the IB port */
    uint8_t     		remoteGid[16];  /* GID  */
};

class RDMA{
public:
    ll node_id;
    ll thd_id;

    struct ibv_context *ctx;
    struct ibv_pd *pd;
    struct ibv_qp **qp;
    struct ibv_cq* send_cq;
    struct ibv_cq* recv_cq;
    struct ibv_comp_channel *comp_channel;
    struct ibv_port_attr port_attr;
    int gidIndex;
    union ibv_gid gid;

    struct ibv_mr **rdma_send_mr;
    struct ibv_mr **rdma_recv_mr;

    char ** rdma_recv_region;
    char ** rdma_send_region;

    int send_signal_cnt;

    struct ibv_recv_wr *wr;
    struct ibv_sge *sge;
    // to expand
    // struct ibv_mr peer_mr;

    void init( ll _node_id, ll _thd_id );
    void send( ll node_id, ll tid, char* ptr, int size, int imm_data );
    void recv( ll nid, ll tid, char* ptr, int size );
    void destroy();
};



#endif //CLOCK_MODEL_RDMA_H
