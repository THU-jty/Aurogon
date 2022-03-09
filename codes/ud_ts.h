#ifndef _UD_TS_H
#define _UD_TS_H

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netdb.h>
#include <malloc.h>
#include <getopt.h>
#include <arpa/inet.h>
#include <time.h>
#include <stdint.h>
#include <cassert>
#include <infiniband/verbs.h>
#include <infiniband/mlx5dv.h>
#include <unordered_map>
#include <iostream>
#include <errno.h>
#include "global.h"
#include "semaphore.h"

#define GRH_SIZE    40

typedef long long int ts_t;

struct pingpong_dest_ud {
	int lid;
	int qpn;
	int psn;
	union ibv_gid gid;
};

struct tsocket_context_ud {
	struct ibv_context	*context;
	struct ibv_comp_channel *channel;
	struct ibv_pd		*pd;
	struct ibv_mr		*mr;
	struct ibv_cq		*cq_send;
	struct ibv_cq_ex    *cq_ex_send;
	struct ibv_cq		*cq_recv;
	struct ibv_cq_ex    *cq_ex_recv;
	struct ibv_qp		*qp;
	struct ibv_qp_ex    *qpx;
    struct pingpong_dest_ud my_dest;  // this node's qp info structure
    struct pingpong_dest_ud**rem_dest; // the list of oppo nodes' qp info structure
	struct ibv_ah		**ah_list;
	int opp_qpn;
    int gidx;
    int ib_port;
	int has_master;  // we fix the 0 pq idx for master, and slaves start from idx 1. Even without master, slaves still start from 1
	void            *grh_buf;
	void			*buf;
	unsigned long long			 size;
	int			 send_flags;
	int			 rx_depth;
	int			 pending;
	struct ibv_port_attr     portinfo;
	int             mtu;
	void            *buf_curr;
	uint64_t		 completion_timestamp_mask;
	ibv_values_ex vex;
	mlx5dv_clock_info clock_info;
    int page_size;
	sem_t mutex_lock;  // to provide atomic operation on opp_qpn
	std::unordered_map<int, int> mapping_table;  // a mapping table from qpn to node_id
};

struct tsocket_context_ud* create_tsocket_ud();

int finalize_tsocket_ud(struct tsocket_context_ud* ctx);

// these two function return -1 when fail, and return qp_idx when success(start from 0)
int tsocket_bind_ud(struct tsocket_context_ud* ctx, int port);
int tsocket_connect_ud(struct tsocket_context_ud* ctx, const char *ipaddr, int port);

// the following 2 function return -1 when fail, and return the opposite qpn when success
// the send function should pass the qp_idx to handle multi-qp scenario
int tsocket_send_ud(struct tsocket_context_ud* ctx, int qp_idx, int request_id, const char *buf, uint32_t len);
void tsocket_recv_ud(struct tsocket_context_ud* ctx, int count);

// the following 2 function return -1 when fail, and return the number of polled items when success
int tsocket_poll_send(struct tsocket_context_ud* ctx, ts_t* ts_array, int* qp_idx_array);
int tsocket_poll_recv(struct tsocket_context_ud* ctx, ts_t* ts_array, int* qp_idx_array, int* request_id_array, int factor);

#endif