#ifndef _RDMA_TS_H_
#define _RDMA_TS_H_

// #include <rdma/rdma_verbs.h>
#include <infiniband/verbs.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include "semaphore.h"
#include <sys/socket.h>   
#include <netinet/in.h>   
#include <arpa/inet.h>   
#include <sys/types.h>
#include <sys/time.h>
#include <netdb.h>
#include <malloc.h>
#include <getopt.h>
#include <time.h>
#include <inttypes.h>
#include "global.h"

// potential statistic
#define STATIS

#ifdef STATIS
extern statistic clock_stat;
#endif

struct rdma_context {
	struct ibv_context	*context;
	struct ibv_comp_channel *channel;
	struct ibv_pd		*pd;
	struct ibv_mr		*mr;
	struct ibv_dm		*dm;
	union {
		struct ibv_cq		*cq;
		struct ibv_cq_ex	*cq_ex;
	} cq_s;
	struct ibv_qp		*qp;
	struct ibv_qp_ex	*qpx;
	char			*buf;
	int			 size;  // size of one buffer
	int 		 slot_num;  // number of slots
	int			 send_flags;
	int			 rx_depth;
	int			 pending;
	struct ibv_port_attr     portinfo;
	uint64_t		 completion_timestamp_mask;
	ibv_values_ex vex;
	mlx5dv_clock_info clock_info;
	int post_recv_window; // how many recv window left
};

struct pingpong_dest {
	int lid;
	int qpn;
	int psn;
	union ibv_gid gid;
};

struct rdma_context* create_rdma();

int finalize_rdma(struct rdma_context* ctx);

int rdma_bind(struct rdma_context* ctx, int port);

int rdma_connect(struct rdma_context* ctx, const char *ipaddr, int port);

int rdma_send(struct rdma_context* ctx, const char *buf, uint32_t len, long long int* timestamp);

int rdma_recv(struct rdma_context* ctx, char *buf, uint32_t len, long long int* timestamp);

#endif
