#ifndef _RC_TS_H_
#define _RC_TS_H_

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
#include <stack>

// potential statistic
#define STATIS

#ifdef STATIS
extern statistic clock_stat;
#endif

struct tsocket_context {
	struct ibv_context	*context;
	struct ibv_comp_channel *channel;
	struct ibv_pd		*pd;
	struct ibv_mr		*mr;
	struct ibv_mr		*mr_large;
	struct ibv_dm		*dm;
	union {
		struct ibv_cq		*cq;
		struct ibv_cq_ex	*cq_ex;
	} cq_send;
	union {
		struct ibv_cq		*cq;
		struct ibv_cq_ex	*cq_ex;
	} cq_recv;
	struct ibv_qp		*qp;
	struct ibv_qp_ex	*qpx;
	char			*buf; // this is a small buff, not used for big msg
	char			*buf_large;
	int			 size;  // size of large buffer(small buffer is fixed for 8)
	int			 send_flags;
	int			 pending;
	struct ibv_port_attr     portinfo;
	uint64_t		 completion_timestamp_mask;
	ibv_values_ex vex;
	mlx5dv_clock_info clock_info;
	std::stack<int>* recv_stack;
	sem_t mutex_lock;
};

struct pingpong_dest_rc {
	int lid;
	int qpn;
	int psn;
	union ibv_gid gid;
};

struct tsocket_context* create_tsocket();

int finalize_tsocket(struct tsocket_context* ctx);

int tsocket_bind(struct tsocket_context* ctx, int port);

int tsocket_connect(struct tsocket_context* ctx, const char *ipaddr, int port);

// following are two types of communication: probe data and batch data. They are differed by request ID. large chunk has request ID -1; The valid request ID should accordingly not exceed 2147483647
// RC send and recv only handle inline data(32 bits)

// batch RC send and recv can handle large data
void tsocket_send(struct tsocket_context* ctx, int request_id, const char *buf, uint32_t len, long long int* timestamp);

// return request ID in probe mode. We seperate recv and poll for batch operation
// this is used for large request
int tsocket_recv(struct tsocket_context* ctx, char *buf, uint32_t len);
// this is used for small request
void tsocket_post_recv(struct tsocket_context* ctx, int type, int count);
int tsocket_poll_recv_rc(struct tsocket_context* ctx, ts_t* ts_array, int* request_id_array);

#endif
