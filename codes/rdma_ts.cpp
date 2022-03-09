#include"rdma_ts.h"

static void wire_gid_to_gid(const char *wgid, union ibv_gid *gid)
{
	char tmp[9];
	__be32 v32;
	int i;
	uint32_t tmp_gid[4];

	for (tmp[8] = 0, i = 0; i < 4; ++i) {
		memcpy(tmp, wgid + i * 8, 8);
		sscanf(tmp, "%x", &v32);
		tmp_gid[i] = be32toh(v32);
	}
	memcpy(gid, tmp_gid, sizeof(*gid));
}

static void gid_to_wire_gid(const union ibv_gid *gid, char wgid[])
{
	uint32_t tmp_gid[4];
	int i;

	memcpy(tmp_gid, gid, sizeof(tmp_gid));
	for (i = 0; i < 4; ++i)
		sprintf(&wgid[i * 8], "%08x", htobe32(tmp_gid[i]));
}

static int pp_connect_ctx(struct rdma_context *ctx, int port, int my_psn,
			  enum ibv_mtu mtu, int sl,
			  struct pingpong_dest *dest, int sgid_idx)
{
	struct ibv_qp_attr attr;
	memset(&attr, 0, sizeof(attr));
	attr.qp_state		= IBV_QPS_RTR;
	attr.path_mtu		= mtu;
	attr.dest_qp_num		= dest->qpn;
	attr.rq_psn			= dest->psn;
	attr.max_dest_rd_atomic	= 1;
	attr.min_rnr_timer		= 0x12;
	attr.ah_attr.is_global	= 0;
	attr.ah_attr.dlid		= dest->lid;
	attr.ah_attr.sl		= sl;
	attr.ah_attr.src_path_bits	= 0;
	attr.ah_attr.port_num	= port;

	if (dest->gid.global.interface_id) {
		attr.ah_attr.is_global = 1;
		attr.ah_attr.grh.hop_limit = 1;
		attr.ah_attr.grh.dgid = dest->gid;
		attr.ah_attr.grh.sgid_index = sgid_idx;
	}
	if (ibv_modify_qp(ctx->qp, &attr,
			  IBV_QP_STATE              |
			  IBV_QP_AV                 |
			  IBV_QP_PATH_MTU           |
			  IBV_QP_DEST_QPN           |
			  IBV_QP_RQ_PSN             |
			  IBV_QP_MAX_DEST_RD_ATOMIC |
			  IBV_QP_MIN_RNR_TIMER)) {
		fprintf(stderr, "Failed to modify QP to RTR\n");
		return 1;
	}

	attr.qp_state	    = IBV_QPS_RTS;
	attr.timeout	    = 0x12;
	attr.retry_cnt	    = 7;
	attr.rnr_retry	    = 7;
	attr.sq_psn	    = my_psn;
	attr.max_rd_atomic  = 1;
	if (ibv_modify_qp(ctx->qp, &attr,
			  IBV_QP_STATE              |
			  IBV_QP_TIMEOUT            |
			  IBV_QP_RETRY_CNT          |
			  IBV_QP_RNR_RETRY          |
			  IBV_QP_SQ_PSN             |
			  IBV_QP_MAX_QP_RD_ATOMIC)) {
		fprintf(stderr, "Failed to modify QP to RTS\n");
		return 1;
	}

	return 0;
}


static struct pingpong_dest *pp_client_exch_dest(const char *servername, int port,
						 const struct pingpong_dest *my_dest)
{
	struct addrinfo *res, *t;
	struct addrinfo hints;
	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	char *service;
	char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
	int n;
	int sockfd = -1;
	struct pingpong_dest *rem_dest = NULL;
	char gid[33];

	if (asprintf(&service, "%d", port) < 0)
		return NULL;

	n = getaddrinfo(servername, service, &hints, &res);

	if (n < 0) {
		fprintf(stderr, "%s for %s:%d\n", gai_strerror(n), servername, port);
		free(service);
		return NULL;
	}

	for (t = res; t; t = t->ai_next) {
		sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
		if (sockfd >= 0) {
			while(connect(sockfd, t->ai_addr, t->ai_addrlen)){
				usleep(100);
			}
			// close(sockfd);
			// sockfd = -1;
		}
	}

	freeaddrinfo(res);
	free(service);

	if (sockfd < 0) {
		fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
		return NULL;
	}

	gid_to_wire_gid(&my_dest->gid, gid);
	sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn,
							my_dest->psn, gid);
	if (write(sockfd, msg, sizeof msg) != sizeof msg) {
		// fprintf(stderr, "Couldn't send local address\n");
		goto out;
	}

	if (read(sockfd, msg, sizeof msg) != sizeof msg ||
	    write(sockfd, "done", sizeof "done") != sizeof "done") {
		perror("client read/write");
		fprintf(stderr, "Couldn't read/write remote address\n");
		goto out;
	}

	rem_dest = (struct pingpong_dest*)malloc(sizeof *rem_dest);
	if (!rem_dest)
		goto out;

	sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn,
						&rem_dest->psn, gid);
	wire_gid_to_gid(gid, &rem_dest->gid);

out:
	close(sockfd);
	return rem_dest;
}

static struct pingpong_dest *pp_server_exch_dest(struct rdma_context *ctx,
						 int port, const struct pingpong_dest *my_dest)
{
	struct addrinfo *res, *t;
	struct addrinfo hints;
    memset( &hints, 0, sizeof(struct addrinfo) );
    hints.ai_flags = AI_PASSIVE;
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
	char *service;
	char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
	int n;
	int sockfd = -1, connfd;
	struct pingpong_dest *rem_dest = NULL;
	char gid[33];

	if (asprintf(&service, "%d", port) < 0)
		return NULL;

	n = getaddrinfo(NULL, service, &hints, &res);

	if (n < 0) {
		fprintf(stderr, "%s for port %d\n", gai_strerror(n), port);
		free(service);
		return NULL;
	}

	for (t = res; t; t = t->ai_next) {
		sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
		if (sockfd >= 0) {
			n = 1;

			setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof n);

			if (!bind(sockfd, t->ai_addr, t->ai_addrlen))
				break;
			close(sockfd);
			sockfd = -1;
		}
	}

	freeaddrinfo(res);
	free(service);

	if (sockfd < 0) {
		fprintf(stderr, "Couldn't listen to port %d\n", port);
		return NULL;
	}

	listen(sockfd, 1);
	connfd = accept(sockfd, NULL, NULL);
	close(sockfd);
	if (connfd < 0) {
		fprintf(stderr, "accept() failed\n");
		return NULL;
	}

	n = read(connfd, msg, sizeof msg);
	if (n != sizeof msg) {
		perror("server read");
		fprintf(stderr, "%d/%d: Couldn't read remote address\n", n, (int) sizeof msg);
		goto out;
	}

	rem_dest = (struct pingpong_dest*)malloc(sizeof *rem_dest);
	if (!rem_dest)
		goto out;

	sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn,
							&rem_dest->psn, gid);
	wire_gid_to_gid(gid, &rem_dest->gid);

	if (pp_connect_ctx(ctx, 1, 0, IBV_MTU_1024, 0, rem_dest,
								0)) {
		fprintf(stderr, "Couldn't connect to remote QP\n");
		free(rem_dest);
		rem_dest = NULL;
		goto out;
	}


	gid_to_wire_gid(&my_dest->gid, gid);
	sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn,
							my_dest->psn, gid);
	if (write(connfd, msg, sizeof msg) != sizeof msg ||
	    read(connfd, msg, sizeof msg) != sizeof "done") {
		fprintf(stderr, "Couldn't send/recv local address\n");
		free(rem_dest);
		rem_dest = NULL;
		goto out;
	}


out:
	close(connfd);
	return rem_dest;
}


struct rdma_context* create_rdma(){
	
    // socket initializing //
	struct ibv_device      **dev_list;
	struct ibv_device	*ib_dev;
	unsigned int             size = 1024;// need to be smaller !!! harm accuracy
	int 					slot_num=8;  // 0-9 for send, 10-19 for recv
	// unsigned int             size = 1024*1025;
	unsigned int             rx_depth = slot_num/2; // should be half of the slot num
	struct rdma_context* ctx;


	dev_list = ibv_get_device_list(NULL);

	ib_dev = *dev_list;

	// init ctx -------------
	// ctx = pp_init_ctx(ib_dev, size, rx_depth, ib_port, use_event);
	int access_flags = IBV_ACCESS_LOCAL_WRITE;

	ctx = (struct rdma_context*)calloc(1, sizeof *ctx);
	if (!ctx){
		printf("Alloc ctx failed!\n");
		return NULL;
	}

	ctx->size       = size;
	ctx->send_flags = IBV_SEND_SIGNALED;
	ctx->rx_depth   = rx_depth;
	ctx->post_recv_window = rx_depth;
	ctx->slot_num 	= slot_num;

	ctx->buf = (char*)memalign(sysconf(_SC_PAGESIZE), size*slot_num);
	if (!ctx->buf) {
		fprintf(stderr, "Couldn't allocate work buf.\n");
		goto clean_ctx;
	}

	memset(ctx->buf, 0x7b, size*slot_num);

	ctx->context = ibv_open_device(ib_dev);
	if (!ctx->context) {
		fprintf(stderr, "Couldn't get context for %s\n",
			ibv_get_device_name(ib_dev));
		goto clean_buffer;
	}

	ctx->channel = NULL;

	ctx->pd = ibv_alloc_pd(ctx->context);
	if (!ctx->pd) {
		fprintf(stderr, "Couldn't allocate PD\n");
		goto clean_comp_channel;
	}

	

		struct ibv_device_attr_ex attrx;

		if (ibv_query_device_ex(ctx->context, NULL, &attrx)) {
			fprintf(stderr, "Couldn't query device for its features\n");
			goto clean_pd;
		}


		if (!attrx.completion_timestamp_mask) {
			fprintf(stderr, "The device isn't completion timestamp capable\n");
			goto clean_pd;
		}
		ctx->completion_timestamp_mask = attrx.completion_timestamp_mask;
		
	

	ctx->mr = ibv_reg_mr(ctx->pd, ctx->buf, size*slot_num, access_flags);

	if (!ctx->mr) {
		fprintf(stderr, "Couldn't register MR\n");
		goto clean_dm;
	}

	struct ibv_cq_init_attr_ex attr_ex;
	memset(&attr_ex, 0, sizeof(ibv_cq_init_attr_ex));
	attr_ex.cqe = rx_depth + 1;
	attr_ex.cq_context = NULL;
	attr_ex.channel = NULL;
	attr_ex.comp_vector = 0;
	attr_ex.wc_flags = IBV_WC_EX_WITH_COMPLETION_TIMESTAMP | IBV_WC_EX_WITH_COMPLETION_TIMESTAMP_WALLCLOCK;
	assert(ctx->context);
	ctx->cq_s.cq_ex = ibv_create_cq_ex(ctx->context, &attr_ex);
	ctx->cq_s.cq = ibv_cq_ex_to_cq(ctx->cq_s.cq_ex);

	if (!ctx->cq_s.cq) {
		fprintf(stderr, "Couldn't create CQ\n");
		goto clean_mr;
	}

	{

		// if (use_new_send) {
			struct ibv_qp_init_attr_ex init_attr_ex = {};
			memset(&init_attr_ex, 0, sizeof(ibv_qp_init_attr_ex));

			init_attr_ex.send_cq = ctx->cq_s.cq;
			init_attr_ex.recv_cq = ctx->cq_s.cq;
			init_attr_ex.cap.max_send_wr = 1;
			init_attr_ex.cap.max_recv_wr = rx_depth;
			init_attr_ex.cap.max_send_sge = 1;
			init_attr_ex.cap.max_recv_sge = 1;
			init_attr_ex.qp_type = IBV_QPT_RC;

			init_attr_ex.comp_mask |= IBV_QP_INIT_ATTR_PD |
						  IBV_QP_INIT_ATTR_SEND_OPS_FLAGS;
			init_attr_ex.pd = ctx->pd;
			init_attr_ex.send_ops_flags = IBV_QP_EX_WITH_SEND;

			ctx->qp = ibv_create_qp_ex(ctx->context, &init_attr_ex);

		if (!ctx->qp)  {
			fprintf(stderr, "Couldn't create QP\n");
			goto clean_cq;
		}

		ctx->qpx = ibv_qp_to_qp_ex(ctx->qp);

	}

	{
		struct ibv_qp_attr attr;
		memset(&attr, 0, sizeof(ibv_qp_attr));

		attr.qp_state = IBV_QPS_INIT;
		attr.pkey_index = 0;
		attr.port_num = 1;
		attr.qp_access_flags = 0;
		
		if (ibv_modify_qp(ctx->qp, &attr,
				  IBV_QP_STATE              |
				  IBV_QP_PKEY_INDEX         |
				  IBV_QP_PORT               |
				  IBV_QP_ACCESS_FLAGS)) {
			fprintf(stderr, "Failed to modify QP to INIT\n");
			goto clean_qp;
		}
	}

	goto next_step;

	clean_qp:
		ibv_destroy_qp(ctx->qp);

	clean_cq:
		ibv_destroy_cq(ctx->cq_s.cq);

	clean_mr:
		ibv_dereg_mr(ctx->mr);

	clean_dm:
		if (ctx->dm)
			ibv_free_dm(ctx->dm);

	clean_pd:
		ibv_dealloc_pd(ctx->pd);

	clean_comp_channel:
		if (ctx->channel)
			ibv_destroy_comp_channel(ctx->channel);

	// clean_device:
		ibv_close_device(ctx->context);

	clean_buffer:
		free(ctx->buf);

	clean_ctx:
		free(ctx);

	return NULL;
	next_step:

	// init ctx -------------

	if (!ctx){
		printf("Not ctx create\n");
		return NULL;
	}

	// post recv ------------
	// routs = pp_post_recv(ctx, ctx->rx_depth);
	
	int i;
	for(i = 0; i < ctx->rx_depth; ++i){

		struct ibv_sge list;
		memset(&list, 0, sizeof(ibv_sge));
		list.addr = (uintptr_t) ctx->buf + ctx->size*(i + ctx->rx_depth); // recv use the other part of memory
		list.length = ctx->size;
		list.lkey = ctx->mr->lkey;

		struct ibv_recv_wr wr;
		memset(&wr, 0, sizeof(ibv_recv_wr));
		wr.wr_id = i;
		wr.sg_list = &list;
		wr.num_sge = 1;

		struct ibv_recv_wr *bad_wr;

		if (ibv_post_recv(ctx->qp, &wr, &bad_wr))
			break;
	}

	if (i < ctx->rx_depth) {
		fprintf(stderr, "Couldn't post receive (%d)\n", i);
		return NULL;
	}

	// get port info ------------

	if (ibv_query_port(ctx->context, 1, &ctx->portinfo)) {
		fprintf(stderr, "Couldn't get port info\n");
		return NULL;
	}

	// NIC clock related
	mlx5dv_get_clock_info(ctx->context, &ctx->clock_info);
    ctx->vex.comp_mask =  IBV_VALUES_MASK_RAW_CLOCK;

    return ctx;
}

int finalize_rdma(struct rdma_context* ctx){

	if (ibv_destroy_qp(ctx->qp)) {
		fprintf(stderr, "Couldn't destroy QP\n");
		return 1;
	}

	if (ibv_destroy_cq(ctx->cq_s.cq)) {
		fprintf(stderr, "Couldn't destroy CQ\n");
		return 1;
	}

	if (ibv_dereg_mr(ctx->mr)) {
		fprintf(stderr, "Couldn't deregister MR\n");
		return 1;
	}

	if (ctx->dm) {
		if (ibv_free_dm(ctx->dm)) {
			fprintf(stderr, "Couldn't free DM\n");
			return 1;
		}
	}

	if (ibv_dealloc_pd(ctx->pd)) {
		fprintf(stderr, "Couldn't deallocate PD\n");
		return 1;
	}

	if (ctx->channel) {
		if (ibv_destroy_comp_channel(ctx->channel)) {
			fprintf(stderr, "Couldn't destroy completion channel\n");
			return 1;
		}
	}

	if (ibv_close_device(ctx->context)) {
		fprintf(stderr, "Couldn't release context\n");
		return 1;
	}

	free(ctx->buf);
	free(ctx);
    return 0;
}

int rdma_bind(struct rdma_context* ctx, int port){
	// server side bind
	struct pingpong_dest    *rem_dest;
	struct pingpong_dest     my_dest;
	char			 gid[33];


	my_dest.lid = ctx->portinfo.lid;
	if (ctx->portinfo.link_layer != IBV_LINK_LAYER_ETHERNET &&
							!my_dest.lid) {
		fprintf(stderr, "Couldn't get local LID\n");
		return 1;
	}

	assert(ctx->context);
	if (ibv_query_gid(ctx->context, 1, 0, &my_dest.gid)) {
		fprintf(stderr, "can't read sgid of index %d\n", 0);
		return 1;
	}

	my_dest.qpn = ctx->qp->qp_num;
	my_dest.psn = 0;
	inet_ntop(AF_INET6, &my_dest.gid, gid, sizeof gid);
	// printf("  local address:  LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
	// my_dest.lid, my_dest.qpn, my_dest.psn, gid);

	rem_dest = pp_server_exch_dest(ctx, port, &my_dest);

	if(!rem_dest){
		printf("Exchange msg failed\n");
		return 1;
	}
	inet_ntop(AF_INET6, &rem_dest->gid, gid, sizeof gid);
	// printf("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
	    //    rem_dest->lid, rem_dest->qpn, rem_dest->psn, gid);

    return 0;
}

int rdma_connect(struct rdma_context* ctx, const char *ipaddr, int port){
	// client side connect
	struct pingpong_dest    *rem_dest;
	struct pingpong_dest     my_dest;
	char			 gid[33];

	my_dest.lid = ctx->portinfo.lid;
	if (ctx->portinfo.link_layer != IBV_LINK_LAYER_ETHERNET &&
							!my_dest.lid) {
		fprintf(stderr, "Couldn't get local LID\n");
		return 1;
	}

	assert(ctx->context);
	if (ibv_query_gid(ctx->context, 1, 0, &my_dest.gid)) {
		fprintf(stderr, "can't read sgid of index %d\n", 0);
		return 1;
	}

	my_dest.qpn = ctx->qp->qp_num;
	my_dest.psn = 0;
	inet_ntop(AF_INET6, &my_dest.gid, gid, sizeof gid);
	// printf("  local address:  LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
	// my_dest.lid, my_dest.qpn, my_dest.psn, gid);

	rem_dest = pp_client_exch_dest(ipaddr, port, &my_dest);

	if(!rem_dest){
		printf("Exchange msg failed\n");
		return 1;
	}
	inet_ntop(AF_INET6, &rem_dest->gid, gid, sizeof gid);
	// printf("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
	    //    rem_dest->lid, rem_dest->qpn, rem_dest->psn, gid);

	// server finish the connect
	if (pp_connect_ctx(ctx, 1, 0, IBV_MTU_1024, 0, rem_dest, 0)){
		printf("Couln't build connect\n");
		return 1;
	}
    return 0;
}

int rdma_send(struct rdma_context* ctx, const char *buf, uint32_t len, long long int* timestamp){

	if(len>(uint32_t)ctx->size){
		printf("Error: msg size exceeds buffer size %d\n", ctx->size);
		return 1;
	}

	memcpy(ctx->buf, buf, len);

	struct ibv_sge list;
	memset(&list, 0, sizeof(ibv_sge));
	list.addr = (uintptr_t) ctx->buf;
	list.length = ctx->size;
	list.lkey = ctx->mr->lkey;

	ibv_wr_start(ctx->qpx);

	ctx->qpx->wr_id = 0;
	ctx->qpx->wr_flags = ctx->send_flags;

	ibv_wr_send(ctx->qpx);
	ibv_wr_set_sge(ctx->qpx, list.lkey, list.addr, list.length);

	if(ibv_wr_complete(ctx->qpx)){
		return 1;
	}
	
	if(ctx->send_flags!=IBV_SEND_SIGNALED){
		printf("Error!!\n");
	}
	
	// poll the request
	int ret;

	struct ibv_poll_cq_attr attr = {};
	do {
		ret = ibv_start_poll(ctx->cq_s.cq_ex, &attr);
	} while (ret == ENOENT);
	if (ret) {
		fprintf(stderr, "poll CQ failed %d\n", ret);
		return ret;
	}

	if(timestamp){
		// *timestamp = ibv_wc_read_completion_ts(ctx->cq_s.cq_ex);
		// if(global_flag==0)
			*timestamp = ibv_wc_read_completion_wallclock_ns(ctx->cq_s.cq_ex);
		// else
			// *timestamp = get_nic_ts(ctx->context, &ctx->vex, &ctx->clock_info);
	}


	ibv_end_poll(ctx->cq_s.cq_ex);
	if (ret && ret != ENOENT) {
		fprintf(stderr, "poll CQ failed %d\n", ret);
		return ret;
	}

    return 0;
}

extern int global_flag;

int rdma_recv(struct rdma_context* ctx, char *buf, uint32_t len, long long int* timestamp){

	int ret;
	if(len>(uint32_t)ctx->size){
		printf("Error: msg size exceeds buffer size %d\n", ctx->size);
		return 1;
	}
	struct ibv_poll_cq_attr attr = {};
	do {
		
		ret = ibv_start_poll(ctx->cq_s.cq_ex, &attr);
	} while (ret == ENOENT);

	if (ret) {
		fprintf(stderr, "poll CQ failed %d\n", ret);
		return ret;
	}

	if(timestamp){
		// *timestamp = ibv_wc_read_completion_ts(ctx->cq_s.cq_ex);
		// if(global_flag==0)
			*timestamp = ibv_wc_read_completion_wallclock_ns(ctx->cq_s.cq_ex);
		// else
			// *timestamp = get_nic_ts(ctx->context, &ctx->vex, &ctx->clock_info);
	}


	ibv_end_poll(ctx->cq_s.cq_ex);
	if (ret && ret != ENOENT) {
		fprintf(stderr, "poll CQ failed %d\n", ret);
		return ret;
	}

	int id = ctx->cq_s.cq_ex->wr_id;


	memcpy(buf, (void*)((uintptr_t)ctx->buf + ctx->size*(id + ctx->rx_depth)), len);
	
	struct ibv_sge list;
	memset(&list, 0, sizeof(ibv_sge));
	list.addr = (uintptr_t)ctx->buf + ctx->size*(id + ctx->rx_depth);
	list.length = ctx->size;
	list.lkey = ctx->mr->lkey;

	struct ibv_recv_wr wr;
	memset(&wr, 0, sizeof(ibv_recv_wr));
	wr.wr_id = id;
	wr.sg_list = &list;
	wr.num_sge = 1;

	struct ibv_recv_wr *bad_wr;

	assert(!ibv_post_recv(ctx->qp, &wr, &bad_wr));
		
    return 0;
}

