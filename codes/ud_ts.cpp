#include"ud_ts.h"

int pp_get_port_info_ud(struct ibv_context *context, int port,
		     struct ibv_port_attr *attr)
{
	return ibv_query_port(context, port, attr);
}

void wire_gid_to_gid_ud(const char *wgid, union ibv_gid *gid)
{
	char tmp[9];
	uint32_t v32;
	int i;

	for (tmp[8] = 0, i = 0; i < 4; ++i) {
		memcpy(tmp, wgid + i * 8, 8);
		sscanf(tmp, "%x", &v32);
		*(uint32_t *)(&gid->raw[i * 4]) = ntohl(v32);
	}
}

void gid_to_wire_gid_ud(const union ibv_gid *gid, char wgid[])
{
	int i;

	for (i = 0; i < 4; ++i)
		sprintf(&wgid[i * 8], "%08x",
			htonl(*(uint32_t *)(gid->raw + i * 4)));
}


static int pp_connect_ctx_ud(struct tsocket_context_ud *ctx, int port, int my_psn, int sl, struct pingpong_dest_ud *dest, int sgid_idx, int qp_idx)
{

	struct ibv_ah_attr ah_attr;
	memset(&ah_attr, 0, sizeof(ibv_ah_attr));

	ah_attr.is_global	= 0;
	ah_attr.dlid		= dest->lid;
	ah_attr.sl		= sl;
	ah_attr.src_path_bits	= 0;
	ah_attr.port_num	= port;

	if (dest->gid.global.interface_id) {
		ah_attr.is_global = 1;
		ah_attr.grh.hop_limit = 10;
		ah_attr.grh.dgid = dest->gid;
		ah_attr.grh.sgid_index = sgid_idx;
	}

	ctx->ah_list[qp_idx] = ibv_create_ah(ctx->pd, &ah_attr);
	// printf("Create ah on %d\n", qp_idx);

	if (!ctx->ah_list[qp_idx]) {
		fprintf(stderr, "Failed to create AH\n");
		return 1;
	}
	printf("QP connection built with %d\n", dest->qpn);

	return 0;

}


static struct pingpong_dest_ud *pp_client_exch_dest(const char *servername, int port, const struct pingpong_dest_ud *my_dest)
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
	struct pingpong_dest_ud *rem_dest = NULL;
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

	gid_to_wire_gid_ud(&my_dest->gid, gid);
	sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn,
							my_dest->psn, gid);
	// printf("QPN is %d\n", my_dest->qpn);
	
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

	rem_dest = (struct pingpong_dest_ud*)malloc(sizeof *rem_dest);
	if (!rem_dest)
		goto out;

	sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn,
						&rem_dest->psn, gid);
	wire_gid_to_gid_ud(gid, &rem_dest->gid);

out:
	close(sockfd);
	return rem_dest;
}

static struct pingpong_dest_ud *pp_server_exch_dest(struct tsocket_context_ud *ctx, int ib_port, int qp_idx,						 int port, int sl, const struct pingpong_dest_ud *my_dest, int sgid_idx)
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
	struct pingpong_dest_ud *rem_dest = NULL;
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

	rem_dest = (struct pingpong_dest_ud*)malloc(sizeof *rem_dest);
	if (!rem_dest)
		goto out;

	sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn,
							&rem_dest->psn, gid);
	wire_gid_to_gid_ud(gid, &rem_dest->gid);

	// diff
	if (pp_connect_ctx_ud(ctx, 1, 0, 0, rem_dest, 0, qp_idx)) {
		fprintf(stderr, "Couldn't connect to remote QP\n");
		free(rem_dest);
		rem_dest = NULL;
		goto out;
	}


	gid_to_wire_gid_ud(&my_dest->gid, gid);
	sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn,
							my_dest->psn, gid);
	// printf("QPN is %d\n", my_dest->qpn);

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

struct tsocket_context_ud* create_tsocket_ud(){

	// parameters
	struct ibv_device      **dev_list;
	struct ibv_device	*ib_dev;
	int                      ib_port = 1;
	unsigned long long      size = 64;
	unsigned int             rx_depth = 5*ud_batch;
	int			 gidx = 0;
	char			 gid[33];

	struct tsocket_context_ud *ctx;

	// ctx = (struct tsocket_context_ud*)malloc(sizeof *ctx);
	// memset(ctx, 0, sizeof(struct tsocket_context_ud));
	ctx = new tsocket_context_ud;
	if (!ctx)
		return NULL;

	ctx->rem_dest = (struct pingpong_dest_ud**)malloc(sizeof(struct pingpong_dest_ud*)*(MAX_PAIR+1));  // we reside one for master

	ctx->opp_qpn = 1;  // we reside one for master, and slave start from 1
	ctx->has_master = 0;
	ctx->ah_list = (struct ibv_ah**)malloc(sizeof(struct ibv_ah*)*(MAX_PAIR+1));

	ctx->page_size = sysconf(_SC_PAGESIZE);

	ctx->size       = size;
	ctx->send_flags = IBV_SEND_SIGNALED;
	ctx->rx_depth   = rx_depth;
	ctx->ib_port = ib_port;
	ctx->gidx = gidx;

	sem_init(&ctx->mutex_lock, 0, 1);

	srand48(getpid() * time(NULL));

	dev_list = ibv_get_device_list(NULL);
	if (!dev_list) {
		perror("Failed to get IB devices list");
		return NULL;
	}

	ib_dev = *dev_list;
	if (!ib_dev) {
		fprintf(stderr, "No IB devices found\n");
		return NULL;
	}

	ctx->buf = memalign(ctx->page_size, size);
	if (!ctx->buf) {
		fprintf(stderr, "Couldn't allocate work buf.\n");
		goto clean_grh_buffer;
	}

	ctx->context = ibv_open_device(ib_dev);
	if (!ctx->context) {
		fprintf(stderr, "Couldn't get context for %s\n",
			ibv_get_device_name(ib_dev));
		goto clean_buffer;
	}

	{
		struct ibv_port_attr port_info = { };
		int mtu;

		if (ibv_query_port(ctx->context, ib_port, &port_info)) {
			fprintf(stderr, "Unable to query port info for port %d\n", ib_port);
			goto clean_device;
		}
		mtu = 1 << (port_info.active_mtu + 7);

		// printf("Use mtu size %d\n", mtu);
		ctx->mtu = mtu;
	}

	ctx->channel = NULL;

	ctx->pd = ibv_alloc_pd(ctx->context);
	if (!ctx->pd) {
		fprintf(stderr, "Couldn't allocate PD\n");
		goto clean_comp_channel;
	}

	ctx->mr = ibv_reg_mr(ctx->pd, ctx->buf, size, IBV_ACCESS_LOCAL_WRITE);
	if (!ctx->mr) {
		fprintf(stderr, "Couldn't register MR\n");
		goto clean_mr_grh;
	}

	struct ibv_cq_init_attr_ex attr_ex;
	memset(&attr_ex, 0, sizeof(struct ibv_cq_init_attr_ex));
	attr_ex.cqe = rx_depth + 1;
	attr_ex.cq_context = NULL;
	attr_ex.channel = NULL;
	attr_ex.comp_vector = 0;
	attr_ex.wc_flags = IBV_WC_EX_WITH_COMPLETION_TIMESTAMP | IBV_WC_EX_WITH_COMPLETION_TIMESTAMP_WALLCLOCK  |
	IBV_WC_EX_WITH_SRC_QP  |
	IBV_WC_EX_WITH_IMM;
	ctx->cq_ex_send = ibv_create_cq_ex(ctx->context, &attr_ex);
	ctx->cq_ex_recv = ibv_create_cq_ex(ctx->context, &attr_ex);
	ctx->cq_send = ibv_cq_ex_to_cq(ctx->cq_ex_send);
	ctx->cq_recv = ibv_cq_ex_to_cq(ctx->cq_ex_recv);

	if (!ctx->cq_send||!ctx->cq_ex_recv) {
		fprintf(stderr, "Couldn't create CQ\n");
		goto clean_mr;
	}

	{

		struct ibv_qp_init_attr_ex init_attr_ex = {};
		memset(&init_attr_ex, 0, sizeof(struct ibv_qp_init_attr_ex));

		init_attr_ex.send_cq = ctx->cq_send;
		init_attr_ex.recv_cq = ctx->cq_recv;
		init_attr_ex.cap.max_send_wr = rx_depth;
		init_attr_ex.cap.max_recv_wr = rx_depth;
		init_attr_ex.cap.max_send_sge = 1;
		init_attr_ex.cap.max_recv_sge = 2;
		init_attr_ex.qp_type = IBV_QPT_UD;

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
		memset(&attr, 0, sizeof(struct ibv_qp_attr));

		attr.qp_state        = IBV_QPS_INIT;
		attr.pkey_index      = 0;
		attr.port_num        = ib_port;
		attr.qkey            = 0x11111111;

		if (ibv_modify_qp(ctx->qp, &attr,
				  IBV_QP_STATE              |
				  IBV_QP_PKEY_INDEX         |
				  IBV_QP_PORT               |
				  IBV_QP_QKEY)) {
			fprintf(stderr, "Failed to modify QP to INIT\n");
			goto clean_qp;
		}
	}

	// fprintf(stdout, "Create ctx success\n");

	// post multiple receiver
	for(int i=0; i<ctx->rx_depth; i++){
		struct ibv_sge list;
		list.addr	= (uintptr_t) ctx->buf;
		list.length = ctx->size;
		list.lkey	= ctx->mr->lkey;
			
		struct ibv_recv_wr wr;
		memset(&wr, 0, sizeof(struct ibv_recv_wr));

		wr.wr_id	    = 0;
		wr.sg_list    = &list;
		wr.num_sge    = 1;
		struct ibv_recv_wr *bad_wr;

		int ret;
		if ((ret = ibv_post_recv(ctx->qp, &wr, &bad_wr)))
			assert(false);
	}

	// get port information
	if (pp_get_port_info_ud(ctx->context, ib_port, &ctx->portinfo)) {
		fprintf(stderr, "Couldn't get port info\n");
		assert(false);
	}


	ctx->my_dest.lid = ctx->portinfo.lid;
	ctx->my_dest.qpn = ctx->qp->qp_num;
	ctx->my_dest.psn = lrand48() & 0xffffff;


	if (ibv_query_gid(ctx->context, ib_port, gidx, &ctx->my_dest.gid)) {
		fprintf(stderr, "Could not get local gid for gid index "
							"%d\n", gidx);
		assert(false);
	}


	// printf("GID %016lx:%016lx\n", my_dest.gid.global.subnet_prefix, my_dest.gid.global.interface_id);
	inet_ntop(AF_INET6, &ctx->my_dest.gid, gid, sizeof gid);
	// printf("  local address:  LID 0x%04x, QPN 0x%06x, PSN 0x%06x: GID %s\n",
	    //    my_dest.lid, my_dest.qpn, my_dest.psn, gid);

	ibv_free_device_list(dev_list);

	// modify QP to RTR and RTS
	struct ibv_qp_attr attr;
	memset(&attr, 0, sizeof(attr));
	attr.qp_state		= IBV_QPS_RTR;

	if (ibv_modify_qp(ctx->qp, &attr, IBV_QP_STATE)) {
		fprintf(stderr, "Failed to modify QP to RTR\n");
		return NULL;
	}

	attr.qp_state	    = IBV_QPS_RTS;
	attr.sq_psn	    = 0;

	if (ibv_modify_qp(ctx->qp, &attr,
			  IBV_QP_STATE              |
			  IBV_QP_SQ_PSN)) {
		fprintf(stderr, "Failed to modify QP to RTS\n");
		return NULL;
	}

	return ctx;

clean_qp:
	ibv_destroy_qp(ctx->qp);

clean_cq:
	ibv_destroy_cq(ctx->cq_send);
	ibv_destroy_cq(ctx->cq_recv);

clean_mr:
	ibv_dereg_mr(ctx->mr);

clean_mr_grh:
	ibv_dealloc_pd(ctx->pd);

clean_comp_channel:
	if (ctx->channel)
		ibv_destroy_comp_channel(ctx->channel);

clean_device:
	ibv_close_device(ctx->context);

clean_buffer:
	free(ctx->buf);

clean_grh_buffer:
	free(ctx->rem_dest);
	free(ctx->ah_list);
	delete ctx;

	return NULL;

}

int finalize_tsocket_ud(struct tsocket_context_ud* ctx){

	if (ibv_destroy_qp(ctx->qp)) {
		fprintf(stderr, "Couldn't destroy QP\n");
		return 1;
	}

	if (ibv_destroy_cq(ctx->cq_send)||ibv_destroy_cq(ctx->cq_recv)) {
		fprintf(stderr, "Couldn't destroy CQ\n");
		return 1;
	}

	if (ibv_dereg_mr(ctx->mr)) {
		fprintf(stderr, "Couldn't deregister MR\n");
		return 1;
	}

	for(int i=(1^ctx->has_master); i<ctx->opp_qpn; i++){
		if (ibv_destroy_ah(ctx->ah_list[i])) {
			fprintf(stderr, "Couldn't destroy AH\n");
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

	for(int i=(1^ctx->has_master); i<ctx->opp_qpn; i++)
		free(ctx->rem_dest[i]);
	free(ctx->buf);
	free(ctx->rem_dest);
	free(ctx->ah_list);
	delete ctx;

	return 0;

}

int tsocket_bind_ud(struct tsocket_context_ud* ctx, int port){

	sem_wait(&ctx->mutex_lock);
	int qpn = ctx->opp_qpn;

	if(qpn>=MAX_PAIR){
		fprintf(stderr, "Error: Eceeding Max PAIR num\n");
		return -1;
	}
	
	ctx->rem_dest[qpn] = pp_server_exch_dest(ctx, ctx->ib_port, qpn, port, 0, &ctx->my_dest, ctx->gidx);
	
	ctx->opp_qpn++;
	sem_post(&ctx->mutex_lock);

	// add the mapping from opp qpn to pq_idx
	int dest_qp = ctx->rem_dest[qpn]->qpn;
	// printf("Add new qp %d in %d\n", dest_qp, qpn);
	ctx->mapping_table[dest_qp] = qpn;

	return qpn;

}

int tsocket_connect_ud(struct tsocket_context_ud* ctx, const char *ipaddr, int port){

	// we fix qp idx 0 for master. in our model, at most one master exists
	int qpn = 0;
	ctx->has_master = 1;
	
	ctx->rem_dest[qpn] = pp_client_exch_dest(ipaddr, port, &ctx->my_dest);

	if (pp_connect_ctx_ud(ctx, ctx->ib_port, ctx->my_dest.psn, 0, ctx->rem_dest[qpn], ctx->gidx, qpn))
		return -1;

	// we still add mapping
	int dest_qp = ctx->rem_dest[qpn]->qpn;
	// printf("Add new qp %d in %d\n", dest_qp, qpn);
	ctx->mapping_table[dest_qp] = qpn;

	return qpn;

}

int tsocket_send_ud(struct tsocket_context_ud* ctx, int qp_idx, int request_id, const char *buf, uint32_t len){

	assert(len<ctx->size-GRH_SIZE);
	if(qp_idx>=ctx->opp_qpn){
		fprintf(stderr, "QP idx exceeds max num %d\n", ctx->opp_qpn);
	}

	struct ibv_sge list;
	memset(&list, 0, sizeof(struct ibv_sge));
	list.addr = (uintptr_t) ctx->buf;
	list.length = len;
	list.lkey = ctx->mr->lkey;

	ibv_wr_start(ctx->qpx);

	ctx->qpx->wr_id = qp_idx;
	ctx->qpx->wr_flags = IBV_SEND_SIGNALED;

	ibv_wr_send_imm(ctx->qpx, request_id);
	ibv_wr_set_sge(ctx->qpx, list.lkey, list.addr, list.length);
	ibv_wr_set_ud_addr(ctx->qpx, ctx->ah_list[qp_idx], ctx->rem_dest[qp_idx]->qpn, 0x11111111);

	if(ibv_wr_complete(ctx->qpx)){
		printf("Error in complete!\n");
		assert(false);
	}
	
	return qp_idx;
}

int tsocket_poll_send(struct tsocket_context_ud* ctx, ts_t* ts_array, int* qp_idx_array){

	int num_cq=0;
	int ret;
	struct ibv_poll_cq_attr attr = {};
	do {
		ret = ibv_start_poll(ctx->cq_ex_send, &attr);
	} while (ret == ENOENT);  // the first one should always poll an element

	if (ret) {
		fprintf(stderr, "poll CQ failed %d\n", ret);
		assert(false);
	}

	ts_array[num_cq] = ibv_wc_read_completion_wallclock_ns(ctx->cq_ex_send);
	qp_idx_array[num_cq] = ctx->cq_ex_send->wr_id;

	num_cq++;

	while(num_cq<lcu_batch){	
		ret = ibv_next_poll(ctx->cq_ex_send);
		if(ret && ret!=ENOENT){
			printf("Error in next poll!");
			assert(false);
		}
		if(ret == ENOENT) break;
		ts_array[num_cq] = ibv_wc_read_completion_wallclock_ns(ctx->cq_ex_send);
		qp_idx_array[num_cq] = ctx->cq_ex_send->wr_id;
		num_cq++;
	}

	ibv_end_poll(ctx->cq_ex_send);

	return num_cq;

}

void tsocket_recv_ud(struct tsocket_context_ud* ctx, int count){

	// post new receiver
	struct ibv_sge list;
	list.addr	= (uintptr_t) ctx->buf;
	list.length = ctx->size;
	list.lkey	= ctx->mr->lkey;
		
	struct ibv_recv_wr wr;
	memset(&wr, 0, sizeof(struct ibv_recv_wr));

	wr.wr_id	    = 0;
	wr.sg_list    = &list;
	wr.num_sge    = 1;
	struct ibv_recv_wr *bad_wr;

	for(int i=0;i<count;i++){
		int rc = ibv_post_recv(ctx->qp, &wr, &bad_wr);
		if(rc){
			printf("post UD recv %d failed. Error code is %d.\n", i, rc);
			assert(false);
		}
	}

}

int tsocket_poll_recv(struct tsocket_context_ud* ctx, ts_t* ts_array, int* qp_idx_array, int* request_id_array, int factor){

	int num_cq=0;
	int ret;
	struct ibv_poll_cq_attr attr = {};
	// do {
	// 	ret = ibv_start_poll(ctx->cq_ex_recv, &attr);
	// } while (ret == ENOENT);  // the first one should always poll an element
	ret = ibv_start_poll(ctx->cq_ex_recv, &attr);
	if(ret==ENOENT){  // no probe arrives
		ibv_end_poll(ctx->cq_ex_recv);
		return 0;
	}
	
	if (ret) {
		fprintf(stderr, "poll CQ failed %d\n", ret);
		assert(false);
	}

	ts_array[num_cq] = ibv_wc_read_completion_wallclock_ns(ctx->cq_ex_recv);
	request_id_array[num_cq] = ibv_wc_read_imm_data(ctx->cq_ex_recv);
	int qpn = ibv_wc_read_src_qp(ctx->cq_ex_recv);
	std::unordered_map<int, int>::const_iterator got = ctx->mapping_table.find(qpn);
	if(got == ctx->mapping_table.end()){
		fprintf(stderr, "Error: qpn %d not found!\n", qpn);
		assert(false);
	}
	qp_idx_array[num_cq] = got->second;
	num_cq++;

	while(num_cq<lcu_batch*factor){	
		ret = ibv_next_poll(ctx->cq_ex_recv);
		if(ret && ret!=ENOENT){
			printf("Error in next poll!");
			assert(false);
		}
		if(ret == ENOENT) break;
		ts_array[num_cq] = ibv_wc_read_completion_wallclock_ns(ctx->cq_ex_recv);
		request_id_array[num_cq] = ibv_wc_read_imm_data(ctx->cq_ex_recv);
		int qpn = ibv_wc_read_src_qp(ctx->cq_ex_recv);
		std::unordered_map<int, int>::const_iterator got = ctx->mapping_table.find(qpn);
		if(got == ctx->mapping_table.end()){
			fprintf(stderr, "Error: qpn %d not found!\n", qpn);
			assert(false);
		}
		qp_idx_array[num_cq] = got->second;
		// printf("The qpn %d is %d th.\n", qpn, qp_idx_array[num_cq]);

		num_cq++;
	}

	ibv_end_poll(ctx->cq_ex_recv);

	return num_cq;

}