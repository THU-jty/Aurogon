#ifndef _PAIR_SYNC
#define _PAIR_SYNC

#include<infiniband/mlx5dv.h>
#include<string.h>
#include<assert.h>
#include<stdio.h>
#include<fstream>
#include<unistd.h>
#include<iostream>
#include<time.h>
#include"semaphore.h"
#include<pthread.h>
#include<vector>
#include<algorithm>
#include<iomanip>
#include<sched.h>
#include"svm.h"
#include"global.h"
#include"rdma_ts.h"
#include"rc_ts.h"
#include"ud_ts.h"
#include<map>
#include<atomic>

#define ts_abs(x) (x>=0?x:-x)


// pair-wise timestamp exchanging msg structure
typedef struct{
	char flag; // 0 start msg, 1 infor msg, 2 final msg
	ts_t current_ts;
	ts_t pigbag_ts;
	// offset related
	int new_value;
	ts_t x_point;
	ts_t y_point;
	int has_master; // denote if the slave side will modify the ts
	ts_t start_ts;
}pw_msg;

// pair-wise timestamp local storage structure
typedef struct{
	ts_t time;
	ts_t lower;
	ts_t upper;
	// the following is related to offset elimination
	ts_t opp_upper_x;
	ts_t opp_lower_x;
}ts_dot;

typedef struct{
	ts_t x;
	ts_t y;
	bool valid=false;
}ts_cord;

// global clock structure, currently we use Tree structure, which only contains two connection
typedef struct{
	int master_port;
	char master_ip[64];
	int slave_port[MAX_SERVERS];
	int slave_port_num;
}cent2master;

// bind&connect thread structure
typedef struct{
	tsocket_context* ctx;
	int port;
}bind_t;

typedef struct{
	tsocket_context* ctx;
	char ip_addr[64];
	int port;
}connect_t;

// center msg collection, can be modify to collect any info you wang
typedef struct{
	int msg_id;
	ts_t ts;
	ts_t soft_ts;
	ts_t send_ts;  // record the send ts
	ts_t recv_ts;
	bool is_warmup_done;
}msg2cent;

typedef struct{
	int msg_id;
	int port;
	ts_t send_ts;  // record the send ts
}msg2master;

typedef struct{
	int execution_period;
	int idx;	
}cns_sort_t;

// DataList structure is used to maintain the probe data, and provide data management functions. Note that this structure is not thread-safety with regard to write. So we have only one writer, and use memory barrier to guarantee read integrity
class GlobalTimer;
class DataList{

public:
	// data list
	ts_t* timestamp;  // the x axis timestamp
	ts_t* upper_dot;  // the upper level dot
	ts_t* lower_dot;  // the lower level dot
	char* state;  // stand for the state

	// pointers
	ts_t start_ts;
	int idx_start;
	int idx_end;

	// if fitting is in process
	int fitting_flag;

	// functions
	void init(int node_id);
	void finalize();
	// Insert new probe data
	void InsertLocalProbe(int request_id, ts_t ts, ts_t upper, ts_t lower);
	void InsertRemoteUpper(int request_id, ts_t upper);
	void InsertRemoteLower(int request_id, ts_t lower); 

	// GC related function
	void DropProbeData(int id_start, int id_end);

	// Data output related
	int node_id;
	std::fstream ofs_upper;
	std::fstream ofs_lower;
	std::fstream ofs_info;
	std::fstream cns_dot;
	std::fstream cns_info;

	// a pointer to the GlobalTimer
	GlobalTimer* gtimer;

	// fitting thread
	pthread_t fitting_thread;

	// record the svm time
	ts_t svm_duration[60];
	int svm_id;

	// tick function, start fitting after particular events. It should be called when the newest point is updated
	void tick(int idx);

};

// clock manager is responsible for high-precision timestamp management. It's responsible for insert timestamp and retrieve timestamp
class ClockManager{

public:
	// NIC sync parameters. Note that batch clock sync method has a delay, so we should deploy two arrays to address the problem
	double mid_x[RECORD_MAX];
	double mid_y[RECORD_MAX];
	bool mid_valid[RECORD_MAX];
	double edge_x[RECORD_MAX];
	double edge_y[RECORD_MAX];
	bool edge_valid[RECORD_MAX];

	// the following two variables behave as base to enable doule format of long long int.
	ts_t start_x;
	ts_t start_y;
	int type;

	// device information
	ibv_context* ctx;
    mlx5dv_clock_info clock_info;
	ibv_values_ex vex;

	GlobalTimer* gtimer;
	
	std::fstream ofs_edge;
	// std::fstream cns_edge;

	// functions
	void init(GlobalTimer* timer_pointer, int type);  // type 0 for CPU, type 1 for NIC
	void finalize();

	// NIC synchronizatio functions
	void InsertResDot(ts_t x, ts_t y);
	// the following two functions get modified TS from NIC timestamp
	ts_t GetModifiedValue(ts_t timestamp);  // return the array value
	ts_t GetRealTime(ts_t timestamp);	// return the offset

	// CPU-NIC synchronization

	// other functions
	ts_t get_nic_time();
	ts_t get_cpu_time();

	int _interval;  // in us
	int delete_idx;

};

// GlobalTimers provider synchronized clock to each other
class GlobalTimer{

public:

    // node-sync(NS) related functions

    // init&finalize
    void init(int local_port, int node_id);
	void finalize();

	void run();  // this function run the "run" function of every Master/Slave in a asynchronous way

    // get accurate cpu/nic timestamp
	ts_t get_timestamp();
	void get_ts_map(ts_t* x, ts_t* y);
	void get_nic_ts_map(ts_t* x, ts_t* y);
	ts_t get_timestamp_nic();
    // get modified offset from nic timestamp
    ts_t get_modify_ts(ts_t input);
	ts_t get_nic_time();  // nic unmodified timestamp

	// function to communicate with GT center
    void send_to_center(msg2cent* msg);
	void recv_from_center(msg2master* msg);

    // test timestamp accuracy(not only hardware timestamp)
	void test_hw_ts();

    // control functions
    // set the whole function done
	void set_done();
    // wait for global warmup done
	void wait_warmup_done();

    // flag functions
    bool is_local_warmup_done(); // local warm up done
	bool is_warmup_done();  // all nodes warm up done
	bool is_done();

    // node-sync(NS) related variables
	tsocket_context* rcc_master; // RC connection master
    tsocket_context** rcc_list;  // RC connection slave list
	rdma_context* rcc_center;  // RC connection with center
    tsocket_context_ud* udc;  // UD connection
	DataList* datalist;
	ClockManager* clockmanager;  // this is used for NIC sync
	ClockManager* clockmanager_cpu;

	// cpu-nic-sync(CNS) related variables
	bool CNS_warmup_done;
	bool local_warmup_done;
	bool global_warmup_done;

	// NS related parameters
	int port;
	int node_id;
	int SlaveCount;

	// multi-thread related variables
	pthread_t master_rc, master_ud;
	// regular probing thread
	pthread_t master_thread, slave_thread, extract_thread;
	pthread_t svm_thread;  // SVM thread
	pthread_t cpu_thread; // this thread sync time between NIC and CPU periodically
	pthread_t test_thread; // this thread asynchronously test timestamp with center node

	bool _is_done;

};

// a node run a GTCenter to construct inter-connection structure and test accuracy
class GTCenter{

public:

	GTCenter(char** addr_list, int* port_list, uint32_t addr_num); // load all GlobalTimer ip address
	// ~GTCenter();
	void finalize();

	void build_connection_config(int layout);

	// test function
	void print_layout();

	uint32_t get_server_num() {return server_num;}

	// some function to communicate with servers
	void collect_info(msg2cent* msg_array); // its user's responsibility to allocate msg_array space

	void broadcast_info(msg2master* msg_array);

	// center TS accuracy test
	void test_hw_ts(long long int& hard_ts_sum, long long int& soft_ts_sum);

	// statistic
	void init_statistic();

	void print_statistic();


// private:
	// maintain RDMA connection with GlobalTimer
	// rdma::socket** sock_array;
	uint32_t server_num;
	char** ip_list;
	rdma_context** context_array;
	int stat_pointer;

	int connection_slave[MAX_SERVERS][MAX_SERVERS];  // idx as slave
	int connection_master[MAX_SERVERS][MAX_SERVERS];  // idx as master

	void send_layout_to_master();

	// statistics
	int stat_ts_array[MAX_SERVERS-1][MAX_TEST_TIMES];

	std::fstream ts_file;

};



#endif