#ifndef _GLOBAL_H
#define _GLOBAL_H

#include<time.h>
#include<unistd.h>
#include<infiniband/verbs.h>
#include<infiniband/mlx5dv.h>
#include<algorithm>
#include<stdio.h>
#include<iostream>
#include<string.h>
#include<pthread.h>
#include<fstream>

//------ tunable ------//
#define buffersize 1024
#define port_base 30000
#define ts_port_delta 100
#define MAX_STATIS_NUM 1000

// probe filters
#define upper_filter 0.80  // (0, 1-lower_filter)
#define lower_filter 0  // (0, upper_filter)

// net-work layouts
#define CENTER_LAYOUT 0
#define LINE_LAYOUT 1

// constant
#define BILLION 1000000000LL
#define MILLION 1000000

// Global clock
#define GTCenter_base_port 18999
#define MAX_SERVERS 100

// Slave clock record history offset dots
#define RECORD_MAX 100

// ts accuracy test
#define MAX_TEST_TIMES 1000

//------ control ------//
// #define OUTPUT_MODE
#define STATISTIC_ENABLE false

//------ type and structure ------//
typedef long long int ts_t;

//------ test related ------//
 #define TEST_MODE
#define TEST_TIMES 50
#define TEST_INTERVAL 100 // milisecond

// bw load parameter
#define bw_test_size 1024
#define bw_test_time 20 // seconds
#define bw_concurrency 2

// test of msg size to sync accuracy
#define DATA_SIZE 4096  // in bytes
#define data_test_epoch 15  // from which time the test begin

// cpu&nic synchronization(CNS) parameter
#define CNS_interval 2 // in second
#define recalculation_epoch 50
#define CNS_gap (CNS_interval*1000000/recalculation_epoch)  // us
#define CNS_history_len 100
#define CNS_warmup_epoch 5

// low-cpu-util related parameters
#define lcu_batch 200  // number of batch
#define MAX_PAIR	100  // the maximum opposite nodes
#define UD_port_delta 2727  // the connection port offset between RC and UD
#define DL_len (10*dot_num)  // the length of buffer in DataList
#define ud_batch 2000  // it must bigger than lcu batch
#define slave_batch_time 10000  // in us
#define lcu_buffer_size (sizeof(batch_data))
#define large_recv_batch 10

// timeslot parameters
#define fit_interval 2 // seconds
#define interval 1000 // this is probe interval, in us
#define dot_num (int)(fit_interval*1000000/interval)  // number of sampling in each epoch
#define warmup_epoch 5
#define fitting_method FM_SVM

// SVM fitting parameters
#define SVM_delay 1  // SVM delay slot(plus 0.5)

// constants
// ts state
#define PROBE_INV 0  // invalid
#define PROBE_UPR 1  // remote upper probe received
#define PROBE_LWR 1<<1  // remote lower probe received
#define PROBE_LR  1<<2  // local probe ready
#define PROBE_OK (PROBE_UPR|PROBE_LWR|PROBE_LR)
// fitting methods
#define FM_SVM 0
#define FM_LSVM 1

// this structure define the data in large buffer
typedef struct{

	int data_num;
	int request_id[ud_batch];
	ts_t timestamp[ud_batch];
	bool is_rc_data[ud_batch]; // we mix two type of ts together, and use this to mark the ts type
	
}batch_data;


typedef struct my{

	ts_t duration1[MAX_STATIS_NUM];
	ts_t duration2[MAX_STATIS_NUM];
	int times;
	
	// construct function
	my(){
		// for(int i=0; i<MAX_STATIS_NUM; i++){
		// 	duration1[i] = 0;
		// 	duration2[i] = 0;
		// }
		times = 0;
	}

}statistic;

// global variable
extern statistic clock_stat;
extern ts_t test_ts;
extern int rdma_ts_test_flag;
extern int global_flag; // global flag for any usage

// get real clock in ns
inline ts_t get_real_clock(){

	struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_nsec + 1000000000*ts.tv_sec;

}

inline ts_t get_nic_ts(ibv_context* context, ibv_values_ex* vex, mlx5dv_clock_info* clock_info){

    ibv_query_rt_values_ex(context, vex);
 
    return mlx5dv_ts_to_ns(clock_info, vex->raw_clock.tv_nsec);

}

template <class T>
T get_percent_val(T* array, double percent, int len){
    
    if(percent<0 || percent>1){
        fprintf(stderr, "Wrong input percent!\n");
        return array[0];
    }
	if(percent==1)
		return array[len-1];

    return array[(int)(percent*len)];

}

template <class T>
void print_all_val(T* array, double start, double end, int len){

    for(int i=(int)(start*len); i<(int)(end*len); i++)
		std::cout<<array[i]<<std::endl;

}

#endif