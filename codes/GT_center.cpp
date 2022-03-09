#include"clock_sync.h"
#include<stdio.h>
#include<string.h>
#include<fstream>
#include<assert.h>
#include <csignal>
#include <unistd.h>
#include "config.h"

void signalhandler( int signum );

int main(){

    uint32_t count;
    char** addr_list;
    int* port_list;
    char buff[64];
    // int port;

    // signal( SIGSEGV, signalhandler );

    std::fstream ifs("ifconfig", std::ios::in);
    if(!ifs){
        printf("Open config file failed\n");
        return 0;
    }
    
    ifs>>count;

    addr_list = (char**) malloc(count*sizeof(char*));
    port_list = (int*) malloc(count*sizeof(int));


    for(uint32_t idx=0; idx<count; idx++){
        addr_list[idx] = new char[64];
        ifs>>buff;
        strcpy(addr_list[idx], buff);
        port_list[idx] = hp_connect_port;
    }

    ifs.close();

    GTCenter gtCenter(addr_list, port_list, count);

    gtCenter.build_connection_config(CENTER_LAYOUT);

    printf("Send connection config finish\n");

    // collect warmup finish info
    msg2cent msg_array[count];
    gtCenter.collect_info(msg_array);

    for(uint32_t i=0; i<count; i++){
        assert(msg_array[i].is_warmup_done);
        assert(msg_array[i].msg_id==111);
    }
        
    printf("collect msg finish.\n");

    // send start msg to all nodes
    msg2master msg_array2[count];
    for(uint32_t i=0; i<count; i++)
        msg_array2[i].msg_id = 222;

    gtCenter.broadcast_info(msg_array2);

    // build TCP connection with other nodes

    printf("Warmup finish, gonna start test.\n");

    // test clock accuracy
    #ifdef TEST_MODE
    gtCenter.ts_file.open("data/ts_precision.txt", std::ios::out);
    if(!gtCenter.ts_file){
        printf("Open file failed!\n");
        assert(false);
    }
    long long int soft_ts_result=0, hard_ts_result=0;
    printf("total test duration is %f seconds\n", 1e-3*TEST_TIMES*TEST_INTERVAL);
    sleep(3);
    for(int i=0; i<TEST_TIMES; i++){
        gtCenter.test_hw_ts(hard_ts_result, soft_ts_result);
        usleep(TEST_INTERVAL*1000);
    }
    ts_t hw_pres_avg = hard_ts_result/(count-1)/TEST_TIMES;
    ts_t sw_pres_avg = soft_ts_result/(count-1)/TEST_TIMES;
//    printf("Hardware AVG precision: %lld ns\nSoftware AVG precision: %lld ns\n", hw_pres_avg, sw_pres_avg);
    gtCenter.ts_file.close();
    #endif


    printf("Test finish, gonna release resources\n");

    free(port_list);

    for(uint32_t idx=0; idx<count; idx++){
        delete addr_list[idx];
    }

    free(addr_list);

    gtCenter.finalize();
    printf("Releasing resources success\n");

    return 0;
}

void signalhandler( int signum ){
    printf("programm exit wrong!!!\n");
    exit(0);
}