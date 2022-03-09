#include"clock_sync.h"
#include<stdio.h>
#include<pthread.h>
#include"sock.h"
// #include"nn.hpp"

extern int _gt_node_id;

int main(int argc, char** argv){

    // bind the core
    // cpu_set_t mask;
    // CPU_ZERO(&mask);
    // CPU_SET(15, &mask);
    // pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &mask);

    GlobalTimer gt;
    assert(argc>=2);
    // _gt_node_id = atoi(argv[1]);
    gt.init(hp_connect_port, atoi(argv[1]));

    while(!gt.is_warmup_done()) usleep(1000);

    msg2cent msg;
    msg.is_warmup_done = true;
    gt.send_to_center(&msg);

    printf("Global timer warmup done");
    fflush(stdout);

    // printf("Gonna test hw ts\n");

    // test convergent EC performance
    // int node_id = 0;
    // assert(argc==3);
    // node_id = atoi(argv[1]);

    // build TCP connection with the center
    // int sock;
    // sock = sock_daemon_connect(12345+321);
    // if(sock<0){
    //     printf("Bind failed!!\n");
    //     return 0;
    // }
    // int rc;
    // char buff[1024*1024];

    // nano-msg
    // nn_init();
    // int sock;
    // sock = nn_socket(AF_SP, NN_PAIR);
    // char ip_addr[128];
    // sprintf(ip_addr, "tcp://%s:%d", argv[2], 12345);
    // int rc = nn_bind(sock, ip_addr);
    // if(rc<=0){
    //     printf("Bind failed\n");
    // }

    // printf("Please enter a char\n");

    // getchar();

    // sleep(node_id);
    // rc = -1;
    // char buff[1024*1024] = "This is the Master side";
    // printf("Gonna send msg\n");
    // sleep(node_id*3);
    // ts_t start = gt.get_timestamp();
    // gt.send_to_center(&msg);
    // sock_send(sock, 1024*1024, buff);

    // while(rc<=0) rc = nn_send(sock, buff, 1024*1024, NN_DONTWAIT);
    // ts_t end = gt.get_timestamp();
    // printf("Send finish\n");


    // printf("Start ts is %lld\n", start%100000000);

    // printf("Msg send use %lld us\n", (end-start)/1000);


    for(int i=0; i<20; i++){
        gt.test_hw_ts();
    }


    // getchar();
    // printf("Nic ts: %lld\nTS: %lld\n", gt.get_timestamp_nic(), gt.get_timestamp());


    // print statistic

    // int times = clock_stat.times;

    // std::sort(clock_stat.duration1, clock_stat.duration1+times);
    // std::sort(clock_stat.duration2, clock_stat.duration2+times);
    
    // int avg1 = 0;
    // int avg2 = 0;


    // for(int i=0; i<times; i++){
    //     avg1 += clock_stat.duration1[i];
    //     avg2 += clock_stat.duration2[i];
    // }

    // if(times){
    //     avg1 /= times;
    //     avg2 /= times;
    // }

    // int min1 = clock_stat.duration1[0];
    // int max1 = clock_stat.duration1[times-1];

    // int min2 = clock_stat.duration2[0];
    // int max2 = clock_stat.duration2[times-1];

    // printf("Statistic:\nTimes: %d\n", times);
    // ts_t* ptr = clock_stat.duration1;
    // ts_t min = get_percent_val<ts_t>(ptr, 0, times);
    // ts_t mid = get_percent_val<ts_t>(ptr, 0.5, times);
    // ts_t p95 = get_percent_val<ts_t>(ptr, 0.95, times);
    // ts_t p99 = get_percent_val<ts_t>(ptr, 0.99, times);
    // ts_t max = get_percent_val<ts_t>(ptr, 1, times);

    // printf("---- print value from p95\n");
    // print_all_val<ts_t>(ptr, 0.5, 1, times);

    // printf("min:%lld\nmid:%lld\np95:%lld\np99:%lld\nmax:%lld\n", min, mid, p95, p99, max);
    gt.set_done();

    gt.finalize();
    printf("test finish\n");

    return 0;

}