# Aurogon: Taming Aborts in All Phases for Distributed In-Memory Transactions
This is an open source repository for [our paper](https://www.usenix.org/conference/fast22/presentation/jiang) in [FAST 2022](https://www.usenix.org/conference/fast22). 

The key idea of Aurogon is to mitigate request reordering, the major cause of transaction aborts in T/O-based systems, in all phases. 
Aurogon adopts three key techniques:
1) High-accuracy clock synchronization mechanism, 2LClock
2) Adaptive request deferral
3) Pre-attaching depedent requests

To be convenient for researchers to use, we seperate 2LClock from Aurogon and publish 2LClock's codes in this link: https://github.com/lizhiyuell/2LClock 
because we believe our distributed clocks can be used in many other interesting areas besides transaction processing.
Aurogon uses a simple-version 2LClock and we further improve it in this link.
If you have any questions, please contact with us via jty1313@126.com or ty-jiang18@mails.tsinghua.edu.cn

## System Requirements
1) Mellanox ConnectX-5 NICs and above
2) RDMA Driver: MLNX_OFED_LINUX-4.7-3.2.9.0-rhel7.6-x86_64
3) memcached (to exchange QP information when starting)

## Getting Started
- `cmake CMakeList.txt`
- `make`
- configure `start.sh`, create a file "/tmp/memcached.pid", add the server ip and port where you want to start memcached (you need to add  memcached server ip and port into `./arg` as well)
- configure `ifconfig`, a file used to start 2LClock, where the first line is the server count X, and each of the next X lines indicatas a server ip.
- configure `arg`, the argument file of Aurogon, and the introductions can be found in `argument_details`

For each run, you should follow the next steps
1) run `./start.sh` and `./gt_center` on the memcached server
2) run `./aurogon -nid x` on each server, where x is the id, ranging from 0 to SERVER_COUNT-1
3) run `killall.sh` if you want to break the test

`./run.sh` implements these steps. You can run `./run.sh $test_name` and the result can be found in `./zresult/$test_name`.

`./see_result.sh` can be used to have a quick view at results. Use `./see_result.sh $test_name $server_count`.

`mul_test.sh` and `mul_test2.sh` can be used to evaluate Aurogon under YCSB and TPC-C respectively in batches.


