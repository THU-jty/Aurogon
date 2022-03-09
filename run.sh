#!/bin/bash
echo $1
res_dir="./zresult/$1/"
mkdir -p ${res_dir}
cp arg ${res_dir}
echo "test start"

x=50
exe=aurogon

./start.sh
nohup /root/nfs_j/aurogon/gt_center >${res_dir}/gt_log 2>&1  &
echo "gt_center start"

ssh root@172.23.12.128 "cd /root/nfs_j/aurogon; ./${exe} -nid 0 >"${res_dir}"/node0 2>&1"  &
ssh root@172.23.12.131 "cd /root/nfs_j/aurogon; ./${exe} -nid 1 >"${res_dir}"/node1 2>&1"  &
ssh root@172.23.12.125 "cd /root/nfs_j/aurogon; ./${exe} -nid 2 >"${res_dir}"/node2 2>&1"  &

#wait
sleep $x

echo "test end"

ssh root@172.23.12.128 "pkill ${exe}"
ssh root@172.23.12.131 "pkill ${exe}"
ssh root@172.23.12.125 "pkill ${exe}"
pkill gt_center

