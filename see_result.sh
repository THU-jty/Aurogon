#!/bin/bash

res_dir=/root/nfs_j/aurogon/zresult/$1/

#cat ${res_dir}/node0 ${res_dir}/node1 \
#${res_dir}/node2 | grep "Txn avg lat(including abort):" \
#| awk 'BEGIN{count=0;print "avg latency:"} {count+=$5; print "node" NR-1 ": "$5}
#END{print count/NR}'
#
#cat ${res_dir}/node0 ${res_dir}/node1 \
#${res_dir}/node2 | grep "Txn thpt:" \
#| awk 'BEGIN{count=0;print "Txn thpt:"} {count+=$3; print "node" NR-1 ": "$3}
#END{print count}'

#cat ${res_dir}/node0 ${res_dir}/node1 \
#${res_dir}/node2 | grep "Txn avg lat(including abort):" \
#| awk 'BEGIN{count=0;printf "avg_latency: "} {count+=$5;}
#END{print count/NR}'
#
#cat ${res_dir}/node0 ${res_dir}/node1 \
#${res_dir}/node2 | grep "Txn thpt:" \
#| awk 'BEGIN{count=0;printf "Txn_thpt: "} {count+=$3;}
#END{print count}'
#

if [[ $2 = "4" ]];
then
cat ${res_dir}/node0 ${res_dir}/node1 \
${res_dir}/node2 ${res_dir}/node3 \
 | grep "error\|full\|hard\|overload\|????????????????" \
| awk 'BEGIN{ if (NR != 0) printf("error! ") }'

cat ${res_dir}/node0 ${res_dir}/node1 \
${res_dir}/node2 ${res_dir}/node3 \
| grep "Txn avg lat(including abort):" \
| awk 'BEGIN{count=0;} {count+=$5;}
END{printf ("Txn avg lat  %.5fus ",count/NR)}'


cat ${res_dir}/node0 ${res_dir}/node1 \
${res_dir}/node2 ${res_dir}/node3 \
| grep "Txn thpt:" \
| awk 'BEGIN{count=0;} {count+=$3;}
END{printf (" thpt  %.2f ", count);}'

cat ${res_dir}/node0 ${res_dir}/node1 \
${res_dir}/node2 ${res_dir}/node3 \
| grep "Txn avg lat(including abort):" \
| awk 'BEGIN{count=0;} {count+=$9;}
END{printf (" p99 lat  %.5f ",count/NR)}'

cat ${res_dir}/node0 ${res_dir}/node1 \
${res_dir}/node2 ${res_dir}/node3 \
| grep "Abort ratio" \
| awk 'BEGIN{count=0;} {count+=$6;}
END{printf ("  abort ratio %.5f ",count/NR)}'

cat ${res_dir}/node0 ${res_dir}/node1 \
${res_dir}/node2 ${res_dir}/node3 \
| grep "Txn thpt:" \
| awk 'BEGIN{count=0;}
END{if(NR < 4) printf(" lack! ")}'

echo ""

elif [[ $2 = "5" ]];
then

cat ${res_dir}/node0 ${res_dir}/node1 \
${res_dir}/node2 ${res_dir}/node3 \
${res_dir}/node4 \
 | grep "error\|full\|hard\|overload\|????????????????" \
| awk 'BEGIN{ if (NR != 0) printf("error! ") }'

cat ${res_dir}/node0 ${res_dir}/node1 \
${res_dir}/node2 ${res_dir}/node3 \
${res_dir}/node4 \
| grep "Txn avg lat(including abort):" \
| awk 'BEGIN{count=0;} {count+=$5;}
END{printf ("Txn avg lat  %.5fus ",count/NR)}'


cat ${res_dir}/node0 ${res_dir}/node1 \
${res_dir}/node2 ${res_dir}/node3 \
${res_dir}/node4 \
| grep "Txn thpt:" \
| awk 'BEGIN{count=0;} {count+=$3;}
END{printf (" thpt  %.2f ", count);}'

cat ${res_dir}/node0 ${res_dir}/node1 \
${res_dir}/node2 ${res_dir}/node3 \
${res_dir}/node4 \
| grep "Txn avg lat(including abort):" \
| awk 'BEGIN{count=0;} {count+=$9;}
END{printf (" p99 lat  %.5f ",count/NR)}'

cat ${res_dir}/node0 ${res_dir}/node1 \
${res_dir}/node2 ${res_dir}/node3 \
${res_dir}/node4 \
| grep "Abort ratio" \
| awk 'BEGIN{count=0;} {count+=$6;}
END{printf ("  abort ratio %.5f ",count/NR)}'

cat ${res_dir}/node0 ${res_dir}/node1 \
${res_dir}/node2 ${res_dir}/node3 \
${res_dir}/node4 \
| grep "Txn thpt:" \
| awk 'BEGIN{count=0;}
END{if(NR < 5) printf(" lack! ")}'

echo ""

elif [[ $2 = "6" ]];
then

cat ${res_dir}/node0 ${res_dir}/node1 \
${res_dir}/node2 ${res_dir}/node3 \
${res_dir}/node4 ${res_dir}/node5 \
 | grep "error\|full\|hard\|overload\|????????????????" \
| awk 'BEGIN{ if (NR != 0) printf("error! ") }'

cat ${res_dir}/node0 ${res_dir}/node1 \
${res_dir}/node2 ${res_dir}/node3 \
${res_dir}/node4 ${res_dir}/node5 \
| grep "Txn avg lat(including abort):" \
| awk 'BEGIN{count=0;} {count+=$5;}
END{printf ("Txn avg lat  %.5fus ",count/NR)}'


cat ${res_dir}/node0 ${res_dir}/node1 \
${res_dir}/node2 ${res_dir}/node3 \
${res_dir}/node4 ${res_dir}/node5 \
| grep "Txn thpt:" \
| awk 'BEGIN{count=0;} {count+=$3;}
END{printf (" thpt  %.2f ", count);}'

cat ${res_dir}/node0 ${res_dir}/node1 \
${res_dir}/node2 ${res_dir}/node3 \
${res_dir}/node4 ${res_dir}/node5 \
| grep "Txn avg lat(including abort):" \
| awk 'BEGIN{count=0;} {count+=$9;}
END{printf (" p99 lat  %.5f ",count/NR)}'

cat ${res_dir}/node0 ${res_dir}/node1 \
${res_dir}/node2 ${res_dir}/node3 \
${res_dir}/node4 ${res_dir}/node5 \
| grep "Abort ratio" \
| awk 'BEGIN{count=0;} {count+=$6;}
END{printf ("  abort ratio %.5f ",count/NR)}'

cat ${res_dir}/node0 ${res_dir}/node1 \
${res_dir}/node2 ${res_dir}/node3 \
${res_dir}/node4 ${res_dir}/node5 \
| grep "Txn thpt:" \
| awk 'BEGIN{count=0;}
END{if(NR < 6) printf(" lack! ")}'

echo ""

elif [[ $2 = "3" ]];
then

cat ${res_dir}/node0 ${res_dir}/node1 \
${res_dir}/node2 \
 | grep "error\|full\|hard\|overload\|????????????????" \
| awk 'BEGIN{ if (NR != 0) printf("error! ") }'

cat ${res_dir}/node0 ${res_dir}/node1 \
${res_dir}/node2 \
| grep "Txn avg lat(including abort):" \
| awk 'BEGIN{count=0;} {count+=$5;}
END{printf ("Txn avg lat  %.5fus ",count/NR)}'


cat ${res_dir}/node0 ${res_dir}/node1 \
${res_dir}/node2 \
| grep "Txn thpt:" \
| awk 'BEGIN{count=0;} {count+=$3;}
END{printf (" thpt  %.2f ", count);}'

cat ${res_dir}/node0 ${res_dir}/node1 \
${res_dir}/node2 \
| grep "Txn avg lat(including abort):" \
| awk 'BEGIN{count=0;} {count+=$9;}
END{printf (" p99 lat  %.5f ",count/NR)}'

cat ${res_dir}/node0 ${res_dir}/node1 \
${res_dir}/node2 \
| grep "Abort ratio" \
| awk 'BEGIN{count=0;} {count+=$6;}
END{printf ("  abort ratio %.5f ",count/NR)}'

cat ${res_dir}/node0 ${res_dir}/node1 \
${res_dir}/node2 \
| grep "Txn thpt:" \
| awk 'BEGIN{count=0;}
END{if(NR < 3) printf(" lack! ")}'

echo ""

else
  echo "wrong node count"
fi









#${res_dir}/node4 ${res_dir}/node5 \
#cat ${res_dir}/node0 ${res_dir}/node1 \
#${res_dir}/node2 | grep "Abort ratio" \
#| awk 'BEGIN{count=0;} {printf (" %s ", $6);}
#END{printf "\n"}'