#!/bin/bash

#num=$(($1))
#end=$(($2))

arg1=scale_factor
arg2=worker_per_server
arg3=neworder_local_ratio


arr1=(0 1)
arr2=(1 2 4 8)
arr3=(0 20 50 99)


#for((i1=${num};i2<${end}-+;i3++));
#do
#./run.sh A_v2_$i
#done

num=0
end=2
# runs you want to test

for v1 in ${arr1[@]};
do
sed -i "/$arg1/d" arg
sed -i "1i\\$arg1 $v1" arg
for v2 in ${arr2[@]};
do
sed -i "/$arg2/d" arg
sed -i "1i\\$arg2 $v2" arg
for v3 in ${arr3[@]};
do
sed -i "/$arg3/d" arg
sed -i "1i\\$arg3 $v3" arg

for((ii=${num};ii<${end};ii++));
do

echo "AB_scawh${v1}_thd${v2}_local${v3}_${ii}"
./run.sh AB_scawh${v1}_thd${v2}_local${v3}_${ii}
sleep 5
done


done
done
done