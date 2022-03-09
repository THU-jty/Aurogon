#!/bin/bash

#num=$(($1))
#end=$(($2))

arg0=txn_size
arg1=zipfan
arg2=read_snapshot_ratio

arr0=(4 8 16)
arr1=(0 50 80 90 99)
arr2=(0 50)

#for((i1=${num};i2<${end};i3++));
#do
#./run.sh A_v2_$i
#done

num=0
end=2
# runs you want to test

for v0 in ${arr0[@]};
do
sed -i "/$arg0/d" arg
sed -i "1i\\$arg0 $v0" arg
for v1 in ${arr1[@]};
do
sed -i "/$arg1/d" arg
sed -i "1i\\$arg1 $v1" arg
for v2 in ${arr2[@]};
do
sed -i "/$arg2/d" arg
sed -i "1i\\$arg2 $v2" arg

for((ii=${num};ii<${end};ii++));
do
echo "AA_sz${v0}_zip${v1}_${v2}_${ii}"
./run.sh AA_sz${v0}_zip${v1}_${v2}_${ii}
done


done
done
done