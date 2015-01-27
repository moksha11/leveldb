#!/bin/bash
rm -rf /mnt/pmfs/*
rm -rf /mnt/pvm/*
make


rm -rf /mnt/pmfs/*
sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
sudo sh -c "sync"
sudo sh -c "sync"
sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"

./db_bench --threads=1 --use_existing_db=0 --killdb=1 --benchmarks=fillseq
./db_bench --threads=1 --use_existing_db=1 --killdb=0 --benchmarks=readseq
#$APP_PREFIX ./db_bench --threads=1 --use_existing_db=1 --killdb=0 --benchmarks=readseq

rm -rf /mnt/pmfs/*
sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
sudo sh -c "sync"
sudo sh -c "sync"
sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"

./db_bench --threads=1 --use_existing_db=0 --killdb=1 --num=200000 --benchmarks=fillrandom
#$APP_PREFIX ./db_bench --threads=1 --use_existing_db=1 --killdb=0 --benchmarks=readrandom
./db_bench --threads=1 --use_existing_db=1 --killdb=0 --benchmarks=readrandom
exit
#rm -rf /mnt/pmfs/*



#$APP_PREFIX "./db_bench --threads=1 --num=100000 --value_size=500"



