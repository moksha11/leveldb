#!/bin/bash
rm -rf /mnt/pmfs/*
rm -rf /mnt/pvm/*
make
rm -rf /mnt/pmfs/*
sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
sudo sh -c "sync"
sudo sh -c "sync"
sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
#./db_bench --threads=1
$APP_PREFIX "./db_bench --threads=1" # --num=100000 --value_size=500"
#$APP_PREFIX "./db_bench --threads=1 --benchmarks=fillseq --num=500000 --value_size=512"
#$APP_PREFIX "./db_bench --threads=1 --benchmarks=fillsync --num=1000000 --value_size=512"
#rm -rf /mnt/pmfs/*
#rm -rf /mnt/pvm/*



