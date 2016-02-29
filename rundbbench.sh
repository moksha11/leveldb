#!/bin/bash
sudo rm -rf /mnt/pmfs/*
sudo rm -rf /mnt/pvm/*
sudo rm -rf /tmp/ramdisk/*
sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
sudo sh -c "sync"
sudo sh -c "sync"
sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
#./db_bench --threads=1
#$APP_PREFIX "
./db_bench --threads=1 --num=50000 --benchmarks=fillsync #--value_size=1000 --write_buffer_size=629145600
#./db_bench --threads=1 --num=10000 --value_size=1000 
#$APP_PREFIX "./db_bench --threads=1 --benchmarks=fillseq --num=500000 --value_size=512"
#$APP_PREFIX "./db_bench --threads=1 --benchmarks=fillsync --num=1000000 --value_size=512"
#rm -rf /mnt/pmfs/*
#rm -rf /mnt/pvm/*



