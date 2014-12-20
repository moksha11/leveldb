#!/bin/bash
rm -rf /mnt/pmfs/*
rm -rf /mnt/pvm/*
<<<<<<< HEAD
make
/usr/bin/time -v ./db_bench --threads=1
=======
#/usr/bin/time -v ./db_bench
/usr/bin/time -v ./db_bench_pmfs
>>>>>>> 66cc2ba983d0fc2aae2f1d2904653d1cb613724a
rm -rf /mnt/pmfs/*
rm -rf /mnt/pvm/*



