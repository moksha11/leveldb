#!/bin/bash
rm -rf /mnt/pmfs/*
rm -rf /mnt/pvm/*
#/usr/bin/time -v ./db_bench
/usr/bin/time -v ./db_bench_pmfs
rm -rf /mnt/pmfs/*
rm -rf /mnt/pvm/*



