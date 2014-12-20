#!/bin/bash
rm -rf /mnt/pmfs/*
rm -rf /mnt/pvm/*
make
/usr/bin/time -v ./db_bench --threads=1
rm -rf /mnt/pmfs/*
rm -rf /mnt/pvm/*



