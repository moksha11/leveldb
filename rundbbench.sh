#!/bin/bash
rm -rf /mnt/pmfs/*
rm -rf /mnt/pvm/*
make
$APP_PREFIX "./db_bench --threads=1"
rm -rf /mnt/pmfs/*
rm -rf /mnt/pvm/*



