#!/bin/bash
rm -rf /mnt/pmfs/*
rm -rf /mnt/pvm/*
/usr/bin/time -v ./db_test
#rm -rf /mnt/pmfs/*
#rm -rf /mnt/pvm/*



