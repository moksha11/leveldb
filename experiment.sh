#!/bin/bash
set -x

let opt=0

#echo "export NVMALLOC_HOME=/home/stewart/nvmalloc" >> ~/.bashrc
#echo "export NVMALLOC_HOME=/home/stewart/nvmalloc" >> $HOME/.bash_profile

#source ~/.bashrc
#source $HOME/.bash_profile

sudo rm -rf /mnt/pmfs/*
sudo umount  /mnt/pmfs

rm tmp.txt
rm tmp1.txt

sudo ~/nvmalloc/scripts/memthrotle_hendrix.sh 0 2
sudo ~/nvmalloc/scripts/memthrotle_hendrix.sh 1 2

echo "run all?"
read opt

print "OPT enabled " $opt

if [ $opt -eq 1 ]
   then


	cd ~/nvmalloc
	~/nvmalloc/nvkernel*/ramdisk_create.sh 4096
	sleep 2

	sed -i 's/NOFAKE_NVMAP/FAKE_NVMAP/g' Makefile
	make clean
	make -j4
	sudo make install

	cd ~/apps
	cd leveldb
	make 

	./rundbbench.sh  &>> pvmuser_pmfs.out
	./rundbbench.sh  &>> pvmuser_pmfs.out
	echo "APP: LDB-UsrObj-SysPMFS" &>> pvmuser_pmfs.out

	grep -r "APP" pvmuser_pmfs.out | awk '{ print $2 }' &>> tmp.txt
	grep -r "User" pvmuser_pmfs.out | awk '{ sum += $4 } END { if (NR > 0) print sum / NR }' &>> tmp.txt
	grep -r "System" pvmuser_pmfs.out | awk '{ sum += $4 } END { if (NR > 0) print sum / NR }' &>> tmp.txt
  fi

	echo "finished user level execution"
	sleep 2
	sudo umount /mnt/pmfs

	~/nvmalloc/nvkernel*/create_pool 1500000
	#Create a ramdisk
	~/nvmalloc/nvkernel*/ramdisk_create.sh 1024

	cd ~/nvmalloc
	sed -i 's/FAKE_NVMAP/NOFAKE_NVMAP/g' Makefile
	make clean
	make -j4
	sudo make install
	cd ~/apps
	cd leveldb
	
	sed -i 's/nvinit_(500)/nvinit_(1500)/g' db/db_bench.cc
	make clean
	make 

	./rundbbench.sh  &> pvmkernel_pmfs.out

	sed -i 's/nvinit_(1500)/nvinit_(500)/g' db/db_bench.cc
	make clean
	make 
	./rundbbench.sh  &>> pvmkernel_pmfs.out
	echo "APP: LDB-UsrObj-SysPVM" &>> pvmkernel_pmfs.out


	grep -r "APP" pvmkernel_pmfs.out | awk '{ print $2 }' &>> tmp1.txt
	grep -r "User" pvmkernel_pmfs.out | awk '{ sum += $4 } END { if (NR > 0) print sum / NR }' &>> tmp1.txt
	grep -r "System" pvmkernel_pmfs.out | awk '{ sum += $4 } END { if (NR > 0) print sum / NR }' &>> tmp1.txt

paste -d '\t' tmp.txt tmp1.txt &> results/all_experiments.out


