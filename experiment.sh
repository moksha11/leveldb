#!/bin/bash
set -x

let opt=0

#echo "export NVMALLOC_HOME=/home/stewart/nvmalloc" >> ~/.bashrc
#echo "export NVMALLOC_HOME=/home/stewart/nvmalloc" >> $HOME/.bash_profile

#source ~/.bashrc
#source $HOME/.bash_profile

sudo rm -rf /mnt/pmfs/*
sudo umount  /mnt/pmfs

#sudo ~/nvmalloc/scripts/memthrotle_hendrix.sh 0 2
#sudo ~/nvmalloc/scripts/memthrotle_hendrix.sh 1 2

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
	exit
	./rundbbench.sh  &> pvmuser_pmfs.out
	./rundbbench.sh  &>> pvmuser_pmfs.out
  else

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
	./rundbbench.sh  &> pvmuser_pmfs.out
	./rundbbench.sh  &>> pvmuser_pmfs.out

	echo "finished user level execution"
	sleep 2
	sudo umount /mnt/pmfs

	~/nvmalloc/nvkernel*/create_pool 2000000
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
fi
