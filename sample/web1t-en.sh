#!/bin/sh
#
#  Copyright(C) 2009-2013 Taro Watanabe <taro.watanabe@nict.go.jp>
#

shard=16
order=5

counts="LDC2006T13"
expgram_dir="expgram-directory"
mpi_dir="openmpi-directory"
pbs_queue="pbs-queue-name"
temporary_dir=/var/tmp

MPI="--mpi-dir ${mpi_dir} --mpi $shard"
THREAD="--threads $shard"
PBS="--mpi-dir ${mpi_dir} --mpi $shard --pbs --pbs-queue ${pbs_queue}"

exec ${expgram_dir}/expgram.py \
	--counts $counts/data \
	--output ngram.$order \
	--order $order \
	--remove-unk \
	--temporary-dir $temporary_dir \
	$THREAD
# for MPI, use $MPI
# for PBS, use $PBS
	
