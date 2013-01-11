#!/bin/sh
#
#  Copyright(C) 2009-2012 Taro Watanabe <taro.watanabe@nict.go.jp>
#

shard=16
order=5
counts=LDC2006T13

MPI="--mpi-dir {openmpi directory} --mpi $shard"
THREAD="--threads $shard"
PBS="--mpi-dir {openmpi directory} --mpi $shard --pbs --pbs-queue {pbs-queue}"

exec {directory to expgram}/expgram.py \
	--counts $counts/data \
	--output ngram.$order \
	--order $order \
	--remove-unk \
	--temporary-dir /var/tmp \
	$MPI or $THREAD or $PBS
	
