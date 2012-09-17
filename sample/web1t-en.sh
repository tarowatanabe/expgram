#!/bin/sh
#
#  Copyright(C) 2009-2012 Taro Watanabe <taro.watanabe@nict.go.jp>
#

shard=16
order=5

export TMPDIR_SPEC=/var/tmp

MPI="--mpi-dir {openmpi directory} --mpi $shard"
THREAD="--threads $shard"
PBS="--mpi-dir {openmpi directory} --mpi $shard --pbs --pbs-queue {pbs-queue}"

exec {directory to expgram}/expgram.py \
	--counts LDC2006T13/data \
	--output ngram.$order \
	--order $order \
	--remove-unk \
	--expgram-dir {directory to expgram} \
	$MPI or $THREAD or $PBS
	
