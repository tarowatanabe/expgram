#!/bin/sh
#
#  Copyright(C) 2009-2012 Taro Watanabe <taro.watanabe@nict.go.jp>
#

shard=16
order=7

### transform the content of LDC2009T08, into the directory, "data"
orig=LDC2009T08/data

if test ! -e data; then
  mkdir data 
fi

for ngram in `find $orig -name "*gms"`; do
  basename=`basename $ngram`

  if test "$basename" != "1gms"; then
    if test ! -e data/$basename; then
      ln -s ../$ngram data/$basename 
    fi
  fi 
done

## special treatment for 1gms, since we need vocab.gz and vocab_cs.gz, total
if test ! -e data/1gms; then
  mkdir data/1gms
fi 

rm -f data/1gms/vocab.gz
rm -f data/1gms/vocab_cs.gz

(cd data/1gms && ln -s ../../$orig/1gms/1gm-0000.gz vocab.gz)

zcat data/1gms/vocab.gz | sort -k2 -r -n | gzip -c > data/1gms/vocab_cs.gz
zcat data/1gms/vocab.gz | gawk 'BEGIN{ sum = 0; } { sum += $2;} END { print sum; }' > data/1gms/total
### finished! now collect counts from "data" and estimate LM

export TMPDIR_SPEC=/var/tmp

MPI="--mpi-dir {openmpi directory} --mpi $shard"
THREAD="--threads $shard"
PBS="--mpi-dir {openmpi directory} --mpi $shard --pbs --pbs-queue {pbs-queue}"

exec {directory to expgram}/expgram.py \
	--counts data \
	--output ngram.$order \
	--order $order \
	--remove-unk \
	--expgram-dir {directory to expgram} \
	$MPI or $THREAD or $PBS
	
