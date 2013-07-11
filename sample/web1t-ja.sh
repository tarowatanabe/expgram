#!/bin/sh
#
#  Copyright(C) 2009-2013 Taro Watanabe <taro.watanabe@nict.go.jp>
#

shard=16
order=7

## some directories
expgram_dir="expgram-directory"
mpi_dir="openmpi-directory"
pbs_queue="pbs-queue-name"
temporary_dir=/var/tmp

### transform the content of LDC2009T08, into the directory, "data"
orig=LDC2009T08/data
dest=data

abs_path() {
  dir__=$1
  "cd" "$dir__"
  if test "$?" = "0"; then
    /bin/pwd
    "cd" -  &>/dev/null
  fi
}

## absolute direcotry
orig=`abs_path $orig`

if test ! -e $dest; then
  mkdir $dest
fi

for ngram in `find $orig -name "*gms"`; do
  basename=`basename $ngram`

  if test "$basename" != "1gms"; then
    if test ! -e $dest/$basename; then
      ln -s $ngram $dest/$basename 
    fi
  fi 
done

## special treatment for 1gms, since we need vocab.gz and vocab_cs.gz, total
if test ! -e $dest/1gms; then
  mkdir $dest/1gms
fi 

rm -f $dest/1gms/vocab.gz
rm -f $dest/1gms/vocab_cs.gz

(cd $dest/1gms && ln -s $orig/1gms/1gm-0000.gz vocab.gz)

zcat $dest/1gms/vocab.gz | sort -k2 -r -n | gzip -c > $dest/1gms/vocab_cs.gz
zcat $dest/1gms/vocab.gz | gawk 'BEGIN{ sum = 0; } { sum += $2;} END { print sum; }' > $dest/1gms/total
### finished!
### Now collect counts from "$dest" and estimate LM

MPI="--mpi-dir ${mpi_dir} --mpi $shard"
THREAD="--threads $shard"
PBS="--mpi-dir ${mpi_dir} --mpi $shard --pbs --pbs-queue ${pbs_queue}"

exec ${expgram_dir}/expgram.py \
	--counts $dest \
	--output ngram.$order \
	--order $order \
	--remove-unk \
	--temporary-dir $temporary_dir \
	$THREAD
# for MPI, use $MPI
# for PBS, use $PBS
