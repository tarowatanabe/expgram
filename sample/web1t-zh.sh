#!/bin/sh
#
#  Copyright(C) 2009-2013 Taro Watanabe <taro.watanabe@nict.go.jp>
#

shard=16
order=5

## some directories
expgram_dir="expgram-directory"
mpi_dir="openmpi-directory"
pbs_queue="pbs-queue-name"
temporary_dir=/var/tmp

## transform the contents of LDC2010T06 into the direcoty "data"
orig=LDC2010T06/data
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

for ((n=1;n<=5;++n)); do
  if test ! -e $dest/${n}gms; then
    mkdir $dest/${n}gms
  fi
done

## unigram...
n=1
echo "order $n"

file_orig=`printf "$orig/ngrams-%05d-of-00394.gz" 0`
file_ngram=`printf "$dest/%dgms/vocab.gz" $n`

ln -sf $file_orig $file_ngram
zcat $dest/1gms/vocab.gz | sort -k2 -r -n | gzip -c > $dest/1gms/vocab_cs.gz
zcat $dest/1gms/vocab.gz | gawk 'BEGIN{ sum = 0; } { sum += $2;} END { print sum; }' > $dest/1gms/total

### bigrams
n=2
echo "order $n"

rm -rf $dest/${n}gms/${n}gm.idx
for ((i=1;i<=29;++i)); do
  file_orig=`printf "$orig/ngrams-%05d-of-00394.gz" $i`
  file_ngram=`printf "$dest/%dgms/%dgm-%04d.gz" $n $n $i`
  
  ln -sf $file_orig $file_ngram

  basename=`basename $file_ngram`
  ngram=`zcat $file_ngram | head -1 | gawk '{for (i=1;i<NF;++i) {printf "%s ", $i; };}'`
  printf "${basename}\t${ngram}\n" >> $dest/${n}gms/${n}gm.idx
done

### trigrams
n=3
echo "order $n"

rm -rf $dest/${n}gms/${n}gm.idx
for ((i=30;i<=132;++i)); do
  file_orig=`printf "$orig/ngrams-%05d-of-00394.gz" $i`
  file_ngram=`printf "$dest/%dgms/%dgm-%04d.gz" $n $n $i`

  ln -sf $file_orig $file_ngram

  basename=`basename $file_ngram`
  ngram=`zcat $file_ngram | head -1 | gawk '{for (i=1;i<NF;++i) {printf "%s ", $i; };}'`
  printf "${basename}\t${ngram}\n" >> $dest/${n}gms/${n}gm.idx
done

### 4-grams
n=4
echo "order $n"

rm -rf $dest/${n}gms/${n}gm.idx
for ((i=133;i<=267;++i)); do
  file_orig=`printf "$orig/ngrams-%05d-of-00394.gz" $i`
  file_ngram=`printf "$dest/%dgms/%dgm-%04d.gz" $n $n $i`

  ln -sf $file_orig $file_ngram

  basename=`basename $file_ngram`
  ngram=`zcat $file_ngram | head -1 | gawk '{for (i=1;i<NF;++i) {printf "%s ", $i; };}'`
  printf "${basename}\t${ngram}\n" >> $dest/${n}gms/${n}gm.idx
done

### 5-grams
n=5
echo "order $n"

rm -rf $dest/${n}gms/${n}gm.idx
for ((i=268;i<=393;++i)); do
  file_orig=`printf "$orig/ngrams-%05d-of-00394.gz" $i`
  file_ngram=`printf "$dest/%dgms/%dgm-%04d.gz" $n $n $i`

  ln -sf $file_orig $file_ngram

  basename=`basename $file_ngram`
  ngram=`zcat $file_ngram | head -1 | gawk '{for (i=1;i<NF;++i) {printf "%s ", $i; };}'`
  printf "${basename}\t${ngram}\n" >> $dest/${n}gms/${n}gm.idx
done

## finished!
## Now collect counts/estimate lm from "$dest"

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
	
