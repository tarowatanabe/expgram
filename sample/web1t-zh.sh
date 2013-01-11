#!/bin/sh
#
#  Copyright(C) 2009-2012 Taro Watanabe <taro.watanabe@nict.go.jp>
#

shard=16
order=5

## transform!
orig=LDC2010T06/data

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

if test ! -e data; then
  mkdir data
fi

for ((n=1;n<=5;++n)); do
  if test ! -e data/${n}gms; then
    mkdir data/${n}gms
  fi
done

## unigram...
n=1
echo "order $n"

file_orig=`printf "$orig/ngrams-%05d-of-00394.gz" 0`
file_ngram=`printf "data/%dgms/vocab.gz" $n`

ln -sf $file_orig $file_ngram
zcat data/1gms/vocab.gz | sort -k2 -r -n | gzip -c > data/1gms/vocab_cs.gz
zcat data/1gms/vocab.gz | gawk 'BEGIN{ sum = 0; } { sum += $2;} END { print sum; }' > data/1gms/total

### bigrams
n=2
echo "order $n"

rm -rf data/${n}gms/${n}gm.idx
for ((i=1;i<=29;++i)); do
  file_orig=`printf "$orig/ngrams-%05d-of-00394.gz" $i`
  file_ngram=`printf "data/%dgms/%dgm-%04d.gz" $n $n $i`
  
  ln -sf $file_orig $file_ngram

  basename=`basename $file_ngram`
  ngram=`zcat $file_ngram | head -1 | gawk '{for (i=1;i<NF;++i) {printf "%s ", $i; };}'`
  printf "${basename}\t${ngram}\n" >> data/${n}gms/${n}gm.idx
done

### trigrams
n=3
echo "order $n"

rm -rf data/${n}gms/${n}gm.idx
for ((i=30;i<=132;++i)); do
  file_orig=`printf "$orig/ngrams-%05d-of-00394.gz" $i`
  file_ngram=`printf "data/%dgms/%dgm-%04d.gz" $n $n $i`

  ln -sf $file_orig $file_ngram

  basename=`basename $file_ngram`
  ngram=`zcat $file_ngram | head -1 | gawk '{for (i=1;i<NF;++i) {printf "%s ", $i; };}'`
  printf "${basename}\t${ngram}\n" >> data/${n}gms/${n}gm.idx
done

### 4-grams
n=4
echo "order $n"

rm -rf data/${n}gms/${n}gm.idx
for ((i=133;i<=267;++i)); do
  file_orig=`printf "$orig/ngrams-%05d-of-00394.gz" $i`
  file_ngram=`printf "data/%dgms/%dgm-%04d.gz" $n $n $i`

  ln -sf $file_orig $file_ngram

  basename=`basename $file_ngram`
  ngram=`zcat $file_ngram | head -1 | gawk '{for (i=1;i<NF;++i) {printf "%s ", $i; };}'`
  printf "${basename}\t${ngram}\n" >> data/${n}gms/${n}gm.idx
done

### 5-grams
n=5
echo "order $n"

rm -rf data/${n}gms/${n}gm.idx
for ((i=268;i<=393;++i)); do
  file_orig=`printf "$orig/ngrams-%05d-of-00394.gz" $i`
  file_ngram=`printf "data/%dgms/%dgm-%04d.gz" $n $n $i`

  ln -sf $file_orig $file_ngram

  basename=`basename $file_ngram`
  ngram=`zcat $file_ngram | head -1 | gawk '{for (i=1;i<NF;++i) {printf "%s ", $i; };}'`
  printf "${basename}\t${ngram}\n" >> data/${n}gms/${n}gm.idx
done

## finished! collect counts/estimate lm from "data"

export TMPDIR_SPEC=/var/tmp

MPI="--mpi-dir {openmpi directory} --mpi $shard"
THREAD="--threads $shard"
PBS="--mpi-dir {openmpi directory} --mpi $shard --pbs --pbs-queue {pbs-queue}"

exec {directory to expgram}/expgram.py \
	--counts data \
	--output ngram.$order \
	--order $order \
	--remove-unk \
	--temporary-dir /var/tmp \
	$MPI or $THREAD or $PBS
	
