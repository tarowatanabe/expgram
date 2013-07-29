#!/usr/bin/env python
#
#  Copyright(C) 2010-2013 Taro Watanabe <taro.watanabe@nict.go.jp>
#

#
#  a wrapper script for expgram!
#

import shutil
import time
import sys
import os, os.path
import subprocess

from optparse import OptionParser, make_option

opt_parser = OptionParser(
    option_list=[
        make_option("--counts", default="", action="store", type="string",
                    help="counts in Google format"),
        make_option("--counts-list", default="", action="store", type="string",
                    help="list of ngram counts either in Google format or in a plain format"),
        make_option("--corpus", default="", action="store", type="string",
                    help="corpus"),
        make_option("--corpus-list", default="", action="store", type="string",
                    help="list of corpus"),
        make_option("--order", default=5, action="store", type="int",
                    help="ngram order (default: %default)"),
        make_option("--output", default="", action="store", type="string",
                    help="ngram output"),
        make_option("--cutoff", default=1, action="store", type="int",
                    help="count cutoff threshold (default: 1 == keep all the counts)"),
        make_option("--kbest", default=0, action="store", type="int",
                    help="kbest vocabulary (default: 0 == keep all the counts)"),
        make_option("--vocab", default="", action="store", type="string",
                    help="vocabulary"),
        make_option("--tokenizer", default="", action="store",
                    help="tokenizer applied to data"),
        make_option("--remove-unk", default=None, action="store_true",
                    help="remove unk from lm estimation"),

        make_option("--erase-temporary", default=None, action="store_true",
                    help="erase temporary allocated disk space"),
        
        make_option("--first-step", default=1, action="store", type="int", metavar='STEP',
                    help="first step (default: %default):"
                    " 1 = vocabulary,"
                    " 2 = counts extraction,"
                    " 3 = counts index,"
                    " 4 = counts modification,"
                    " 5 = estimated language model,"
                    " 6 = backward trie,"
                    " 7 = quantization"),
        make_option("--last-step",  default=7, action="store", type="int", metavar='STEP',
                    help="last step (default: %default):"
                    " 1 = vocabulary,"
                    " 2 = counts extraction,"
                    " 3 = counts index,"
                    " 4 = counts modification,"
                    " 5 = estimated language model,"
                    " 6 = backward trie,"
                    " 7 = quantization"),
        
        # expgram Expgram directory
        make_option("--expgram-dir", default="", action="store", type="string",
                    metavar="DIRECTORY", help="expgram directory"),
        # MPI Implementation.. if different from standard location...
        make_option("--mpi-dir", default="", action="store", type="string",
                    metavar="DIRECTORY", help="MPI directory"),
        # temporary dir
        make_option("--temporary-dir", default="", action="store", type="string",
                    metavar="DIRECTORY", help="temporary directory"),
        ## max-malloc
        make_option("--max-malloc", default=8, action="store", type="float",
                    metavar="MALLOC", help="maximum memory in GB (default: %default)"),
        
        # perform threading or MPI training    
        make_option("--mpi", default=0, action="store", type="int",
                    help="# of processes for MPI-based parallel processing. Identical to --np for mpirun"),
        make_option("--mpi-host", default="", action="store", type="string",
                    help="list of hosts to run job. Identical to --host for mpirun", metavar="HOSTS"),
        make_option("--mpi-host-file", default="", action="store", type="string",
                    help="host list file to run job. Identical to --hostfile for mpirun", metavar="FILE"),
        
        make_option("--threads", default=2, action="store", type="int",
                    help="# of thrads for thread-based parallel processing"),
        
        make_option("--pbs", default=None, action="store_true",
                    help="PBS for launching processes"),
        make_option("--pbs-queue", default="ltg", action="store", type="string",
                    help="PBS queue for launching processes (default: %default)", metavar="NAME"),

        ## debug messages
        make_option("--debug", default=0, action="store", type="int"),
        ])


def run_command(command, logfile=None):
    try:
        if logfile:
            command += ' 2> ' + logfile
            
        retcode = subprocess.call(command, shell=True)
        if retcode:
            sys.exit(retcode)
    except:
        raise ValueError, "subprocess.call failed: %s" %(command)

def compressed_file(file):
    if not file:
        return file
    if os.path.exists(file):
        return file
    if os.path.exists(file+'.gz'):
	return file+'.gz'
    if os.path.exists(file+'.bz2'):
	return file+'.bz2'
    (base, ext) = os.path.splitext(file)
    if ext == '.gz' or ext == '.bz2':
	if os.path.exists(base):
	    return base
    return file

class Quoted:
    def __init__(self, arg):
        self.arg = arg
        
    def __str__(self):
        return '"' + str(self.arg) + '"'

class Option:
    def __init__(self, arg, value=None):
        self.arg = arg
        self.value = value

    def __str__(self,):
        option = self.arg
        
        if self.value is not None:
            if isinstance(self.value, int):
                option += " %d" %(self.value)
            elif isinstance(self.value, long):
                option += " %d" %(self.value)
            elif isinstance(self.value, float):
                option += " %.20g" %(self.value)
            else:
                option += " %s" %(str(self.value))
        return option

class Program:
    def __init__(self, *args):
        self.args = list(args[:])

    def __str__(self,):
        return ' '.join(map(str, self.args))
    
    def __iadd__(self, other):
        self.args.append(other)
        return self

    def append(self, other):
        self.args.append(other)
        return self

class PBS:
    def __init__(self, queue=""):
        self.queue = queue
        self.qsub = 'qsub'
            
    def run(self, command="", threads=1, memory=0.0, name="name", mpi=None, logfile=None):
        popen = subprocess.Popen("qsub -S /bin/sh", shell=True, stdin=subprocess.PIPE)

        pipe = popen.stdin
        
        pipe.write("#!/bin/sh\n")
        pipe.write("#PBS -S /bin/sh\n")
        pipe.write("#PBS -N %s\n" %(name))
        pipe.write("#PBS -W block=true\n")
        pipe.write("#PBS -e localhost:/dev/null\n")
        pipe.write("#PBS -o localhost:/dev/null\n")
        
        if self.queue:
            pipe.write("#PBS -q %s\n" %(self.queue))

        mem = ""
        if memory >= 1.0:
            mem=":mem=%dgb" %(int(memory))
        elif memory >= 0.001:
            mem=":mem=%dmb" %(int(memory * 1000))
        elif memory >= 0.000001:
            mem=":mem=%dkb" %(int(memory * 1000 * 1000))
        
        if mpi:
            pipe.write("#PBS -l select=%d:ncpus=%d:mpiprocs=1%s\n" %(mpi.number, threads, mem))
        else:
            pipe.write("#PBS -l select=1:ncpus=%d:mpiprocs=1%s\n" %(threads, mem))
        
        # setup variables
        if os.environ.has_key('TMPDIR_SPEC'):
            pipe.write("export TMPDIR_SPEC=%s\n" %(os.environ['TMPDIR_SPEC']))
        if os.environ.has_key('LD_LIBRARY_PATH'):
            pipe.write("export LD_LIBRARY_PATH=%s\n" %(os.environ['LD_LIBRARY_PATH']))
        if os.environ.has_key('DYLD_LIBRARY_PATH'):
            pipe.write("export DYLD_LIBRARY_PATH=%s\n" %(os.environ['DYLD_LIBRARY_PATH']))
        
        pipe.write("if test \"$PBS_O_WORKDIR\" != \"\"; then\n")
        pipe.write("  cd $PBS_O_WORKDIR\n")
        pipe.write("fi\n")
        
        prefix = ''
        if mpi:
            prefix = mpi.mpirun

            if mpi.dir:
                prefix += ' --prefix %s' %(mpi.dir)
            if os.environ.has_key('TMPDIR_SPEC'):
                prefix += ' -x TMPDIR_SPEC'
            if os.environ.has_key('LD_LIBRARY_PATH'):
                prefix += ' -x LD_LIBRARY_PATH'
            if os.environ.has_key('DYLD_LIBRARY_PATH'):
                prefix += ' -x DYLD_LIBRARY_PATH'
            prefix += ' '
        
        suffix = ''
        if logfile:
            suffix = " 2> %s" %(logfile)
        
        pipe.write(prefix + command + suffix + '\n')
        
        pipe.close()
        popen.wait()

class MPI:
    
    def __init__(self, dir="", hosts="", hosts_file="", number=0):
        
	self.dir = dir
	self.hosts = hosts
        self.hosts_file = hosts_file
        self.number = number
	
        if self.dir:
            if not os.path.exists(self.dir):
                raise ValueError, self.dir + " does not exist"
            self.dir = os.path.realpath(self.dir)

        if self.hosts_file:
            if not os.path.exists(self.hosts_file):
                raise ValueError, self.hosts_file + " does no exist"
            self.hosts_file = os.path.realpath(hosts_file)

        self.bindir = self.dir
	
        for binprog in ['mpirun']:
            if self.bindir:
                prog = os.path.join(self.bindir, 'bin', binprog)
                if not os.path.exists(prog):
                    prog = os.path.join(self.bindir, binprog)
                    if not os.path.exists(prog):
                        raise ValueError, prog + " does not exist at " + self.bindir
                    
                setattr(self, binprog, prog)
            else:
                setattr(self, binprog, binprog)
                
    def run(self, command, logfile=None):
        mpirun = self.mpirun
        if self.dir:
            mpirun += ' --prefix %s' %(self.dir)
        if self.number > 0:
            mpirun += ' --np %d' %(self.number)
        if self.hosts:
            mpirun += ' --host %s' %(self.hosts)
        elif self.hosts_file:
            mpirun += ' --hostfile %s' %(self.hosts_file)
        
        if os.environ.has_key('TMPDIR_SPEC'):
            mpirun += ' -x TMPDIR_SPEC'
        if os.environ.has_key('LD_LIBRARY_PATH'):
            mpirun += ' -x LD_LIBRARY_PATH'
        if os.environ.has_key('DYLD_LIBRARY_PATH'):
            mpirun += ' -x DYLD_LIBRARY_PATH'

	mpirun += ' ' + command

	run_command(mpirun, logfile=logfile)

class QSub:
    def __init__(self, mpi=None, pbs=None):
        self.mpi = mpi
        self.pbs = pbs
        
    def run(self, command, name="name", memory=0.0, threads=1, logfile=None):
        if logfile:
            print str(command), '2> %s' %(logfile)
        else:
            print str(command)

        if self.pbs:
            self.pbs.run(str(command), name=name, memory=memory, threads=threads, logfile=logfile)
        else:
            run_command(str(command), logfile=logfile)
    
    def mpirun(self, command, name="name", memory=0.0, threads=1, logfile=None):
        if not self.mpi:
            raise ValueError, "no mpi?"

        if logfile:
            print str(command), '2> %s' %(logfile)
        else:
            print str(command)

        if self.pbs:
            self.pbs.run(str(command), name=name, memory=memory, mpi=self.mpi, threads=threads, logfile=logfile)
        else:
            self.mpi.run(str(command), logfile=logfile)

class Expgram:
    def __init__(self, dir=""):
        bindirs = []
        
        if not dir:
            dir = os.path.abspath(os.path.dirname(__file__))
            bindirs.append(dir)
            parent = os.path.dirname(dir)
            if parent:
                dir = parent
        else:
            dir = os.path.realpath(dir)
            if not os.path.exists(dir):
                raise ValueError, dir + " does not exist"
            bindirs.append(dir)
        
	for subdir in ('bin', 'progs', 'scripts'): 
	    bindir = os.path.join(dir, subdir)
	    if os.path.exists(bindir) and os.path.isdir(bindir):
		bindirs.append(bindir)
	
        for binprog in (## vocabulary
                        'expgram_vocab', 'expgram_vocab_mpi',
                        
                        ## counts...
                        'expgram_counts_extract',  'expgram_counts_extract_mpi',
                        'expgram_counts_index',    'expgram_counts_index_mpi',
                        'expgram_counts_modify',   'expgram_counts_modify_mpi',
                        'expgram_counts_estimate', 'expgram_counts_estimate_mpi',
                        
                        ## final post-processing
                        'expgram_bound',    'expgram_bound_mpi',
                        'expgram_backward', 'expgram_backward_mpi',
                        'expgram_quantize', 'expgram_quantize_mpi',):
	    
	    for bindir in bindirs:
		prog = os.path.join(bindir, binprog)
                
                if not os.path.exists(prog): continue
                if os.path.isdir(prog): continue

                setattr(self, binprog, prog)
                break
            
	    if not hasattr(self, binprog):
	    	raise ValueError, binprog + ' does not exist'

class Corpus:
    def __init__(self, corpus="", counts="", corpus_list="", counts_list=""):
        self.corpus      = corpus
        self.counts      = counts
        self.corpus_list = corpus_list
        self.counts_list = counts_list

class VocabFile:
    def __init__(self, vocab):
        self.vocab = vocab
        
        if not os.path.exists(self.vocab):
            raise ValueError, "no vocabulary file? %s" %(self.vocab)

class Vocab:
    def __init__(self, expgram=None, corpus=None, output="",
                 tokenizer="", cutoff=1, kbest=0, max_malloc=4,
                 threads=4, mpi=None, pbs=None, debug=None):
        
        if kbest:
            self.vocab = output + '.vocab.' + str(kbest)
        else:
            self.vocab = output + '.vocab.' + str(cutoff)
        self.counts = output + '.vocab'
        self.log = self.counts + '.log'
        
        self.cutoff = cutoff
        self.kbest  = kbest
        
        self.mpi = mpi
        self.threads = threads
        self.pbs = pbs
        
        if self.mpi:
            self.threads = 1
        
        self.max_malloc = max_malloc

        command = Program(expgram.expgram_vocab)
        if mpi:
            command = Program(expgram.expgram_vocab_mpi)
        
        if os.path.exists(corpus.corpus):
            command += Option('--corpus', Quoted(corpus.corpus))
        if os.path.exists(corpus.corpus_list) or os.path.exists(corpus.counts_list):
            if os.path.exists(corpus.corpus_list):
                command += Option('--corpus-list', Quoted(corpus.corpus_list))
            if os.path.exists(corpus.counts_list):
                command += Option('--counts-list', Quoted(corpus.counts_list))
        else:
            command += Option('--map-line')
            
        command += Option('--output', Quoted(self.counts))
        
        if tokenizer:
            command += Option('--filter', Quoted(tokenizer))

        if mpi:
            command += Option('--prog', Quoted(expgram.expgram_vocab_mpi))

            if mpi.hosts:
                command += Option('--host', Quoted(mpi.hosts))
            elif mpi.hosts_file:
                command += Option('--hostfile', Quoted(mpi.hosts_file))

        else:
            command += Option('--threads', threads)
            
        if debug >= 2:
            command += Option('--debug', debug)
        else:
            command += Option('--debug')

        self.command = command
        
    def run(self):
        qsub = QSub(mpi=self.mpi, pbs=self.pbs)

        if self.mpi:
            qsub.mpirun(self.command, threads=self.threads, name="vocab", memory=self.max_malloc, logfile=self.log)
        else:
            qsub.run(self.command, threads=self.threads, name="vocab", memory=self.max_malloc, logfile=self.log)

        if self.kbest > 0:
            fp = open(self.vocab, 'w')
            i = 0
            for line in open(self.counts):
                if i == self.kbest: break
                tokens = line.split()
                if len(tokens) != 2: continue
                fp.write(tokens[0]+'\n')
                i += 1
        else:
            fp = open(self.vocab, 'w')
            for line in open(self.counts):
                tokens = line.split()
                if len(tokens) != 2: continue
                if int(tokens[1]) >= self.cutoff:
                    fp.write(tokens[0]+'\n')

class Counts:

    def __init__(self, counts=""):
        self.ngram = counts

class Extract:

    def __init__(self, expgram=None, corpus=None, output="",
                 vocab=None, order=5,
                 tokenizer="", max_malloc=4.0,
                 threads=4, mpi=None, pbs=None, debug=None):
        
        self.ngram = output + '.counts'
        self.log = self.ngram + '.log'
        
        self.mpi = mpi
        self.threads = threads
        self.pbs = pbs

        if self.mpi:
            self.threads = 1
            
            if tokenizer:
                self.threads += 1
        
        self.max_malloc = max_malloc
        
        command = Program(expgram.expgram_counts_extract)
        if mpi:
            command = Program(expgram.expgram_counts_extract_mpi)
        
        if os.path.exists(corpus.corpus):
            command += Option('--corpus', Quoted(corpus.corpus))
        if os.path.exists(corpus.corpus_list) or os.path.exists(corpus.counts_list):
            if os.path.exists(corpus.corpus_list):
                command += Option('--corpus-list', Quoted(corpus.corpus_list))
            if os.path.exists(corpus.counts_list):
                command += Option('--counts-list', Quoted(corpus.counts_list))
        else:
            command += Option('--map-line')
        
        command += Option('--output', Quoted(self.ngram))
        command += Option('--order', order)
        
        if vocab is not None:
            command += Option('--vocab', Quoted(vocab.vocab))
        if tokenizer:
            command += Option('--filter', Quoted(tokenizer))
        
        command += Option('--max-malloc', max_malloc)

        if mpi:
            command += Option('--prog', Quoted(expgram.expgram_counts_extract_mpi))

            if mpi.hosts:
                command += Option('--host', Quoted(mpi.hosts))
            elif mpi.hosts_file:
                command += Option('--hostfile', Quoted(mpi.hosts_file))

        else:
            command += Option('--threads', threads)

        if debug >= 2:
            command += Option('--debug', debug)
        else:
            command += Option('--debug')
            
        self.command = command

    def run(self):
        qsub = QSub(mpi=self.mpi, pbs=self.pbs)

        if self.mpi:
            qsub.mpirun(self.command, threads=self.threads, name="extract", memory=self.max_malloc, logfile=self.log)
        else:
            qsub.run(self.command, threads=self.threads, name="extract", memory=self.max_malloc, logfile=self.log)

class Index:

    def __init__(self, expgram=None, output="", temporary="",
                 extract=None,
                 max_malloc=4,
                 threads=4, mpi=None, pbs=None, debug=None):
        
        self.ngram = output + '.index'
        self.log = self.ngram + '.log'
        
        self.mpi = mpi
        self.threads = threads
        self.pbs = pbs

        if self.mpi:
            self.threads = 2
        
        self.max_malloc = max_malloc

        
        command = Program(expgram.expgram_counts_index)
        if mpi:
            command = Program(expgram.expgram_counts_index_mpi)

        command += Option('--ngram', Quoted(extract.ngram))
        command += Option('--output', Quoted(self.ngram))

        if temporary:
            command += Option('--temporary', Quoted(temporary))
        
        if mpi:
            command += Option('--prog', Quoted(expgram.expgram_counts_index_mpi))

            if mpi.hosts:
                command += Option('--host', Quoted(mpi.hosts))
            elif mpi.hosts_file:
                command += Option('--hostfile', Quoted(mpi.hosts_file))

        else:
            command += Option('--shard', threads)

        if debug >= 2:
            command += Option('--debug', debug)
        else:
            command += Option('--debug')

        self.command = command

    def run(self):
        qsub = QSub(mpi=self.mpi, pbs=self.pbs)

        if self.mpi:
            qsub.mpirun(self.command, threads=self.threads, name="index", memory=self.max_malloc, logfile=self.log)
        else:
            qsub.run(self.command, threads=self.threads, name="index", memory=self.max_malloc, logfile=self.log)

class Modify:

    def __init__(self, expgram=None, output="", temporary="",
                 index=None,
                 max_malloc=4,
                 threads=4, mpi=None, pbs=None, debug=None):
        
        self.ngram = output + '.modified'
        self.log = self.ngram + '.log'
        
        self.mpi = mpi
        self.threads = threads
        self.pbs = pbs

        if self.mpi:
            self.threads = 2
        
        self.max_malloc = max_malloc
        
        command = Program(expgram.expgram_counts_modify)
        if mpi:
            command = Program(expgram.expgram_counts_modify_mpi)

        command += Option('--ngram', Quoted(index.ngram))
        command += Option('--output', Quoted(self.ngram))
        
        if temporary:
            command += Option('--temporary', Quoted(temporary))

        if mpi:
            command += Option('--prog', Quoted(expgram.expgram_counts_modify_mpi))

            if mpi.hosts:
                command += Option('--host', Quoted(mpi.hosts))
            elif mpi.hosts_file:
                command += Option('--hostfile', Quoted(mpi.hosts_file))

        if debug >= 2:
            command += Option('--debug', debug)
        else:
            command += Option('--debug')
            
        self.command = command

    def run(self):
        qsub = QSub(mpi=self.mpi, pbs=self.pbs)

        if self.mpi:
            qsub.mpirun(self.command, threads=self.threads, name="modify", memory=self.max_malloc, logfile=self.log)
        else:
            qsub.run(self.command, threads=self.threads, name="modify", memory=self.max_malloc, logfile=self.log)

class Estimate:

    def __init__(self, expgram=None, output="", temporary="",
                 modify=None, remove_unk=None,
                 max_malloc=4,
                 threads=4, mpi=None, pbs=None, debug=None):
        
        self.ngram = output + '.estimated'
        self.log = self.ngram + '.log'
        
        self.mpi = mpi
        self.threads = threads
        self.pbs = pbs

        if self.mpi:
            self.threads = 2
        
        self.max_malloc = max_malloc
        
        command = Program(expgram.expgram_counts_estimate)
        if mpi:
            command = Program(expgram.expgram_counts_estimate_mpi)
            
        command += Option('--ngram', Quoted(modify.ngram))
        command += Option('--output', Quoted(self.ngram))
        
        if temporary:
            command += Option('--temporary', Quoted(temporary))

        if remove_unk:
            command += Option('--remove-unk')
        
        if mpi:
            command += Option('--prog', Quoted(expgram.expgram_counts_estimate_mpi))

            if mpi.hosts:
                command += Option('--host', Quoted(mpi.hosts))
            elif mpi.hosts_file:
                command += Option('--hostfile', Quoted(mpi.hosts_file))

        if debug >= 2:
            command += Option('--debug', debug)
        else:
            command += Option('--debug')

        self.command = command

    def run(self):
        qsub = QSub(mpi=self.mpi, pbs=self.pbs)
        
        if self.mpi:
            qsub.mpirun(self.command, threads=self.threads, name="estimate", memory=self.max_malloc, logfile=self.log)
        else:
            qsub.run(self.command, threads=self.threads, name="estimate", memory=self.max_malloc, logfile=self.log)


class Backward:

    def __init__(self, expgram=None, output="", temporary="",
                 estimate=None,
                 max_malloc=4,
                 threads=4, mpi=None, pbs=None, debug=None):
        
        self.ngram = output + '.lm'
        self.log = self.ngram + '.log'
        
        self.mpi     = mpi
        self.threads = threads
        self.pbs     = pbs

        if self.mpi:
            self.threads = 2
        
        self.max_malloc = max_malloc
        
        command = Program(expgram.expgram_backward)
        if mpi:
            command = Program(expgram.expgram_backward_mpi)
        
        command += Option('--ngram', Quoted(estimate.ngram))
        command += Option('--output', Quoted(self.ngram))

        if temporary:
            command += Option('--temporary', Quoted(temporary))
        
        if mpi:
            command += Option('--prog', Quoted(expgram.expgram_backward_mpi))

            if mpi.hosts:
                command += Option('--host', Quoted(mpi.hosts))
            elif mpi.hosts_file:
                command += Option('--hostfile', Quoted(mpi.hosts_file))

        if debug >= 2:
            command += Option('--debug', debug)
        else:
            command += Option('--debug')

        self.command = command

    def run(self):
        qsub = QSub(mpi=self.mpi, pbs=self.pbs)

        if self.mpi:
            qsub.mpirun(self.command, threads=self.threads, name="backward", memory=self.max_malloc, logfile=self.log)
        else:
            qsub.run(self.command, threads=self.threads, name="backward", memory=self.max_malloc, logfile=self.log)

class Quantize:

    def __init__(self, expgram=None, output="", temporary="",
                 backward=None,
                 max_malloc=4,
                 threads=4, mpi=None, pbs=None, debug=None):
        
        self.ngram = output + '.lm.quantize'
        self.log = self.ngram + '.log'
        
        self.mpi     = mpi
        self.threads = threads
        self.pbs     = pbs

        if self.mpi:
            self.threads = 1
        
        self.max_malloc = max_malloc
        
        command = Program(expgram.expgram_quantize)
        if mpi:
            command = Program(expgram.expgram_quantize_mpi)
        
        command += Option('--ngram', Quoted(backward.ngram))
        command += Option('--output', Quoted(self.ngram))

        if temporary:
            command += Option('--temporary', Quoted(temporary))
        
        if mpi:
            command += Option('--prog', Quoted(expgram.expgram_quantize_mpi))

            if mpi.hosts:
                command += Option('--host', Quoted(mpi.hosts))
            elif mpi.hosts_file:
                command += Option('--hostfile', Quoted(mpi.hosts_file))

        if debug >= 2:
            command += Option('--debug', debug)
        else:
            command += Option('--debug')

        self.command = command

    def run(self):
        qsub = QSub(mpi=self.mpi, pbs=self.pbs)

        if self.mpi:
            qsub.mpirun(self.command, threads=self.threads, name="quantize", memory=self.max_malloc, logfile=self.log)
        else:
            qsub.run(self.command, threads=self.threads, name="quantize", memory=self.max_malloc, logfile=self.log)

if __name__ == '__main__':
    (options, args) = opt_parser.parse_args()

    ### dump to stderr
    stdout = sys.stdout
    sys.stdout = sys.stderr

    if not options.output:
        raise ValueError, "no output for ngram language model"

    if not options.counts and not options.corpus and not options.corpus_list and not options.counts_list:
        raise ValueError, "no corpus?"

    if options.counts and not os.path.exists(options.counts):
        raise ValueError, "no counts? %s" %(options.counts)
    if options.corpus and not os.path.exists(options.corpus):
        raise ValueError, "no corpus? %s" %(options.corpus)
    if options.corpus_list and not os.path.exists(options.corpus_list):
        raise ValueError, "no corpus list? %s" %(options.corpus_list)
    if options.counts_list and not os.path.exists(options.counts_list):
        raise ValueError, "no counts list? %s" %(options.counts_list)

    if options.counts:
        if options.corpus or options.corpus_list or options.counts_list:
            raise ValueError, "counts is supplied, but do we need to collect counts from corpus/corpus-list/counts-list?"

    if options.tokenizer and not os.path.exists(options.tokenizer):
        raise ValueError, "no tokenizer? %s" %(options.tokenizer)

    if not options.temporary_dir:
        if os.environ.has_key('TMPDIR_SPEC') and os.environ['TMPDIR_SPEC']:
            options.temporary_dir = os.environ['TMPDIR_SPEC']
    else:
        os.environ['TMPDIR_SPEC'] = options.temporary_dir

    check = 0
    if options.cutoff > 1:
        check += 1
    if options.kbest > 0:
        check += 1
    if options.vocab:
        check += 1

    if check > 1:
        raise ValueError, "count-cutoff and/or kbest-vocabulary and/or vocab-file are selected. Use only one of them"

    expgram = Expgram(options.expgram_dir)

    mpi = None
    if options.mpi_host or options.mpi_host_file or options.mpi > 0:
        mpi = MPI(dir=options.mpi_dir,
                  hosts=options.mpi_host,
                  hosts_file=options.mpi_host_file,
                  number=options.mpi)

    pbs = None
    if options.pbs:
        pbs = PBS(queue=options.pbs_queue)

    corpus = Corpus(corpus=options.corpus,
                    counts=options.counts,
                    corpus_list=options.corpus_list,
                    counts_list=options.counts_list)

    extract = None
    if os.path.exists(options.counts):
        extract = Counts(options.counts)
    else:
        vocab = None
        if options.cutoff > 1 or options.kbest > 0:
            vocab = Vocab(expgram=expgram,
                          corpus=corpus,
                          output=options.output,
                          tokenizer=options.tokenizer,
                          cutoff=options.cutoff,
                          kbest=options.kbest,
                          max_malloc=options.max_malloc,
                          threads=options.threads, mpi=mpi, pbs=pbs,
                          debug=options.debug)
            
            if options.first_step <= 1 and options.last_step >= 1:
                print "(1) compute vocabulary started  @", time.ctime()
                vocab.run()
                print "(1) compute vocabulary finished @", time.ctime()
                print "(1) vocabulary:", vocab.vocab
            
        elif options.vocab:
            vocab = VocabFile(vocab)

            if options.first_step <= 1 and options.last_step >= 1:
                print "(1) vocabulary:", vocab.vocab
        
        extract = Extract(expgram=expgram,
                          corpus=corpus,
                          output=options.output,
                          vocab=vocab,
                          order=options.order,
                          tokenizer=options.tokenizer,
                          max_malloc=options.max_malloc,
                          threads=options.threads, mpi=mpi, pbs=pbs,
                          debug=options.debug)

        if options.first_step <= 2 and options.last_step >= 2:
            print "(2) extract counts started  @", time.ctime()
            extract.run()
            print "(2) extract counts finished @", time.ctime()
            print "(2) extracted counts:", extract.ngram

    index = Index(expgram=expgram,
                  output=options.output,
                  temporary=options.temporary_dir,
                  extract=extract,
                  max_malloc=options.max_malloc,
                  threads=options.threads, mpi=mpi, pbs=pbs,
                  debug=options.debug)

    if options.first_step <= 3 and options.last_step >= 3:
        print "(3) index counts started  @", time.ctime()
        index.run()
        print "(3) index counts finished @", time.ctime()
        print "(3) indexed counts:", index.ngram

    modify = Modify(expgram=expgram,
                    output=options.output,
                    temporary=options.temporary_dir,
                    index=index,
                    max_malloc=options.max_malloc,
                    threads=options.threads, mpi=mpi, pbs=pbs,
                    debug=options.debug)

    if options.first_step <= 4 and options.last_step >= 4:
        print "(4) modify counts started  @", time.ctime()
        modify.run()
        print "(4) modify counts finished @", time.ctime()
        print "(4) modified counts:", modify.ngram
    
    estimate = Estimate(expgram=expgram,
                        output=options.output,
                        temporary=options.temporary_dir,
                        modify=modify,
                        remove_unk=options.remove_unk,
                        max_malloc=options.max_malloc,
                        threads=options.threads, mpi=mpi, pbs=pbs,
                        debug=options.debug)

    if options.first_step <= 5 and options.last_step >= 5:
        print "(5) estimate language model started  @", time.ctime()
        estimate.run()
        print "(5) estimate language model finished @", time.ctime()
        print "(5) language model:", estimate.ngram

    backward = Backward(expgram=expgram,
                        output=options.output,
                        temporary=options.temporary_dir,
                        estimate=estimate,
                        max_malloc=options.max_malloc,
                        threads=options.threads, mpi=mpi, pbs=pbs,
                        debug=options.debug)

    if options.first_step <= 6 and options.last_step >= 6:
        print "(6) backward trie language model started  @", time.ctime()
        backward.run()
        print "(6) backward trie language model finished @", time.ctime()
        print "(6) language model:", backward.ngram
    
    quantize = Quantize(expgram=expgram,
                        output=options.output,
                        temporary=options.temporary_dir,
                        backward=backward,
                        max_malloc=options.max_malloc,
                        threads=options.threads, mpi=mpi, pbs=pbs,
                        debug=options.debug)

    if options.first_step <= 7 and options.last_step >= 7:
        print "(7) quantization started  @", time.ctime()
        quantize.run()
        print "(7) quantization finished @", time.ctime()
        print "(7) quantized language model:", quantize.ngram
    
    if options.erase_temporary:
        shutil.rmtree(index.ngram)
        shutil.rmtree(estimate.ngram)
        shutil.rmtree(modify.ngram)
