#!/usr/bin/env python

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
        make_option("--corpus", default="", action="store", type="string",
                    help="corpus"),
        make_option("--corpus-list", default="", action="store", type="string",
                    help="list of corpus"),
        make_option("--order", default=5, action="store", type="int",
                    help="ngram order (default: 5)"),
        make_option("--output", default="", action="store", type="string",
                    help="ngram output"),
        make_option("--cutoff", default=1, action="store", type="int",
                    help="count cutoff threshold (default: 1 == keep all the counts)"),
        make_option("--tokenizer", default="", action="store",
                    help="tokenizer applied to data"),
        make_option("--remove-unk", default=None, action="store_true",
                    help="remove unk from lm estimation"),

        make_option("--erase-temporary", default=None, action="store_true",
                    help="erase temporary allocated disk space"),
        
        # expgram Expgram directory
        make_option("--expgram-dir", default="", action="store", type="string",
                    metavar="DIRECTORY", help="expgram directory"),
        # MPI Implementation.. if different from standard location...
        make_option("--mpi-dir", default="", action="store", type="string",
                    metavar="DIRECTORY", help="MPI directory"),

        ## max-malloc
        make_option("--max-malloc", default=8, action="store", type="float",
                    metavar="MALLOC", help="maximum memory in GB (default: 8)"),
        
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
                    help="PBS queue for launching processes (default: ltg)", metavar="NAME"),

        ## debug messages
        make_option("--debug", default=0, action="store", type="int"),
        ])


def run_command(command, logfile=None):
    fp = None
    if logfile:
        fp = os.popen(command + '>& ' +logfile)
    else:
        fp = os.popen(command)
    while 1:
        data = fp.read(1)
        if not data: break
        stdout.write(data)

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

### dump to stderr
stdout = sys.stdout
sys.stdout = sys.stderr

class PBS:
    def __init__(self, queue="", workingdir=os.getcwd()):
        self.queue = queue
        self.workingdir = workingdir
        self.tmpdir = None
        self.tmpdir_spec = None

        if os.environ.has_key('TMPDIR'):
            self.tmpdir = os.environ['TMPDIR']

        if os.environ.has_key('TMPDIR_SPEC'):
            self.tmpdir_spec = os.environ['TMPDIR_SPEC']
            
    def run(self, command="", threads=1, memory=0.0, name="name", mpi=None, logfile=None):
        popen = subprocess.Popen("qsub -S /bin/sh", shell=True, stdin=subprocess.PIPE)

        pipe = popen.stdin
        
        pipe.write("#!/bin/sh\n")
        pipe.write("#PBS -N %s\n" %(name))
        pipe.write("#PBS -W block=true\n")
        
        if logfile:
            pipe.write("#PBS -e %s\n" %(logfile))
        else:
            pipe.write("#PBS -e /dev/null\n")
        pipe.write("#PBS -o /dev/null\n")
        
        if self.queue:
            pipe.write("#PBS -q %s\n" %(self.queue))

        if mpi:
            if memory > 0.0:
                if memory < 1.0:
                    pipe.write("#PBS -l select=%d:ncpus=2:mpiprocs=1:mem=%dmb\n" %(mpi.number, int(memory * 1000)))
                else:
                    pipe.write("#PBS -l select=%d:ncpus=2:mpiprocs=1:mem=%dgb\n" %(mpi.number, int(memory)))
            else:
                pipe.write("#PBS -l select=%d:ncpus=2:mpiprocs=1\n" %(mpi.number))
            pipe.write("#PBS -l place=scatter\n")
                
        else:
            if memory > 0.0:
                if memory < 1.0:
                    pipe.write("#PBS -l select=1:ncpus=%d:mpiprocs=1:mem=%dmb\n" %(threads, int(memory * 1000)))
                else:
                    pipe.write("#PBS -l select=1:ncpus=%d:mpiprocs=1:mem=%dgb\n" %(threads, int(memory)))
            else:
                pipe.write("#PBS -l select=1:ncpus=%d:mpiprocs=1\n" %(threads))
        
        # setup TMPDIR and TMPDIR_SPEC
        if self.tmpdir:
            pipe.write("export TMPDIR=%s\n" %(self.tmpdir))
        if self.tmpdir_spec:
            pipe.write("export TMPDIR_SPEC=%s\n" %(self.tmpdir_spec))
            
        pipe.write("cd \"%s\"\n" %(self.workingdir))

        if mpi:
            pipe.write("%s %s\n" %(mpi.mpirun, command))
        else:
            pipe.write("%s\n" %(command))
        
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
                prog = os.path.join(self.bindir, binprog)
                if not os.path.exists(prog):
                    prog = os.path.join(self.bindir, 'bin', binprog)
                    if not os.path.exists(prog):
                        raise ValueError, prog + " does not exist at " + self.bindir
                    
                setattr(self, binprog, prog)
            else:
                setattr(self, binprog, binprog)
                
    def run(self, command, logfile=None):
        mpirun = self.mpirun
        #if self.dir:
        #    mpirun += ' --prefix %s' %(self.dir)
        if self.number > 0:
            mpirun += ' --np %d' %(self.number)
        if self.hosts:
            mpirun += ' --host %s' %(self.hosts)
        elif self.hosts_file:
            mpirun += ' --hostfile %s' %(self.hosts_file)
	mpirun += ' ' + command

	run_command(mpirun, logfile=logfile)

class Expgram:
    def __init__(self, dir=""):

	self.dir = dir	
	if not dir: return
	
	if not os.path.exists(self.dir):
	    raise ValueError, self.dir + " does not exist"
	
	self.dir = os.path.realpath(self.dir)
        
	self.bindirs = [self.dir]
	for dir in ('bin', 'progs', 'scripts'): 
	    bindir = os.path.join(self.dir, dir)
	    if os.path.exists(bindir) and os.path.isdir(bindir):
		self.bindirs.append(bindir)

	if not self.bindirs:
	    raise ValueError, str(self.bindirs) + "  does not exist"
	
        for binprog in (## vocabulary
                        'ngram_vocab', 'ngram_vocab_mpi',
                        
                        ## counts...
                        'ngram_counts_extract', 'ngram_counts_extract_mpi',
                        'ngram_counts_index', 'ngram_counts_index_mpi',
                        'ngram_counts_modify', 'ngram_counts_modify_mpi',
                        'ngram_counts_estimate', 'ngram_counts_estimate_mpi',
                        
                        ## final post-processing
                        'ngram_bound', 'ngram_bound_mpi',
                        'ngram_quantize', 'ngram_quantize_mpi',):
	    
	    for bindir in self.bindirs:
		prog = os.path.join(bindir, binprog)
		if os.path.exists(prog):
		    setattr(self, binprog, prog)
		    break
	    if not hasattr(self, binprog):
		raise ValueError, binprog + ' does not exist'

class Corpus:
    def __init__(self, corpus="", corpus_list=""):
        self.corpus      = corpus
        self.corpus_list = corpus_list
        

class Vocab:
    def __init__(self, expgram=None, corpus=None, output="",
                 tokenizer="", cutoff=2, max_malloc=4,
                 threads=4, mpi=None, pbs=None, debug=None):
        
        self.vocab = output + '.vocab.' + str(cutoff)
        self.counts = output + '.vocab'
        self.log = self.counts + '.log'
        
        self.cutoff = cutoff
        
        self.mpi = mpi
        self.threads = threads
        self.pbs = pbs
        
        self.max_malloc = max_malloc

        command = ""
        if mpi:
            command = "%s" %(expgram.ngram_vocab_mpi)
        else:
            command = "%s" %(expgram.ngram_vocab)
        
        if os.path.exists(corpus.corpus):
            command += " --corpus \"%s\"" %(corpus.corpus)
        if os.path.exists(corpus.corpus_list):
            command += " --corpus-list \"%s\"" %(corpus.corpus_list)
        else:
            command += " --map-line"

        command += " --output \"%s\"" %(self.counts)
        
        if tokenizer:
            command += " --filter \"%s\"" %(tokenizer)

        if mpi:
            command += " --prog %s" %(expgram.ngram_vocab_mpi)
        else:
            command += " --threads %d" %(threads)

        if debug >= 2:
            command += " --debug %d" %(debug)
        else:
            command += " --debug"

        self.command = command
        
    def run(self):
        
        if self.mpi:
            if self.pbs:
                pbs.run(command=self.command, threads=self.threads, mpi=self.mpi, name="vocab", memory=self.max_malloc, logfile=self.log)
            else:
                self.mpi.run(self.command, logfile=self.log)
        else:
            if self.pbs:
                pbs.run(command=self.command, threads=self.threads, name="vocab", memory=self.max_malloc, logfile=self.log)
            else:
                run_command(self.command, logfile=self.log)
        
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
                 tokenizer="", max_malloc=4,
                 threads=4, mpi=None, pbs=None, debug=None):
        
        self.ngram = output + '.counts'
        self.log = self.ngram + '.log'
        
        self.mpi = mpi
        self.threads = threads
        self.pbs = pbs
        
        self.max_malloc = max_malloc
        
        
        command = ""
        if mpi:
            command = "%s" %(expgram.ngram_counts_extract_mpi)
        else:
            command = "%s" %(expgram.ngram_counts_extract)
        
        if os.path.exists(corpus.corpus):
            command += " --corpus \"%s\"" %(corpus.corpus)
        if os.path.exists(corpus.corpus_list):
            command += " --corpus-list \"%s\"" %(corpus.corpus_list)
        else:
            command += " --map-line"
        
        command += " --output \"%s\"" %(self.ngram)
        
        command += " --order %d" %(order)
        
        if vocab:
            command += " --vocab \"%s\"" %(vocab.vocab)
        
        if tokenizer:
            command += " --filter \"%s\"" %(tokenizer)
        
        command += " --max-malloc %g" %(max_malloc)

        if mpi:
            command += " --prog %s" %(expgram.ngram_counts_extract_mpi)
        else:
            command += " --threads %d" %(threads)

        if debug >= 2:
            command += " --debug %d" %(debug)
        else:
            command += " --debug"

        self.command = command

    def run(self):
        if self.mpi:
            if self.pbs:
                pbs.run(command=self.command, threads=self.threads, mpi=self.mpi, name="extract", memory=self.max_malloc, logfile=self.log)
            else:
                self.mpi.run(self.command, logfile=self.log)
                
        else:
            if self.pbs:
                pbs.run(command=self.command, threads=self.threads, name="extract", memory=self.max_malloc, logfile=self.log)
            else:
                run_command(self.command, logfile=self.log)

class Index:

    def __init__(self, expgram=None, output="",
                 extract=None,
                 max_malloc=4,
                 threads=4, mpi=None, pbs=None, debug=None):
        
        self.ngram = output + '.index'
        self.log = self.ngram + '.log'
        
        self.mpi = mpi
        self.threads = threads
        self.pbs = pbs
        
        self.max_malloc = max_malloc

        
        command = ""
        if mpi:
            command = "%s" %(expgram.ngram_counts_index_mpi)
        else:
            command = "%s" %(expgram.ngram_counts_index)

        command += " --ngram \"%s\"" %(extract.ngram)
        command += " --output \"%s\"" %(self.ngram)
        
        if mpi:
            command += " --prog %s" %(expgram.ngram_counts_index_mpi)
        else:
            command += " --shard %d" %(threads)

        if debug >= 2:
            command += " --debug %d" %(debug)
        else:
            command += " --debug"

        self.command = command

    def run(self):
        if self.mpi:
            if self.pbs:
                pbs.run(command=self.command, threads=self.threads, mpi=self.mpi, name="index", memory=self.max_malloc, logfile=self.log)
            else:
                self.mpi.run(self.command, logfile=self.log)
        else:
            if self.pbs:
                pbs.run(command=self.command, threads=self.threads, name="index", memory=self.max_malloc, logfile=self.log)
            else:
                run_command(self.command, logfile=self.log)

class Modify:

    def __init__(self, expgram=None, output="",
                 index=None,
                 max_malloc=4,
                 threads=4, mpi=None, pbs=None, debug=None):
        
        self.ngram = output + '.modified'
        self.log = self.ngram + '.log'
        
        self.mpi = mpi
        self.threads = threads
        self.pbs = pbs
        
        self.max_malloc = max_malloc

        
        command = ""
        if mpi:
            command = "%s" %(expgram.ngram_counts_modify_mpi)
        else:
            command = "%s" %(expgram.ngram_counts_modify)

        command += " --ngram \"%s\"" %(index.ngram)
        command += " --output \"%s\"" %(self.ngram)
        
        
        if mpi:
            command += " --prog %s" %(expgram.ngram_counts_modify_mpi)

        if debug >= 2:
            command += " --debug %d" %(debug)
        else:
            command += " --debug"

        self.command = command

    def run(self):
        if self.mpi:
            if self.pbs:
                pbs.run(command=self.command, threads=self.threads, mpi=self.mpi, name="modify", memory=self.max_malloc, logfile=self.log)
            else:
                self.mpi.run(self.command, logfile=self.log)
        else:
            if self.pbs:
                pbs.run(command=self.command, threads=self.threads, name="modify", memory=self.max_malloc, logfile=self.log)
            else:
                run_command(self.command, logfile=self.log)

class Estimate:

    def __init__(self, expgram=None, output="",
                 modify=None, remove_unk=None,
                 max_malloc=4,
                 threads=4, mpi=None, pbs=None, debug=None):
        
        self.ngram = output + '.lm'
        self.log = self.ngram + '.log'
        
        self.mpi = mpi
        self.threads = threads
        self.pbs = pbs
        
        self.max_malloc = max_malloc
        
        command = ""
        if mpi:
            command = "%s" %(expgram.ngram_counts_estimate_mpi)
        else:
            command = "%s" %(expgram.ngram_counts_estimate)

        command += " --ngram \"%s\"" %(modify.ngram)
        command += " --output \"%s\"" %(self.ngram)

        if remove_unk:
            command += " --remove-unk"
        
        if mpi:
            command += " --prog %s" %(expgram.ngram_counts_estimate_mpi)

        if debug >= 2:
            command += " --debug %d" %(debug)
        else:
            command += " --debug"

        self.command = command

    def run(self):
        if self.mpi:
            if self.pbs:
                pbs.run(command=self.command, threads=self.threads, mpi=self.mpi, name="estimate", memory=self.max_malloc, logfile=self.log)
            else:
                self.mpi.run(self.command, logfile=self.log)
        else:
            if self.pbs:
                pbs.run(command=self.command, threads=self.threads, name="estimate", memory=self.max_malloc, logfile=self.log)
            else:
                run_command(self.command, logfile=self.log)

class Bound:

    def __init__(self, expgram=None, output="",
                 estimate=None,
                 max_malloc=4,
                 threads=4, mpi=None, pbs=None, debug=None):
        
        self.ngram = output + '.lm.final'
        self.log = self.ngram + '.log'
        
        self.mpi = mpi
        self.threads = threads
        self.pbs = pbs
        
        self.max_malloc = max_malloc
        
        command = ""
        if mpi:
            command = "%s" %(expgram.ngram_bound_mpi)
        else:
            command = "%s" %(expgram.ngram_bound)

        command += " --ngram \"%s\"" %(estimate.ngram)
        command += " --output \"%s\"" %(self.ngram)
        
        if mpi:
            command += " --prog %s" %(expgram.ngram_bound_mpi)

        if debug >= 2:
            command += " --debug %d" %(debug)
        else:
            command += " --debug"

        self.command = command

    def run(self):
        if self.mpi:
            if self.pbs:
                pbs.run(command=self.command, threads=self.threads, mpi=self.mpi, name="bound", memory=self.max_malloc, logfile=self.log)
            else:
                self.mpi.run(self.command, logfile=self.log)
        else:
            if self.pbs:
                pbs.run(command=self.command, threads=self.threads, name="bound", memory=self.max_malloc, logfile=self.log)
            else:
                run_command(self.command, logfile=self.log)

class Quantize:

    def __init__(self, expgram=None, output="",
                 bound=None,
                 max_malloc=4,
                 threads=4, mpi=None, pbs=None, debug=None):
        
        self.ngram = output + '.lm.quantize'
        self.log = self.ngram + '.log'
        
        self.mpi = mpi
        self.threads = threads
        self.pbs = pbs
        
        self.max_malloc = max_malloc
        
        command = ""
        if mpi:
            command = "%s" %(expgram.ngram_quantize_mpi)
        else:
            command = "%s" %(expgram.ngram_quantize)

        command += " --ngram \"%s\"" %(bound.ngram)
        command += " --output \"%s\"" %(self.ngram)
        
        if mpi:
            command += " --prog %s" %(expgram.ngram_quantize_mpi)

        if debug >= 2:
            command += " --debug %d" %(debug)
        else:
            command += " --debug"

        self.command = command

    def run(self):
        if self.mpi:
            if self.pbs:
                pbs.run(command=self.command, threads=self.threads, mpi=self.mpi, name="quantize", memory=self.max_malloc, logfile=self.log)
            else:
                self.mpi.run(self.command, logfile=self.log)
        else:
            if self.pbs:
                pbs.run(command=self.command, threads=self.threads, name="quantize", memory=self.max_malloc, logfile=self.log)
            else:
                run_command(self.command, logfile=self.log)

(options, args) = opt_parser.parse_args()

if not options.output:
    raise ValueError, "no output for ngram language model"

if not options.counts and not options.corpus and not options.corpus_list:
    raise ValueError, "no corpus?"

if options.counts and not os.path.exists(options.counts):
    raise ValueError, "no counts? %s" %(options.counts)
if options.corpus and not os.path.exists(options.corpus):
    raise ValueError, "no corpus? %s" %(options.corpus)
if options.corpus_list and not os.path.exists(options.corpus_list):
    raise ValueError, "no corpus list? %s" %(options.corpus_list)

if options.tokenizer and not os.path.exists(options.tokenizer):
    raise ValueError, "no tokenizer? %s" %(options.tokenizer)

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
                corpus_list=options.corpus_list)

extract = None

if os.path.exists(options.counts):
    extract = Counts(options.counts)
else:
    vocab = None
    if options.cutoff > 1:
        
        vocab = Vocab(expgram=expgram,
                      corpus=corpus,
                      output=options.output,
                      tokenizer=options.tokenizer,
                      cutoff=options.cutoff,
                      max_malloc=options.max_malloc,
                      threads=options.threads, mpi=mpi, pbs=pbs,
                      debug=options.debug)
        
        print "compute vocabulary started  @", time.ctime()
        vocab.run()
        print "compute vocabulary finished @", time.ctime()

    extract = Extract(expgram=expgram,
                      corpus=corpus,
                      output=options.output,
                      vocab=vocab,
                      order=options.order,
                      tokenizer=options.tokenizer,
                      max_malloc=options.max_malloc,
                      threads=options.threads, mpi=mpi, pbs=pbs,
                      debug=options.debug)

    print "extract counts started  @", time.ctime()
    extract.run()
    print "extract counts finished @", time.ctime()
    

index = Index(expgram=expgram,
              output=options.output,
              extract=extract,
              max_malloc=options.max_malloc,
              threads=options.threads, mpi=mpi, pbs=pbs,
              debug=options.debug)

print "index counts started  @", time.ctime()
index.run()
print "index counts finished @", time.ctime()

modify = Modify(expgram=expgram,
                output=options.output,
                index=index,
                max_malloc=options.max_malloc,
                threads=options.threads, mpi=mpi, pbs=pbs,
                debug=options.debug)

print "modify counts started  @", time.ctime()
modify.run()
print "modify counts finished @", time.ctime()

estimate = Estimate(expgram=expgram,
                    output=options.output,
                    modify=modify,
                    remove_unk=options.remove_unk,
                    max_malloc=options.max_malloc,
                    threads=options.threads, mpi=mpi, pbs=pbs,
                    debug=options.debug)

print "estimate language model started  @", time.ctime()
estimate.run()
print "estimate language model finished @", time.ctime()

bound = Bound(expgram=expgram,
              output=options.output,
              estimate=estimate,
              max_malloc=options.max_malloc,
              threads=options.threads, mpi=mpi, pbs=pbs,
              debug=options.debug)
print "estimate upper bound started  @", time.ctime()
bound.run()
print "estimate upper bound finished @", time.ctime()

print "estimated language model:", bound.ngram

quantize = Quantize(expgram=expgram,
                    output=options.output,
                    bound=bound,
                    max_malloc=options.max_malloc,
                    threads=options.threads, mpi=mpi, pbs=pbs,
                    debug=options.debug)

print "quantization started  @", time.ctime()
quantize.run()
print "quantization finished @", time.ctime()
print "quantized language model:", quantize.ngram

if options.erase_temporary:
    shutil.rmtree(index.ngram)
    shutil.rmtree(modify.ngram)
    shutil.rmtree(estimate.ngram)
