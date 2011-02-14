#!/usr/bin/env python

#
#  a wrapper script for expgram!
#

import threading
import multiprocessing

import time
import sys
import os, os.path
import string
import re
import subprocess

from optparse import OptionParser, make_option

opt_parser = OptionParser(
    option_list=[
        make_option("--corpus", default="", action="store", type="string",
                    help="corpus"),
        make_option("--corpus-list", default="", action="store", type="string",
                    help="list of corpus"),
        make_option("--order", default=5, action="store", type="int",
                    help="ngram order (default: 5)"),
        make_option("--output", default=5, action="store", type="string",
                    help="ngram output"),
        make_option("--cutoff", default=1, action="store", type="int",
                    help="count cutoff threshold (default: 1 == keep all the counts)")

        # expgram Expgram directory
        make_option("--expgram-dir", default="", action="store", type="string",
                    metavar="DIRECTORY", help="expgram directory"),
        # MPI Implementation.. if different from standard location...
        make_option("--mpi-dir", default="", action="store", type="string",
                    metavar="DIRECTORY", help="MPI directory"),
        
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
