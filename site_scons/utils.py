##
#  @file
#
#  Internal utilities for sconsUtils.
##

import sys
import warnings
import SCons.Script

##
#  @brief A dead-simple logger for all messages.
#
#  This simply centralizes decisions about whether to throw exceptions or print user-friendly messages
#  (the traceback variable) and whether to print extra debug info (the verbose variable).
#  These are set from command-line options in state.py.
##
class Log(object):

    def __init__(self, verbose, silent, traceback):
        self.traceback = traceback
        self.verbose = verbose
        self.silent = silent

    def debug(self, message):
        if self.verbose and not self.silent:
            print "DEBUG : " + message

    def info(self, message):
        if not self.silent:
            print "INFO : " + message

    def warn(self, message):
        if self.traceback:
            warnings.warn(message, stacklevel=2)
        else:
            sys.stderr.write("WARN : " + message + "\n")

    def fail(self, message):
        if self.traceback:
            raise RuntimeError(message)
        else:
            if message:
                sys.stderr.write("ERROR : " + message + "\n")
            SCons.Script.Exit(1)

    def flush(self):
        sys.stderr.flush()
