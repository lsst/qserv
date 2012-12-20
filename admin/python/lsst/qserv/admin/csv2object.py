#!/usr/bin/env python

# Author: Emmanuel Medernach
# Date: 14 Dec 2012

# Usage: csv2object.py <nbworkers> <data directory> ...

import sys
import glob
import os
import os.path
import csv


def ObjectFiles(dirlist):
  result = []
  for dir in dirlist:
    for subdir in glob.glob(dir + os.sep + "stripe_*"):
      if os.path.isdir(subdir):
        for file in  glob.glob(subdir + os.sep + "Object_*"):
          if os.path.isfile(file):
            result.append(file)
  return result                    

def ExtractFields(filename):
  outfile = "/tmp/" + os.path.basename(filename) + ".tmp"
  with open(outfile, 'w') as out:
    with open(filename) as csvfile:
      reader = csv.reader(csvfile)
      for row in reader:
        out.write(row[0] + ",  " + row[-2] + ",  " + row[-1] + "\n")
  return outfile


args = sys.argv
nbworkers = int(args[1])
files = ObjectFiles(args[2:])

try:
  import multiprocessing

  poolbuilder = multiprocessing.Pool
  pool = poolbuilder(nbworkers)
  for filename in pool.map(ExtractFields, files):
    print open(filename).read()
    # os.remove(filename)

except ImportError:
  for objectfile in files:
    ExtractFields(objectfile)
