#!/usr/bin/env python

"""
CSS Watcher - service responsible for synchronizing database schema with CSS.

This script will be a long-running daemon which watches the state of CSS
database makes sure that state of the local mysql server correctly reflects
the state of CSS by creating/deleting databases and tables.
"""

from __future__ import absolute_import, division, print_function

# --------------------------------
#  Imports of standard modules --
# --------------------------------
from argparse import ArgumentParser
import configparser
import logging

# -----------------------------
# Imports for other modules --
# -----------------------------
import lsst.log
from lsst.qserv.admin import watcherLib

# ----------------------------------
# Local non-exported definitions --
# ----------------------------------

# ------------------------
# Exported definitions --
# ------------------------


def main():

    descr = 'Synchronize database contents with CSS.'
    parser = ArgumentParser(description=descr)
    parser.add_argument('-v', '--verbose', dest='verbose', action='count', default=0,
                        help='More verbose output, can use several times.')
    parser.add_argument('-c', '--config', dest='configFile', default='./watcher.cfg', metavar='PATH',
                        help='Read configuration from provided file, def: %(default)s.')
    args = parser.parse_args()

    # configure logging
    levels = {0: lsst.log.WARN, 1: lsst.log.INFO, 2: lsst.log.DEBUG}
    level = levels.get(args.verbose, lsst.log.DEBUG)
    lsst.log.setLevel('', level)

    # redirect Python logging to LSST logger
    pylgr = logging.getLogger()
    pylgr.setLevel(logging.DEBUG)
    pylgr.addHandler(lsst.log.LogHandler())
    logging.captureWarnings(True)

    # parameters
    interval = 3.0             # scan interval in seconds
    wmgrSecret = None
    extraCzar = None

    # parse config file
    defaults = dict(interval=str(interval))
    cfg = configparser.SafeConfigParser(defaults)
    cfg.readfp(open(args.configFile))

    # get few parameters
    if cfg.has_section('watcher'):
        interval = cfg.getfloat('watcher', 'interval')
    wmgrSecret = cfg.get('wmgr', 'secret')
    if cfg.has_option('czar_wmgr', 'host'):
        host = cfg.get('czar_wmgr', 'host')
        port = 5012
        if cfg.has_option('czar_wmgr', 'port'):
            port = cfg.getint('czar_wmgr', 'port')
        extraCzar = (host, port)

    # instantiate CSS
    cssConfig = dict(cfg.items('css'))
    wcss = watcherLib.WatcherCss(cssConfig, wmgrSecret, extraCzar)

    # instantiate executor
    qmetaConfig = dict(cfg.items('qmeta'))
    executor = watcherLib.QservExecutor(wcss, qmetaConfig)

    # start watcher, this will not return
    watcher = watcherLib.Watcher(wcss, executor, interval)
    watcher.run()


if __name__ == "__main__":
    main()
