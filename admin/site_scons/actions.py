import os
import logging
from datetime import datetime
import sys

from SCons.Script import Delete
from urllib2 import Request, urlopen, URLError, HTTPError

import fileutils
import commons

# TODO : put in a shell script
def check_root_dirs(target, source, env):

    logger = logging.getLogger()

    check_success=True

    config=env['config']

    for (section,option) in (('qserv','base_dir'),('qserv','log_dir'),('qserv','tmp_dir'),('mysqld','data_dir')):
        dir = config[section][option]
        if not fileutils.exists_and_is_writable(dir):
       	    logging.fatal(  ("%s is not writable check/update permissions or"
                            " change config['%s']['%s']") %
                            (dir,section,option)
                         )
            sys.exit(1)

    for suffix in ('etc', 'build', 'var', 'var/lib', 'var/run', 'var/run/mysqld', 'var/lock/subsys'):
        dir = os.path.join(config['qserv']['base_dir'],suffix)
        if not fileutils.exists_and_is_writable(dir):
       	    logging.fatal("%s is not writable check/update permissions" % dir)
            sys.exit(1)

    # user config
    user_config_dir=os.path.join(os.getenv("HOME"),".lsst")
    if not fileutils.exists_and_is_writable(user_config_dir):
       	    logging.fatal("%s is not writable check/update permissions" % dir)
            sys.exit(1)

    logger.info("Qserv directory structure creation succeeded")

def check_root_symlinks(target, source, env):
    """ symlinks creation for directories externalised of qserv directory tree
    """
    log = logging.getLogger()
    config=env['config']

    check_success=True

    for (section,option,symlink_suffix) in (
        ('qserv','log_dir','var/log'),
        ('qserv','tmp_dir','tmp'),
        ('mysqld','data_dir', 'var/lib/mysql')
        ):
        symlink_target = config[section][option]
        symlink_name = os.path.join(config['qserv']['base_dir'],symlink_suffix)

        # A symlink is needed if :
        #   - the target directory is not in qserv base dir
        #   - it doesn't already exists
        if  not os.path.samefile(symlink_target, os.path.realpath(symlink_name)):
            # cleaning if needed, management of build configuration file update
            # log.debug("TARGET %s, REALNAME %s " % (symlink_target, os.path.realpath(symlink_name)))
            if os.path.exists(symlink_name) :
                if os.path.islink(symlink_name):
                    os.unlink(symlink_name)
                else:
                    env.Execute(Delete(symlink_name))
            symlink_with_log(symlink_target, symlink_name)
                    #env.Execute(env.Command(symlink_name, symlink_target, actions.symlink))
                    #init_target_lst.append(symlink_name)

    if check_success :
        log.info("Qserv symlinks creation for externalized directories succeeded")
    else:
        sys.exit(1)

def symlink(target, source, env):
    symlink_with_log(os.path.abspath(str(source[0])), os.path.abspath(str(target[0])))

def symlink_with_log(target, link_name):
    logger = logging.getLogger()
    logger.debug("Creating symlink, target : %s, link name : %s " % (target,link_name))
    os.symlink(target, link_name)

def uninstall(target, source, env):
    logger = logging.getLogger()
    for path in env['uninstallpaths']:
        if not os.path.exists(path):
            logger.info("Not uninstalling %s because it doesn't exists." % path)
        else:
            env.Execute(Delete(path))

