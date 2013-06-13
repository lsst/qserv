import os
import logging
from datetime import datetime
import sys

from SCons.Script import Delete
from urllib2 import Request, urlopen, URLError, HTTPError

import utils
import commons

def download(target, source, env):

    logger = logging.getLogger()

    logger.debug("Target %s :" % target[0])
    logger.debug("Source %s :" % source[0])

    url_str = str(source[0])
    url = Request(url_str)
    file_name = str(target[0])

    file_size_dl = -2
    file_size = -1

    success = True

    try:

        logger.debug("Opening %s :" % url_str)
        u = urlopen(url_str)
        f = open(file_name, 'wb')
        meta = u.info()
        file_size = int(meta.getheaders("Content-Length")[0])
        logger.info("Downloading: %s Bytes: %s" % (file_name, file_size))

        file_size_dl = 0
        block_sz = 64 * 256
        while True:
            buffer = u.read(block_sz)
            if not buffer:
                break

            file_size_dl += len(buffer)
            f.write(buffer)
            status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
            status = status + chr(8)*(len(status)+1)
            sys.stdout.write(status)
            sys.stdout.flush()
            sys.stdout.write('\r')
            sys.stdout.flush()

        f.close()

    #handle errors
    except HTTPError, e:
        logger.fatal("HTTP Error: %s %s" % (e, url_str))
        success = False
    except URLError, e:
        logger.fatal("URL Error: %s %s " % (e, url_str))
        success = False

    if file_size_dl != file_size:
        logger.fatal("Download of file %s failed" % url_str)
        success = False

    if not success:
        env.Execute(Delete(target[0]))
        sys.exit(1)

# TODO : add and test fail on error
#    if not os.path.isfile(file_name):
#        logger.fatal("Retrieval failed for file : %s " % file_name)
#        sys.exit(1)

def build_cmd_with_opts( config, target='install'):

    logger = logging.getLogger('scons-qserv')

    install_opts="--install-dir=\"%s\"" % config['qserv']['base_dir']
    install_opts="%s --log-dir=\"%s\"" % (install_opts, config['qserv']['log_dir'])
    install_opts="%s --mysql-data-dir=\"%s\"" % (install_opts, config['mysqld']['data_dir'])
    install_opts="%s --mysql-port=%s" % (install_opts, config['mysqld']['port'])
    install_opts="%s --mysql-pass=\"%s\"" % (install_opts,config['mysqld']['pass'])

    if config.has_key('geometry_src_dir') :
        if commons.is_readable(configi['qserv']['geometry_src_dir']) :
            install_opts=("%s --geometry-dir=\"%s\"" %
                            (install_opts,config['qserv']['geometry_src_dir'])
                    )
        else :
            logger.fatal("Error while accessing geometry src dir : '%s' for reading." % config['qserv']['geometry_src_dir'])
            exit(1)

    log_file_prefix = config['qserv']['log_dir']
    if target=='qserv-only' :
        install_opts="%s --qserv" % install_opts
        log_file_prefix += "/QSERV-ONLY"
    elif target == 'clean-all' :
        install_opts="%s --clean-all" % install_opts
        log_file_prefix = "~/QSERV-CLEAN"
    elif target == 'init-mysql-db' :
        install_opts="%s --init-mysql-db" % install_opts
        log_file_prefix += "/QSERV-INIT-MYSQL-DB"
    else :
        log_file_prefix += "/INSTALL"
    log_file_name = log_file_prefix + "-" + datetime.now().strftime("%Y-%m-%d-%H:%M:%S") + ".log"

    command_str = os.path.join(config['src_dir' ], "admin", "qserv-install")
    command_str += " "+install_opts + " &> " + log_file_name

    #logger.debug("Launching perl install script with next command : %s" % command_str)
    return command_str

def check_root_dirs(target, source, env):

    logger = logging.getLogger()

    check_success=True

    config=env['config']

    for (section,option) in (('qserv','base_dir'),('qserv','log_dir'),('qserv','tmp_dir'),('mysqld','data_dir')):
        dir = config[section][option]
        if not utils.exists_and_is_writable(dir):
       	    logging.fatal(  "%s is not writable check/update permissions or" %
                            "change config[%s]['%s']" %
                            (dir,section,option)
                         )
            sys.exit(1)

    for suffix in ('etc', 'build', 'var', 'var/lib', 'var/run', 'var/run/mysqld'):
        dir = os.path.join(config['qserv']['base_dir'],suffix)
        if not utils.exists_and_is_writable(dir):
       	    logging.fatal("%s is not writable check/update permissions" % dir)
            sys.exit(1)

    # user config
    # user_config_dir=os.path.join(os.getenv("HOME"),".lsst") 
    # if not utils.exists_and_is_writable(user_config_dir) 
    #   	    logging.fatal("%s is not writable check/update permissions" % dir)
    #        sys.exit(1)

    if not commons.is_readable(config['lsst']['data_dir']):
    	logging.warning("LSST data directory (config['lsst']['data_dir']) is not readable : %s" %
                       config['lsst']['data_dir']
                     )

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

def create_uninstall_target(env, path, is_glob):
    logger = logging.getLogger()
    if not os.path.exists(path):
        logger.info("Not uninstalling %s because it doesn't exists." % path)
    else:    
        if is_glob:
            all_files = Glob(path,strings=True)
            for filei in all_files:
                env.Command( "uninstall-"+filei, filei,
                [
                Delete("$SOURCE"),
                ])
                env.Alias("uninstall", "uninstall-"+filei)   
        else:
            env.Command( "uninstall-"+path, "",
            [
            Delete(path),
            ])
            env.Alias("uninstall", "uninstall-"+path)
