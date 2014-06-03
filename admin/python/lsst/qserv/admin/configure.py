import commons
from datetime import datetime
from distutils.util import strtobool
import logging
import os
import sys
import string
from twisted.python.procutils import which

from lsst.qserv.admin import path

COMPONENTS = ['mysql', 'xrootd', 'qserv-czar', 'scisql']
STEP_RUN_LIST = ['read-meta-cfg', 'mkdirs', 'etc'] + COMPONENTS + ['client']
STEP_LIST = ['prep'] + STEP_RUN_LIST

def exists_and_is_writable(dir) :
    """
    Test if a dir exists. If no creates it, if yes checks if it is writeable.
    Return True if a writeable directory exists at the end of function execution, else False
    """
    logger = logging.getLogger()
    logger.debug("Checking existence and write access for : %s", dir)
    if not os.path.exists(dir):
        try:
                os.makedirs(dir)
        except OSerror:
            logger.error("Unable to create dir : %s " % dir)
            return False
    elif not path.is_writable(dir):
        return False
    return True


# TODO : put in a shell script
def check_root_dirs():

    logger = logging.getLogger()

    check_success=True

    config = commons.getConfig()

    for (section,option) in (('qserv','base_dir'),('qserv','log_dir'),('qserv','tmp_dir'),('mysqld','data_dir')):
        dir = config[section][option]
        if not exists_and_is_writable(dir):
            logging.fatal(  ("%s is not writable check/update permissions or"
                            " change config['%s']['%s']") %
                            (dir,section,option)
                         )
            sys.exit(1)

    for suffix in ('etc', 'var', 'var/lib', 'var/run', 'var/run/mysqld', 'var/lock/subsys'):
        dir = os.path.join(config['qserv']['run_base_dir'],suffix)
        if not exists_and_is_writable(dir):
            logging.fatal("%s is not writable check/update permissions" % dir)
            sys.exit(1)

    # user config
    user_config_dir=os.path.join(os.getenv("HOME"),".lsst")
    if not exists_and_is_writable(user_config_dir):
            logging.fatal("%s is not writable check/update permissions" % dir)
            sys.exit(1)
    logger.info("Qserv directory structure creation succeeded")

def check_root_symlinks():
    """ symlinks creation for directories externalised from qserv run dir
        i.e. QSERV_RUN_DIR/var/log will be symlinked to  config['qserv']['log_dir'] if needed
    """
    log = logging.getLogger()
    config=commons.getConfig()

    for (section,option,symlink_suffix) in (
        ('qserv','log_dir','var/log'),
        ('qserv','tmp_dir','tmp'),
        ('mysqld','data_dir', 'var/lib/mysql')
        ):
        symlink_target = config[section][option]
        default_dir = os.path.join(config['qserv']['run_base_dir'],symlink_suffix)

        # A symlink is needed if the target directory is not set to its default value
        if  not os.path.samefile(symlink_target, os.path.realpath(default_dir)):
            if os.path.exists(default_dir) :
                if os.path.islink(default_dir):
                    os.unlink(default_dir)
                else:
                    log.fatal("Please remove {0} and restart the configuration procedure".format(default_dir))
                    sys.exit(1)
            _symlink(symlink_target, default_dir)

    log.info("Qserv symlinks creation for externalized directories succeeded")

def _symlink(target, link_name):
    logger = logging.getLogger()
    logger.debug("Creating symlink, target : %s, link name : %s " % (target,link_name))
    os.symlink(target, link_name)

def uninstall(target, source, env):
    logger = logging.getLogger()
    uninstall_paths = [
            os.path.join(config['qserv']['log_dir']),
            os.path.join(config['mysqld']['data_dir']),
            os.path.join(config['qserv']['scratch_dir']),
            client_config_dir
            ]
    for path in uninstall_paths:
        if not os.path.exists(path):
            logger.info("Not uninstalling %s because it doesn't exists." % path)
        else:
            shutil.rmtree(path)

def _get_template_params():
    """ Compute templates parameters from configuration file
    """
    logger=logging.getLogger()
    config=commons.getConfig()

    if config['qserv']['node_type']=='mono':
        comment_mono_node = '#MONO-NODE# '
    else:
        comment_mono_node = ''

    if 'testdata_dir' in config['qserv'].keys() :
        testdata_dir=config['qserv']['testdata_dir']
    else :
        testdata_dir=os.environ.get('QSERV_TESTDATA_DIR')

    params_dict = {
    'COMMENT_MONO_NODE' : comment_mono_node,
    'PATH': os.environ.get('PATH'),
    'LD_LIBRARY_PATH': os.environ.get('LD_LIBRARY_PATH'),
    'PYTHON_BIN': which("python"),
    'PYTHONPATH': os.environ['PYTHONPATH'],
    'QSERV_MASTER': config['qserv']['master'],
    'QSERV_DIR': config['qserv']['base_dir'],
    'QSERV_RUN_DIR': config['qserv']['run_base_dir'],
    'QSERV_UNIX_USER': os.getlogin(),
    'QSERV_LOG_DIR': config['qserv']['log_dir'],
    'QSERV_PID_DIR': os.path.join(config['qserv']['run_base_dir'],"var", "run"),
    'QSERV_TESTDATA_DIR': testdata_dir,
    'QSERV_RPC_PORT': config['qserv']['rpc_port'],
    'QSERV_USER': config['qserv']['user'],
    'QSERV_LUA_SHARE': os.path.join(config['lua']['base_dir'],"share","lua","5.1"),
    'QSERV_LUA_LIB': os.path.join(config['lua']['base_dir'],"lib","lua","5.1"),
    'QSERV_SCRATCH_DIR': config['qserv']['scratch_dir'],
    'MYSQL_DIR': config['mysqld']['base_dir'],
    'MYSQLD_DATA_DIR': config['mysqld']['data_dir'],
    'MYSQLD_PORT': config['mysqld']['port'],
    # used for mysql-proxy in mono-node
    'MYSQLD_HOST': '127.0.0.1',
    'MYSQLD_SOCK': config['mysqld']['sock'],
    'MYSQLD_USER': config['mysqld']['user'],
    'MYSQLD_PASS': config['mysqld']['pass'],
    'MYSQL_PROXY_PORT': config['mysql_proxy']['port'],
    'XROOTD_DIR': config['xrootd']['base_dir'],
    'XROOTD_MANAGER_HOST': config['qserv']['master'],
    'XROOTD_PORT': config['xrootd']['xrootd_port'],
    'XROOTD_RUN_DIR': os.path.join(config['qserv']['run_base_dir'],"xrootd-run"),
    'XROOTD_ADMIN_DIR': os.path.join(config['qserv']['run_base_dir'],'tmp'),
    'CMSD_MANAGER_PORT': config['xrootd']['cmsd_manager_port'],
    'ZOOKEEPER_PORT': config['zookeeper']['port'],
    'HOME': os.path.expanduser("~")
    }

    logger.debug("Input parameters :\n {0}".format(params_dict))

    return params_dict

def _set_perms(file):
    (path, basename) = os.path.split(file)
    script_list = [
        "xrootd.sh",
        "mysql.sh",
        "css.sh",
        "scisql.sh",
        "qserv-czar.sh"
        ]
    if (os.path.basename(path) == "bin" or
        os.path.basename(path) == "init.d" or
        basename in script_list):
        os.chmod(file, 0760)
    # all other files are configuration files
    else:
        os.chmod(file, 0660)

def _apply_tpl(src_file, target_file):
    """ Creating one configuration file from one template
    """

    logger = logging.getLogger()
    logger.debug("Creating {0} from {1}".format(target_file, src_file))
    params_dict = _get_template_params()

    with open(src_file, "r") as tpl:
        t = QservConfigTemplate(tpl.read())

    out_cfg = t.safe_substitute(**params_dict)
    for match in t.pattern.findall(t.template):
        name = match[1]
        if len(name) != 0 and not params_dict.has_key(name):
            logger.fatal("Template \"{0}\" in file {1}".format(name, src_file) + 
                " is not defined in configuration tool")
            sys.exit(1)

    dirname = os.path.dirname(target_file)
    if not os.path.exists(dirname):
        os.makedirs(os.path.dirname(target_file))
    with open(target_file, "w") as cfg:
        cfg.write(out_cfg)

def apply_templates(template_root, dest_root):

    logger = logging.getLogger()

    logger.info("Creating configuration using templates files")

    target_files=[]
    for root, dirs, files in os.walk(template_root):
        os.path.normpath(template_root)
        suffix=root[len(template_root)+1:]
        dest_dir=os.path.join(dest_root, suffix)
        for f in files:
            src_file = os.path.join(root,f)
            target_file = os.path.join(dest_dir, f)

            _apply_tpl(src_file, target_file)

            # applying perms
	    _set_perms(target_file)

    return True

def user_yes_no_query(question):
    sys.stdout.write('\n%s [y/n]\n' % question)
    while True:
        try:
            return strtobool(raw_input().lower())
        except ValueError:
            sys.stdout.write('Please respond with \'y\' or \'n\'.\n')

class QservConfigTemplate(string.Template):
    delimiter = '{{'
    pattern = r'''
    \{\{(?:
    (?P<escaped>\{\{)|
    (?P<named>[_a-z][_a-z0-9]*)\}\}|
    (?P<braced>[_a-z][_a-z0-9]*)\}\}|
    (?P<invalid>)
    )
    '''


