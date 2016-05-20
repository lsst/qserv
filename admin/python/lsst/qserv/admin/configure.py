# LSST Data Management System
# Copyright 2015 AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.

"""
Configuration module for Qserv

Utilities for directory tree creation, templates management and command-line options

@author  Fabrice Jammes, IN2P3

"""

# --------------------------------
#  Imports of standard modules --
# -------------------------------
from distutils.util import strtobool
import getpass
import logging
import os
import random
import sys
import string

# ----------------------------
# Imports for other modules --
# ----------------------------
from lsst.qserv.admin import path
from lsst.qserv.admin import commons

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_LOG = logging.getLogger(__name__)

# -----------------------
# Exported definitions --
# -----------------------

# used in cmd-line tool
INIT = 'init'
DIRTREE = 'directory-tree'
ETC = 'etc'
CLIENT = 'client'

MYSQL = 'mysql'
CSS_WATCHER = 'css-watcher'
CZAR = 'qserv-czar'
WORKER = 'qserv-worker'
QSERV = 'qserv'
SCISQL =  'scisql'

DB_COMPONENTS = [MYSQL, CZAR, WORKER, SCISQL]
NODB_COMPONENTS = [CSS_WATCHER]
COMPONENTS = NODB_COMPONENTS + DB_COMPONENTS
CONFIGURATION_STEPS = [DIRTREE, ETC] + COMPONENTS + [CLIENT]

ALL_STEPS = [INIT] + CONFIGURATION_STEPS
ALL_STEPS_DOC = {
    INIT: "Remove previous QSERV_RUN_DIR if exists, then create QSERV_RUN_DIR pointing to both current Qserv binaries "
          "and QSERV_DATA_DIR (see QSERV_RUN_DIR/qserv-meta.conf for details)",
    DIRTREE: "Create directory tree in QSERV_RUN_DIR, "
             "eventually create symbolic link from QSERV_RUN_DIR/var/lib to QSERV_DATA_DIR.",
    ETC: "Create Qserv configuration files in QSERV_RUN_DIR using values issued " +
         "from meta-config file QSERV_RUN_DIR/qserv-meta.conf",
    MYSQL: "Remove MariaDB previous data, install db and set password",
    CSS_WATCHER: "Configure CSS-watcher (i.e. MySQL credentials)",
    CZAR: "Initialize Qserv master database",
    WORKER: "Initialize Qserv worker database",
    SCISQL: "Install and configure SciSQL",
    CLIENT: "Create client configuration file (used by integration tests for example)"
}

ALL_STEPS_SHORT = dict()
for step in ALL_STEPS:
    if step in COMPONENTS:
        if step == WORKER:
            ALL_STEPS_SHORT[step] = 'W'
        else:
            ALL_STEPS_SHORT[step] = step[0].upper()
    else:
        ALL_STEPS_SHORT[step] = step[0]

# list of files that should only be readable by this account
SECRET_FILES = ['qserv-wmgr.cnf', 'wmgr.secret']

def random_string(charset, size):
    """
    Generates a random string consisting of size charaters picked randomly
    from a given character set.
    """
    return ''.join(random.choice(charset) for _ in range(size))

def exists_and_is_writable(dir):
    """
    Test if a dir exists. If no creates it, if yes checks if it is writeable.
    Return True if a writeable directory exists at the end of function execution, else False
    """
    _LOG.debug("Checking existence and write access for: %r", dir)
    if not os.path.exists(dir):
        try:
            os.makedirs(dir)
        except OSError:
            logger.error("Unable to create dir: %r", dir)
            return False
    elif not path.is_writable(dir):
        return False
    return True


# TODO : put in a shell script
def update_root_dirs():

    config = commons.getConfig()

    for (section, option) in (('qserv', 'log_dir'), ('qserv', 'tmp_dir'),
                             ('qserv', 'qserv_data_dir')):
        dir = config[section][option]
        if not exists_and_is_writable(dir):
            _LOG.fatal("%r is not writable check/update permissions or"
                       " change config[%r][%r]", dir, section, option)
            sys.exit(1)

    for suffix in ('etc', 'var', 'var/run',
                   'var/run/mysqld', 'var/lock/subsys'):
        dir = os.path.join(config['qserv']['qserv_run_dir'], suffix)
        if not exists_and_is_writable(dir):
            _LOG.fatal("%r is not writable check/update permissions", dir)
            sys.exit(1)

    # user config
    user_config_dir = os.path.join(os.getenv("HOME"), ".lsst")
    if not exists_and_is_writable(user_config_dir):
        _LOG.fatal("%r is not writable check/update permissions", dir)
        sys.exit(1)
    _LOG.info("Qserv directory structure creation succeeded")

def update_root_symlinks():
    """ symlinks creation for directories externalised from qserv run dir
        i.e. QSERV_RUN_DIR/var/log will be symlinked to  config['qserv']['log_dir'] if needed
    """
    config = commons.getConfig()

    for (section, option, symlink_suffix) in (('qserv', 'log_dir', os.path.join("var", "log")),
                                              ('qserv', 'tmp_dir', 'tmp'),
                                              ('qserv', 'qserv_data_dir', os.path.join("var", "lib"))):
        symlink_target = config[section][option]
        default_dir = os.path.join(config['qserv']['qserv_run_dir'], symlink_suffix)

        # symlink if target directory is not set to its default value
        if symlink_target != default_dir:
            if os.path.exists(default_dir):
                if os.path.islink(default_dir):
                    os.unlink(default_dir)
                else:
                    log.fatal("Please remove {0} and restart the configuration procedure".format(default_dir))
                    sys.exit(1)
            _symlink(symlink_target, default_dir)

    _LOG.info("Qserv symlinks creation for externalized directories succeeded")

def _symlink(target, link_name):
    _LOG.debug("Creating symlink, target: %r, link name: %r", target, link_name)
    os.symlink(target, link_name)

template_params_dict = None
def _get_template_params():
    """ Compute templates parameters from Qserv meta-configuration file
        from PATH or from environment variables for products not needed during build
    """
    config = commons.getConfig()

    global template_params_dict

    if template_params_dict is None:

        if config['qserv']['node_type'] == 'mono':
            comment_mono_node = '#MONO-NODE# '
        else:
            comment_mono_node = ''

        testdata_dir = os.getenv('QSERV_TESTDATA_DIR', "NOT-AVAILABLE # please set environment variable QSERV_TESTDATA_DIR if needed")

        scisql_dir = os.environ.get('SCISQL_DIR')
        if scisql_dir is None:
            _LOG.fatal("sciSQL install : sciSQL is missing, please install it and set SCISQL_DIR environment variable.")
            sys.exit(1)

        # find python executable in $PATH
        python_bin = "NOT-AVAILABLE"
        path = os.environ.get('PATH', '')
        for p in path.split(os.pathsep):
            python = os.path.join(p, 'python')
            if os.access(p, os.X_OK):
                python_bin = python
                break

        params_dict = {
            'CMSD_MANAGER_PORT': config['xrootd']['cmsd_manager_port'],
            'COMMENT_MONO_NODE' : comment_mono_node,
            'HOME': os.path.expanduser("~"),
            'LD_LIBRARY_PATH': os.environ.get('LD_LIBRARY_PATH'),
            'LUA_DIR': os.path.join(config['lua']['base_dir']),
            'MYSQLD_DATA_DIR': os.path.join(config['qserv']['qserv_data_dir'], "mysql"),
            # used for mysql-proxy in mono-node
            'MYSQLD_HOST': '127.0.0.1',
            'MYSQLD_PASSWORD_MONITOR': config['mysqld']['password_monitor'],
            'MYSQLD_PASSWORD_ROOT': config['mysqld']['password_root'],
            'MYSQLD_PORT': config['mysqld']['port'],
            'MYSQLD_SOCK': config['mysqld']['sock'],
            'MYSQLD_USER_MONITOR': config['mysqld']['user_monitor'],
            'MYSQLD_USER_QSERV': config['mysqld']['user_qserv'],
            'MYSQL_DIR': config['mysqld']['base_dir'],
            'MYSQLPROXY_DIR': config['mysql_proxy']['base_dir'],
            'MYSQLPROXY_PORT': config['mysql_proxy']['port'],
            'NODE_TYPE': config['qserv']['node_type'],
            'PATH': os.environ.get('PATH'),
            'PYTHONPATH': os.environ['PYTHONPATH'],
            'PYTHON_BIN': python_bin,
            'QSERV_DATA_DIR': config['qserv']['qserv_data_dir'],
            'QSERV_DIR': config['qserv']['base_dir'],
            'QSERV_LOG_DIR': config['qserv']['log_dir'],
            'QSERV_MASTER': config['qserv']['master'],
            'QSERV_META_CONFIG_FILE': config['qserv']['meta_config_file'],
            'QSERV_PID_DIR': os.path.join(config['qserv']['qserv_run_dir'], "var", "run"),
            'QSERV_RUN_DIR': config['qserv']['qserv_run_dir'],
            'QSERV_UNIX_USER': getpass.getuser(),
            'QSERV_VERSION': config['qserv']['version'],
            'SCISQL_DIR': scisql_dir,
            'WMGR_PASSWORD': random_string(string.ascii_letters + string.digits + string.punctuation, 23),
            'WMGR_PORT': config['wmgr']['port'],
            'WMGR_SECRET_KEY': random_string(string.ascii_letters + string.digits, 57),
            'WMGR_USER_NAME': random_string(string.ascii_letters + string.digits, 12),
            'XROOTD_ADMIN_DIR': os.path.join(config['qserv']['qserv_run_dir'], 'tmp'),
            'XROOTD_DIR': config['xrootd']['base_dir'],
            'XROOTD_MANAGER_HOST': config['qserv']['master'],
            'XROOTD_PORT': config['xrootd']['xrootd_port'],
        }

        _LOG.debug("Template input parameters:\n {0}".format(params_dict))
        template_params_dict=params_dict
    else:
        params_dict=template_params_dict

    return params_dict

def _set_perms(file):
    (path, basename) = os.path.split(file)
    script_list = [c+".sh" for c in COMPONENTS]
    if (os.path.basename(path) == "bin" or
        os.path.basename(path) == "init.d" or
        basename in script_list):
        os.chmod(file, 0o760)
    elif basename in SECRET_FILES:
        os.chmod(file, 0o600)
    else:
        # all other files are configuration files
        os.chmod(file, 0o660)

def apply_tpl_once(src_file, target_file, params_dict = None):
    """ Creating one configuration file from one template
    """

    _LOG.debug("Creating {0} from {1}".format(target_file, src_file))

    if params_dict is None:
        params_dict = _get_template_params()

    with open(src_file, "r") as tpl:
        t = QservConfigTemplate(tpl.read())

    out_cfg = t.safe_substitute(**params_dict)
    for match in t.pattern.findall(t.template):
        name = match[1]
        if len(name) != 0 and name not in params_dict:
            _LOG.fatal("Template %r in file %r is not defined in configuration tool", name, src_file)
            sys.exit(1)

    dirname = os.path.dirname(target_file)
    if not os.path.exists(dirname):
        os.makedirs(os.path.dirname(target_file))
    with open(target_file, "w") as cfg:
        cfg.write(out_cfg)

def apply_tpl_all(template_root, dest_root):

    _LOG.info("Creating configuration from templates")
    if not os.path.isdir(template_root):
        _LOG.fatal("Template root directory: {0} doesn't exist.".format(template_root))
        sys.exit(1)

    for root, dirs, files in os.walk(template_root):
        os.path.normpath(template_root)
        suffix = root[len(template_root)+1:]
        dest_dir = os.path.join(dest_root, suffix)
        for fname in files:
            src_file = os.path.join(root, fname)
            target_file = os.path.join(dest_dir, fname)

            apply_tpl_once(src_file, target_file)

            # applying perms
            _set_perms(target_file)

    return True


def keep_data(components, qserv_data_dir):
    """
    If qserv_data_dir isn't empty then remove from components list
    those whose configuration impact data

    @param components: list of components to analyze
    @param qserv_data_dir: absolute path to directory containing data
    @return: list of components to configure
    """
    if os.listdir(qserv_data_dir):
        current_db_comp = intersect(components, DB_COMPONENTS)
        _LOG.warn("Remove configuration steps impacting data (%r) because of non-empty QSERV_DATA_DIR (%r)",
                  current_db_comp,
                  qserv_data_dir)
        components = [item for item in components if item not in current_db_comp]
    return components


def user_yes_no_query(question):
    sys.stdout.write('\n%r [y/n]\n' % question)
    while True:
        try:
            return strtobool(raw_input().lower())
        except ValueError:
            sys.stdout.write('Please respond with \'y\' or \'n\'.\n')


def intersect(seq1, seq2):
    """
    Performs intersection of two configuration steps lists
    @param seq1: first list of string
    @type: list
    @param seq2: second list of string
    @type: list
    @return: subset of seq1 which is contained in seq2 keeping original ordering of items
    """
    seq2 = set(seq2)
    return [item for item in seq1 if item in seq2]


def has_configuration_step(steps):
    """
    Check if steps contains at least one configuration step
    @param step_list: list of string
    @return: True if step_list contains a configuration step
    """
    return bool(intersect(steps, CONFIGURATION_STEPS))


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
