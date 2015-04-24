#!/usr/bin/env python

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
Configuration script for Qserv

Can configure mono/master/worker instance.

Create a Qserv run directory which contains:
 - a meta-configuration file containing Qserv main parameters
   and information about the binaries used for the run
 - configuration/startup files for each services
 - log files
 - pid files
 - data file for zookeeper, MySQL and Qserv

A Qserv run directory can only run one Qserv instance at a time.

@author  Fabrice Jammes, IN2P3

"""

# --------------------------------
#  Imports of standard modules --
# -------------------------------
import argparse
import ConfigParser
import logging
import os
import shutil
from subprocess import check_output
import sys

# ----------------------------
# Imports for other modules --
# ----------------------------
from lsst.qserv.admin import configure, commons
import lsst.qserv.admin.logger

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_LOG = logging.getLogger()

# -----------------------
# Exported definitions --
# -----------------------
class Configurator(object):
    """
    Application class for configuration application
    """

    def __init__(self):
        """
        Constructor parse all arguments and prepares for execution.
        """

        qserv_version = check_output(["qserv-version.sh"])
        qserv_version = qserv_version.strip(' \t\n\r')
        default_qserv_run_dir = os.path.join(
            os.path.expanduser("~"), "qserv-run", qserv_version)

        parser = argparse.ArgumentParser(
            description="Qserv services configuration tool.",
            epilog="DESCRIPTION:\n"
                   "  - Create an execution directory (QSERV_RUN_DIR) which contains configuration and execution\n"
                   "    data for a given Qserv instance.\n"
                   "  - Create  a data directory (QSERV_DATA_DIR) which contains MySQL/Qserv/Zookeeper data.\n"
                   "  - Use templates and meta-config file parameters (see QSERV_RUN_DIR/qserv-meta.conf) to generate\n"
                   "    Qserv configuration files and databases.\n"
                   "  - Default behaviour will configure a mono-node instance in " + default_qserv_run_dir + ".\n\n"
                   "CAUTION:\n"
                   "  - --all must be used for a setup from scratch.\n"
                   "  - Consistency of binaries/configuration/data not garanteed when not starting from scratch.\n\n",
            formatter_class=argparse.RawDescriptionHelpFormatter
        )

        parser.add_argument('-v', '--verbose', dest='verbose', default=[],
                            action='append_const',
                            const=None,
                            help='More verbose output, can use several times.')
        parser = lsst.qserv.admin.logger.add_logfile_opt(parser)

        parser.add_argument('-a', '--all', dest='all', action='store_true',
                            default=False,
                            help='Run initialization and configuration'
                            )
        # Defining option of each configuration step
        init_group = parser.add_argument_group('Initialization', 'Creation of QSERV_RUN_DIR and QSERV_DATA_DIR')
        config_group = parser.add_argument_group('Configuration steps', 'General configuration steps')
        nodb_component_group = parser.add_argument_group('Components configuration', 'Configuration of external components')
        db_component_group = parser.add_argument_group('Database components configuration', 'Configuration of external components '
                                                       'impacting data,\nlaunched if and only if QSERV_DATA_DIR is empty')
        for step_name in configure.ALL_STEPS:
            if step_name in configure.INIT:
                group = init_group
            elif step_name in configure.DB_COMPONENTS:
                group = db_component_group
            elif step_name in configure.NODB_COMPONENTS:
                group = nodb_component_group
            else:
                group = config_group
            group.add_argument(
                "-{0}".format(configure.ALL_STEPS_SHORT[step_name]),
                "--{0}".format(step_name),
                dest="step_list",
                action='append_const',
                const=step_name,
                help=configure.ALL_STEPS_DOC[step_name]
            )

        # forcing options which may ask user confirmation
        parser.add_argument("-f", "--force", dest="force", action='store_true',
                            default=False,
                            help="Answer yes to all questions, use with care. Default: %(default)s"
                            )

        # run dir, all mutable data related to a qserv running instance are
        # located here
        parser.add_argument("-R", "--qserv-run-dir", dest="qserv_run_dir",
                            default=default_qserv_run_dir,
                            help="Absolute path to qserv_run_dir. Default: %(default)s"
                            )

        # run dir, all mutable data related to a qserv running instance are
        # located here
        init_group.add_argument("-D", "--qserv-data-dir", dest="qserv_data_dir",
                            default=None,
                            help="Absolute path to directory containing Qserv data, default to QSERV_RUN_DIR/var/lib. "
                                 "IMPORTANT: Set QSERV_DATA_DIR outside of QSERV_RUN_DIR to protect data when configuring Qserv."
                            )

        # meta-configuration file whose parameters will be dispatched in Qserv
        # services configuration files
        self.args = parser.parse_args()
        default_meta_config_file = os.path.join(
            self.args.qserv_run_dir, "qserv-meta.conf")
        parser.add_argument("-m", "--metaconfig", dest="meta_config_file",
                            default=default_meta_config_file,
                            help="full path to Qserv meta-configuration file"
                            )

        self.args = parser.parse_args()

        verbosity = len(self.args.verbose)
        if verbosity != 0:
            levels = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
            simple_format = "[%(levelname)s] %(name)s: %(message)s"
            level = levels.get(verbosity, logging.DEBUG)
            logging.basicConfig(format=simple_format, level=level)
        else:
            # if -v(vv) option isn't used then
            # switch to global configuration file for logging
            lsst.qserv.admin.logger.setup_logging(self.args.log_conf)

        if self.args.all:
            self.args.step_list = configure.ALL_STEPS
        elif self.args.step_list is None:
            self.args.step_list = configure.CONFIGURATION_STEPS


        qserv_dir = os.path.abspath(
            os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "..")
        )
        self._in_config_dir = os.path.join(qserv_dir, "cfg")
        self._template_root = os.path.join(self._in_config_dir, "templates")

        if self.args.qserv_data_dir:
            self._qserv_data_dir = self.args.qserv_data_dir
        else:
            self._qserv_data_dir = os.path.join(self.args.qserv_run_dir, "var", "lib")

    def _template_to_symlink(self, filename, symlink):
        """
        Generate qserv_run_dir/etc/filename from
        qserv template and symlink it
        @param filename:
        @param symlink:
        @return:
        """

        template_file = os.path.join(
            self._template_root, "etc", filename
        )
        cfg_file = os.path.join(
            self.args.qserv_run_dir, "etc", filename
        )
        configure.apply_tpl_once(
            template_file,
            cfg_file
        )
        _LOG.debug(
            "Client configuration file created: {0}".format(cfg_file)
        )

        if os.path.exists(symlink):
            try:
                is_symlink_correct = os.path.samefile(symlink, cfg_file)
            except os.error:
                # link is broken
                is_symlink_correct = False

            if not is_symlink_correct:
                if self.args.force or configure.user_yes_no_query(
                        "Do you want to update symbolic link {0} to {1}?".format(os.path.realpath(symlink),
                                                                                 cfg_file)):
                    os.remove(symlink)
                    os.symlink(cfg_file, symlink)
                else:
                    _LOG.fatal("Symbolic link to client " +
                               "configuration unmodified. Exiting.")
                    sys.exit(1)
        else:
            try:
                os.remove(symlink)
                _LOG.debug("Removing broken symbolic link : {0}".format(symlink))
            except os.error:
                pass
            os.symlink(cfg_file, symlink)

        _LOG.info(
            "{0} points to: {1}".format(symlink, cfg_file)
        )

    def _template_to_client_config(self, product):
        """
        Create configuration files for a product from qserv_run_dir
        templates files, and symlink it to client configuration
        (for example ~/.lsst/ directory for Qserv product)
        """
        homedir = os.path.expanduser("~")
        if product == configure.QSERV:
            # might need to create directory first
            try:
                os.makedirs(os.path.join(homedir, ".lsst"))
                _LOG.debug(
                    "Creating client configuration directory : ~/.lsst")
            except os.error:
                pass
            self._template_to_symlink("qserv-client.conf", os.path.join(homedir, ".lsst", "qserv.conf"))
            self._template_to_symlink("logging.ini", os.path.join(homedir, ".lsst", "logging.ini"))
        elif product == configure.MYSQL:
            self._template_to_symlink("my-client.cnf",
                                      os.path.join(homedir, ".my.cnf"))
        else:
            _LOG.fatal("Unable to apply configuration template " +
                       "for product %s", product)
            sys.exit(1)

    def run(self):
        """
        Do actual configuration based on parameters provided on command-line-interface and qserv-meta.conf file
        This will throw exception if anything goes wrong.
        """
        _LOG.info("Qserv configuration tool\n" +
                  "======================================================================="
                  )

        if commons.status(self.args.qserv_run_dir) not in [commons.NO_STATUS_SCRIPT, commons.DOWN]:
            _LOG.fatal(
                "Qserv services are still running "
                "for this Qserv run directory (%s),"
                " stop it before running this script.", self.args.qserv_run_dir)
            sys.exit(1)

        if configure.INIT in self.args.step_list:

            if os.path.exists(self.args.qserv_run_dir):

                if self.args.force or configure.user_yes_no_query(
                                "WARNING : Do you want to erase all configuration" +
                                " data in {0} ?".format(self.args.qserv_run_dir)
                ):
                    shutil.rmtree(self.args.qserv_run_dir)
                else:
                    _LOG.fatal(
                        "Stop Qserv configuration, specify an other configuration directory")
                    sys.exit(1)

            in_meta_config_file = os.path.join(self._in_config_dir, "qserv-meta.conf")
            _LOG.info("Create meta-configuration file: {0}"
                      .format(self.args.meta_config_file)
                      )
            params_dict = {
                'QSERV_RUN_DIR': self.args.qserv_run_dir,
                'QSERV_DATA_DIR': self._qserv_data_dir
            }
            _LOG.info("Store data in: {0}"
                      .format(self._qserv_data_dir)
                      )
            configure.apply_tpl_once(
                in_meta_config_file, self.args.meta_config_file, params_dict)

        ###################################
        #
        # Running configuration procedure
        #
        ###################################
        if configure.has_configuration_step(self.args.step_list):

            try:
                _LOG.info(
                    "Read meta-configuration file {0}".format(self.args.meta_config_file))
                config = commons.read_config(self.args.meta_config_file)

                # used in templates targets comments
                config['qserv']['meta_config_file'] = self.args.meta_config_file

            except ConfigParser.NoOptionError, exc:
                _LOG.fatal("Miss option in meta-configuration file: %s", exc)
                sys.exit(1)

            if configure.DIRTREE in self.args.step_list:
                _LOG.info("Define main directory structure")
                configure.check_root_dirs()
                configure.check_root_symlinks()

            ##########################################
            #
            # Creating Qserv services configuration
            # using templates and meta_config_file
            #
            ##########################################
            qserv_run_dir = config['qserv']['qserv_run_dir']
            qserv_data_dir = config['qserv']['qserv_data_dir']

            if configure.ETC in self.args.step_list:
                _LOG.info(
                    "Create configuration files in {0}".format(os.path.join(qserv_run_dir, "etc")) +
                    " and scripts in {0}".format(os.path.join(qserv_run_dir, "tmp"))
                )

                # TODO: see DM-2580
                # in_template_config_dir = os.path.join(self._in_config_dir, "templates")
                # out_template_config_dir = os.path.join(self.args.qserv_run_dir, "templates")
                # _LOG.info("Copying template configuration from {0} to {1}".format(in_template_config_dir,
                #                                                                   self.args.qserv_run_dir)
                #          )
                # shutil.copytree(in_template_config_dir, out_template_config_dir)

                dest_root = os.path.join(qserv_run_dir)
                configure.apply_tpl_all(
                    self._template_root,
                    dest_root
                )

            component_cfg_steps = configure.intersect(
                self.args.step_list, configure.COMPONENTS)
            if len(component_cfg_steps) > 0:
                _LOG.info("Run configuration scripts")
                configuration_scripts_dir = os.path.join(
                    qserv_run_dir, 'tmp', 'configure'
                )

                if config['qserv']['node_type'] in ['master']:
                    _LOG.info(
                        "Master instance: not configuring " +
                        "%s and %s",
                        configure.SCISQL,
                        configure.WORKER
                    )
                    component_cfg_steps.remove(configure.SCISQL)
                    component_cfg_steps.remove(configure.WORKER)
                elif config['qserv']['node_type'] in ['worker']:
                    _LOG.info(
                        "Worker instance: not configuring " +
                        "{0}".format(configure.CZAR)
                    )
                    component_cfg_steps.remove(configure.CZAR)

                component_cfg_steps = configure.keep_data(component_cfg_steps, qserv_data_dir)

                for comp in component_cfg_steps:
                    cfg_script = os.path.join(
                        configuration_scripts_dir, comp + ".sh")
                    if os.path.isfile(cfg_script):
                        commons.run_command([cfg_script])

            if configure.CSS_WATCHER in self.args.step_list:
                self._template_to_client_config(configure.MYSQL)

            if configure.CLIENT in self.args.step_list:
                self._template_to_client_config(configure.QSERV)

if __name__ == '__main__':
    try:
        configurator = Configurator()
        sys.exit(configurator.run())
    except Exception as exc:
        _LOG.critical('Exception occured: %s', exc, exc_info=True)
        sys.exit(1)

