#!/usr/bin/env python


# LSST Data Management System
# Copyright 2014 AURA/LSST.
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
User-friendly configuration script for Qserv

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
#--------------------------------
import argparse
import ConfigParser
import logging
import os
import shutil
from subprocess import check_output
import sys

#-----------------------------
# Imports for other modules --
#-----------------------------
from lsst.qserv.admin import configure, commons
import lsst.qserv.admin.logger

#----------------------------------
# Local non-exported definitions --
#----------------------------------
_LOG = logging.getLogger()

#------------------------
# Exported definitions --
#------------------------
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
            description='''Qserv configuration tool. Creates an execution
            directory (qserv_run_dir) which will contains configuration and execution
            data for a given Qserv instance. Deploys values from meta-config file $qserv_run_dir/qserv-meta.conf
            in all Qserv configuration files and databases. Default behaviour will configure a mono-node
            instance in ''' + default_qserv_run_dir + '''. IMPORTANT : --all MUST BE USED
            FOR A SETUP FROM SCRATCH.''',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )

        parser.add_argument('-v', '--verbose', dest='verbose', default=[],
                            action='append_const',
                            const=None,
                            help='More verbose output, can use several times.')
        parser = lsst.qserv.admin.logger.add_logfile_opt(parser)

        parser.add_argument("-a", "--all", dest="all", action='store_true',
                            default=False,
                            help='''clean execution directory and then run all configuration steps'''
                            )
        # Defining option of each configuration step
        for step_name in configure.STEP_LIST:
            parser.add_argument(
                "-{0}".format(configure.STEP_ABBR[step_name]),
                "--{0}".format(step_name),
                dest="step_list",
                action='append_const',
                const=step_name,
                help=configure.STEP_DOC[step_name]
            )

        # forcing options which may ask user confirmation
        parser.add_argument("-f", "--force", dest="force", action='store_true',
                            default=False,
                            help="forcing removal of existing execution data"
                            )

        # run dir, all mutable data related to a qserv running instance are
        # located here
        parser.add_argument("-R", "--qserv-run-dir", dest="qserv_run_dir",
                            default=default_qserv_run_dir,
                            help="full path to qserv_run_dir"
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
            self.args.step_list = configure.STEP_LIST
        elif self.args.step_list is None:
            self.args.step_list = configure.STEP_RUN_LIST


        qserv_dir = os.path.abspath(
            os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "..")
        )
        self._in_config_dir = os.path.join(qserv_dir, "cfg")
        self._template_root = os.path.join(self._in_config_dir, "templates")

    @staticmethod
    def _intersect(seq1, seq2):
        ''' returns subset of seq1 which is contained in seq2 keeping original ordering of items '''
        seq2 = set(seq2)
        return [item for item in seq1 if item in seq2]

    @staticmethod
    def _contains_configuration_step(step_list):
        return bool(Configurator._intersect(step_list, configure.STEP_RUN_LIST))

    def _template_to_symlink(self, filename, symlink):
        """
        Generate qserv_run_dir/etc/filename from
        qserv template and symlink it
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

        if configure.PREPARE in self.args.step_list:

            if os.path.exists(self.args.qserv_run_dir):

                if self.args.force or configure.user_yes_no_query(
                                "WARNING : Do you want to erase all configuration" +
                                " data in {0} ?".format(self.args.qserv_run_dir)
                ):
                    shutil.rmtree(self.args.qserv_run_dir)
                else:
                    _LOG.info(
                        "Stopping Qserv configuration, please specify an other configuration directory")
                    sys.exit(1)

            in_meta_config_file = os.path.join(self._in_config_dir, "qserv-meta.conf")
            _LOG.info("Creating meta-configuration file: {0}"
                      .format(self.args.meta_config_file)
                      )
            params_dict = {
                'RUN_BASE_DIR': self.args.qserv_run_dir
            }
            configure.apply_tpl_once(
                in_meta_config_file, self.args.meta_config_file, params_dict)

        ###################################
        #
        # Running configuration procedure
        #
        ###################################
        if Configurator._contains_configuration_step(self.args.step_list):

            try:
                _LOG.info(
                    "Reading meta-configuration file {0}".format(self.args.meta_config_file))
                config = commons.read_config(self.args.meta_config_file)

                # used in templates targets comments
                config['qserv']['meta_config_file'] = self.args.meta_config_file

            except ConfigParser.NoOptionError, exc:
                _LOG.fatal("Missing option in meta-configuration file: %s", exc)
                sys.exit(1)

            if configure.DIRTREE in self.args.step_list:
                _LOG.info("Defining main directory structure")
                configure.check_root_dirs()
                configure.check_root_symlinks()

            ##########################################
            #
            # Creating Qserv services configuration
            # using templates and meta_config_file
            #
            ##########################################
            run_base_dir = config['qserv']['run_base_dir']
            if configure.ETC in self.args.step_list:
                _LOG.info(
                    "Creating configuration files in {0}".format(os.path.join(run_base_dir, "etc")) +
                    " and scripts in {0}".format(os.path.join(run_base_dir, "tmp"))
                )

                # TODO: see DM-2580
                # in_template_config_dir = os.path.join(self._in_config_dir, "templates")
                # out_template_config_dir = os.path.join(self.args.qserv_run_dir, "templates")
                # _LOG.info("Copying template configuration from {0} to {1}".format(in_template_config_dir,
                #                                                                   self.args.qserv_run_dir)
                #          )
                # shutil.copytree(in_template_config_dir, out_template_config_dir)

                dest_root = os.path.join(run_base_dir)
                configure.apply_tpl_all(
                    self._template_root,
                    dest_root
                )

            components_to_configure = Configurator._intersect(
                self.args.step_list, configure.COMPONENTS)
            if len(components_to_configure) > 0:
                _LOG.info("Running configuration scripts")
                configuration_scripts_dir = os.path.join(
                    run_base_dir, 'tmp', 'configure'
                )

                if config['qserv']['node_type'] in ['master']:
                    _LOG.info(
                        "Master instance : not configuring " +
                        "%s and %s",
                        configure.SCISQL,
                        configure.WORKER
                    )
                    components_to_configure.remove(configure.SCISQL)
                    components_to_configure.remove(configure.WORKER)
                elif config['qserv']['node_type'] in ['worker']:
                    _LOG.info(
                        "Worker instance : not configuring " +
                        "{0}".format(configure.CZAR)
                    )
                    components_to_configure.remove(configure.CZAR)

                for comp in components_to_configure:
                    cfg_script = os.path.join(
                        configuration_scripts_dir, comp + ".sh")
                    if os.path.isfile(cfg_script):
                        commons.run_command([cfg_script])

            if configure.CSS in self.args.step_list:
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

