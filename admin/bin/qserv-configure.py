#!/usr/bin/env python

import argparse
import ConfigParser
import fileinput
from lsst.qserv.admin import configure, commons
import logging
import os
import shutil
from subprocess import check_output
import sys


def parseArgs():

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
FOR A  SETUP FROM SCRATCH.''',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

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

    # Logging management
    verbose_dict = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'FATAL': logging.FATAL,
    }
    verbose_arg_values = verbose_dict.keys()
    parser.add_argument("-v", "--verbose-level", dest="verbose_str",
                        choices=verbose_arg_values,
                        default='INFO',
                        help="verbosity level"
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
    args = parser.parse_args()
    default_meta_config_file = os.path.join(
        args.qserv_run_dir, "qserv-meta.conf")
    parser.add_argument("-m", "--metaconfig", dest="meta_config_file",
                        default=default_meta_config_file,
                        help="full path to Qserv meta-configuration file"
                        )

    args = parser.parse_args()

    if args.all:
        args.step_list = configure.STEP_LIST
    elif args.step_list is None:
        args.step_list = configure.STEP_RUN_LIST

    args.verbose_level = verbose_dict[args.verbose_str]

    return args


def main():

    args = parseArgs()

    logging.basicConfig(
        format='%(levelname)s: %(message)s', level=args.verbose_level)

    logging.info("Qserv configuration tool\n" +
                 "======================================================================="
                 )

    qserv_dir = os.path.abspath(
        os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "..")
    )

    if configure.PREPARE in args.step_list:

        if os.path.exists(args.qserv_run_dir):
            if args.force or configure.user_yes_no_query(
                "WARNING : Do you want to erase all configuration" +
                " data in {0} ?".format(args.qserv_run_dir)
            ):
                shutil.rmtree(args.qserv_run_dir)
            else:
                logging.info(
                    "Stopping Qserv configuration, please specify an other configuration directory")
                sys.exit(1)

        in_config_dir = os.path.join(qserv_dir, "cfg")
        in_template_config_dir = os.path.join(in_config_dir, "templates")
        out_template_config_dir = os.path.join(args.qserv_run_dir, "templates")
        logging.info("Copying template configuration from {0} to {1}"
                     .format(in_template_config_dir, args.qserv_run_dir)
                     )
        shutil.copytree(in_template_config_dir, out_template_config_dir)

        in_meta_config_file = os.path.join(in_config_dir, "qserv-meta.conf")
        logging.info("Creating meta-configuration file: {0}"
                     .format(args.meta_config_file)
                     )
        params_dict = {
            'RUN_BASE_DIR': args.qserv_run_dir
        }
        configure.apply_tpl(
            in_meta_config_file, args.meta_config_file, params_dict)

    def intersect(seq1, seq2):
        ''' returns subset of seq1 which is contained in seq2 keeping original ordering of items '''
        seq2 = set(seq2)
        return [item for item in seq1 if item in seq2]

    def contains_configuration_step(step_list):
        return bool(intersect(step_list, configure.STEP_RUN_LIST))

    ###################################
    #
    # Running configuration procedure
    #
    ###################################
    if contains_configuration_step(args.step_list):

        try:
            logging.info(
                "Reading meta-configuration file {0}".format(args.meta_config_file))
            config = commons.read_config(args.meta_config_file)

            # used in templates targets comments
            config['qserv']['meta_config_file'] = args.meta_config_file

        except ConfigParser.NoOptionError, exc:
            logging.fatal("Missing option in meta-configuration file: %s", exc)
            sys.exit(1)

        if configure.DIRTREE in args.step_list:
            logging.info("Defining main directory structure")
            configure.check_root_dirs()
            configure.check_root_symlinks()

        ##########################################
        #
        # Creating Qserv services configuration
        # using templates and meta_config_file
        #
        ##########################################
        run_base_dir = config['qserv']['run_base_dir']
        if configure.ETC in args.step_list:
            logging.info(
                "Creating configuration files in {0}".format(os.path.join(run_base_dir, "etc")) +
                " and scripts in {0}".format(os.path.join(run_base_dir, "tmp"))
            )
            template_root = os.path.join(run_base_dir, "templates")
            dest_root = os.path.join(run_base_dir)
            configure.apply_templates(
                template_root,
                dest_root
            )

        components_to_configure = intersect(
            args.step_list, configure.COMPONENTS)
        if len(components_to_configure) > 0:
            logging.info("Running configuration scripts")
            configuration_scripts_dir = os.path.join(
                run_base_dir, 'tmp', 'configure'
            )

            if config['qserv']['node_type'] in ['master']:
                logging.info(
                    "Master instance : not configuring " +
                    "%s and %s",
                    configure.SCISQL,
                    configure.WORKER
                )
                components_to_configure.remove(configure.SCISQL)
                components_to_configure.remove(configure.WORKER)
            elif config['qserv']['node_type'] in ['worker']:
                logging.info(
                    "Worker instance : not configuring " +
                    "{0}".format(configure.CZAR)
                )
                components_to_configure.remove(configure.CZAR)

            for comp in components_to_configure:
                cfg_script = os.path.join(
                    configuration_scripts_dir, comp + ".sh")
                if os.path.isfile(cfg_script):
                    commons.run_command([cfg_script])

            def template_to_client_config(product):
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
                        logging.debug(
                            "Creating client configuration directory : ~/.lsst")
                    except os.error:
                        pass
                    _template_to_symlink("qserv-client.conf",
                                         os.path.join(homedir,
                                                      ".lsst",
                                                      "qserv.conf"))
                    _template_to_symlink("logging.yaml",os.path.join(homedir,
                                                      ".lsst",
                                                      "logging.yaml"))
                elif product == configure.MYSQL:
                    _template_to_symlink("my-client.cnf",
                                         os.path.join(homedir, ".my.cnf"))
                else:
                    logging.fatal("Unable to apply configuration template " +
                                  "for product %s", product)
                    sys.exit(1)

            def _template_to_symlink(filename, symlink):
                """
                Generate qserv_run_dir/etc/filename from
                qserv_run_dir/templates/etc/filename and symlink it
                """
                template_file = os.path.join(
                    run_base_dir, "templates", "etc", filename
                )
                cfg_file = os.path.join(
                    run_base_dir, "etc", filename
                )
                configure.apply_tpl(
                    template_file,
                    cfg_file
                )
                logging.debug(
                    "Client configuration file created: {0}".format(cfg_file)
                )

                if os.path.exists(symlink):
                    try:
                        is_symlink_correct = os.path.samefile(symlink, cfg_file)
                    except os.error:
                        # link is broken
                        is_symlink_correct = False

                    if not is_symlink_correct:
                        if args.force or configure.user_yes_no_query(
                            "Do you want to update symbolic link {0} to {1}?"
                            .format(os.path.realpath(symlink),cfg_file)):
                            os.remove(symlink)
                            os.symlink(cfg_file, symlink)
                        else:
                            logging.fatal("Symbolic link to client " +
                                         "configuration unmodified. Exiting.")
                            sys.exit(1)
                else:
                    try:
                        os.remove(symlink)
                        logging.debug("Removing broken symbolic link : {0}".format(symlink))
                    except os.error:
                        pass
                    os.symlink(cfg_file, symlink)

                logging.info(
                    "{0} points to: {1}".format(symlink, cfg_file)
                )


        if configure.CSS in args.step_list:
                template_to_client_config(configure.MYSQL)

        if configure.CLIENT in args.step_list:
            template_to_client_config(configure.QSERV)

if __name__ == '__main__':
    main()
