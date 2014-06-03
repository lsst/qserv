#!/usr/bin/env python

import argparse
import ConfigParser
import fileinput
from lsst.qserv.admin import configure, commons, logger
import logging
import os
import shutil
from subprocess import check_output
import sys

def parseArgs():
    parser = argparse.ArgumentParser(
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
            )

    #
    step_option_values = configure.STEP_LIST
    components = ['mysql', 'xrootd', 'qserv-czar', 'scisql']
    configure.STEP_RUN_LIST = ['read-meta-cfg', 'mkdirs', 'etc'] + components + ['client']
    step_list = ['prep'] + configure.STEP_RUN_LIST
    step_option_values = step_list
    parser.add_argument("-s", "--step", dest="step", choices=step_option_values,
        help="Qserv configurator install step : \n'" +
        "     " + step_option_values[0] + " : create a qserv_run_dir " +
        "directory which will contains configuration and execution data " +
        "for a given Qserv instance\n" +
        "     " + step_option_values[1] + " : fill qserv_run_dir " +
        "configuration files with values from meta-config file qserv.conf"
        )
    
    parser.add_argument("-a", "--all", dest="all", action='store_true',
            default=False,
            help="Clean execution directory and then run all configuration steps"
            )

    # Logging management
    verbose_dict = {
        'DEBUG'     : logging.DEBUG,
        'INFO'      : logging.INFO,
        'WARNING'   : logging.WARNING,
        'ERROR'     : logging.ERROR,
        'FATAL'   : logging.FATAL,
    }
    verbose_arg_values = verbose_dict.keys() 
    parser.add_argument("-v", "--verbose-level", dest="verbose_str", choices=verbose_arg_values,
        default='INFO',
        help="Verbosity level"
        )

    # forcing options which may ask user confirmation
    parser.add_argument("-f", "--force", dest="force", action='store_true',
            default=False,
            help="Forcing removal of existing execution data"
            )

    # run dir, all mutable data related to a qserv running instance are
    # located here
    qserv_version=check_output(["qserv-version.sh"])
    qserv_version = qserv_version.strip(' \t\n\r')
    default_qserv_run_dir=os.path.join(os.path.expanduser("~"),"qserv-run",qserv_version)
    parser.add_argument("-r", "--qserv-run-dir", dest="qserv_run_dir",
            default=default_qserv_run_dir,
            help="Full path to qserv_run_dir"
            )

    # meta-configuration file whose parameters will be dispatched in Qserv
    # services configuration files
    args = parser.parse_args()
    default_meta_config_file = os.path.join(args.qserv_run_dir, "qserv.conf")
    parser.add_argument("-c", "--metaconfig",  dest="meta_config_file",
            default=default_meta_config_file,
            help="Full path to Qserv meta-configuration file"
            )

    args = parser.parse_args()

    if args.step is not None and len(args.step) != 0 :
        args.step_list = [args.step]
    elif args.all:
        args.step_list = configure.STEP_LIST
    else:
        args.step_list = configure.STEP_RUN_LIST

    args.verbose_level = verbose_dict[args.verbose_str]

    return args

def copy_and_overwrite(from_path, to_path):
    if os.path.exists(to_path):
        shutil.rmtree(to_path)
    shutil.copytree(from_path, to_path)

def main():

    args = parseArgs()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=args.verbose_level)

    logging.info("Qserv configuration tool\n"+
        "======================================================================="
    )

    qserv_dir = os.path.abspath(
                    os.path.join(
                        os.path.dirname(os.path.realpath(__file__)),
                        "..")
                )

    if 'prep' in args.step_list:
        template_config_dir = os.path.join( qserv_dir, "admin")

        logging.info("Initializing template configuration in {1} using {0}"
            .format(template_config_dir, args.qserv_run_dir)
        )

        if not args.force :
            if configure.user_yes_no_query(
                "WARNING : Do you want to erase all configuration" +
                " data in {0} ?".format(args.qserv_run_dir)
            ):
                copy_and_overwrite(template_config_dir, args.qserv_run_dir)
            else:
                logging.info("Stopping Qserv configuration, please specify an other configuration directory")
                sys.exit(1)

        for line in fileinput.input(args.meta_config_file, inplace = 1):
            print line.replace("run_base_dir =", "run_base_dir = " + args.qserv_run_dir),

    def intersect(l1, l2):
        l = []
        for elem in l2 :
            if elem in l1 :
                l.append(elem)
        return l

    def contains_configuration_step(step_list):
        return (len(intersect(step_list, configure.STEP_RUN_LIST)) > 0)
    

    ###################################
    #
    # Running configuration procedure
    #
    ###################################
    if  contains_configuration_step(args.step_list):

        try:
            logging.info("Reading meta-configuration file")
            config = commons.read_config(args.meta_config_file)
        except ConfigParser.NoOptionError, exc:
            logging.fatal("An option is missing in your configuration file: %s" % exc)
            sys.exit(1)

        if 'mkdirs' in args.step_list:
            logging.info("Defining main directory structure")
            configure.check_root_dirs()
            configure.check_root_symlinks()

        ##########################################
        #
        # Creating Qserv services configuration
        # using templates and meta_config_file
        #
        ##########################################
        if 'etc' in args.step_list:
            logging.info(
                "Creating configuration files in {0}".format(os.path.join(config['qserv']['run_base_dir'],"etc")) +
                " and scripts in {0}".format(os.path.join(config['qserv']['run_base_dir'],"tmp"))
            )
            template_root = os.path.join(config['qserv']['run_base_dir'],"templates", "server")
            dest_root = os.path.join(config['qserv']['run_base_dir'])
            configure.apply_templates(
                template_root,
                dest_root
            )

        components_to_configure = intersect(args.step_list, configure.COMPONENTS)
        if len(components_to_configure) > 0 :
            logging.info("Running configuration scripts")
            configuration_scripts_dir=os.path.join(config['qserv']['run_base_dir'],'tmp','configure')

            if not config['qserv']['node_type'] in ['mono','worker']:
                logging.info("Service isn't a worker or a mono-node instance : not configuring SciSQL")
                component_to_configure.remove('scisql')

            for c in components_to_configure:
                script = os.path.join( configuration_scripts_dir, c+".sh")
                commons.run_command(script)

        if 'client' in args.step_list:
            template_root = os.path.join(config['qserv']['run_base_dir'],"templates", "client")
            homedir = os.path.expanduser("~")
            dest_root = os.path.join(homedir, ".lsst")
            logging.info(
                "Creating client configuration file in {0}".format(dest_root)
            )
            configure.apply_templates(
                template_root,
                dest_root
            )

if __name__ == '__main__':
    main()
