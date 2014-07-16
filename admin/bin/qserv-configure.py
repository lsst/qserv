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

    qserv_version=check_output(["qserv-version.sh"])
    qserv_version = qserv_version.strip(' \t\n\r')
    default_qserv_run_dir=os.path.join(os.path.expanduser("~"),"qserv-run",qserv_version)

    parser = argparse.ArgumentParser(
            description='''Qserv configuration tool. Creates an execution
directory (qserv_run_dir) which will contains configuration and execution
data for a given Qserv instance. Default behaviour will configure a mono-node
instance in ''' + default_qserv_run_dir + '''. IMPORTANT : --all MUST BE USED
FOR A  SETUP FROM SCRATCH.''',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
            )

    parser.add_argument("-a", "--all", dest="all", action='store_true',
            default=False,
            help='''clean execution directory and then run all configuration
steps'''
            )
    # Defining option of each configuration step
    for step_name in configure.STEP_LIST:
        parser.add_argument(
            "-{0}".format(step_name[0]),
            "--{0}".format(step_name),
            dest="step_list",
            action='append_const',
            const=step_name,
            help=configure.STEP_DOC[step_name]
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
        help="verbosity level"
        )

    # forcing options which may ask user confirmation
    parser.add_argument("-f", "--force", dest="force", action='store_true',
            default=False,
            help="forcing removal of existing execution data"
            )

    # run dir, all mutable data related to a qserv running instance are
    # located here
    parser.add_argument("-r", "--qserv-run-dir", dest="qserv_run_dir",
            default=default_qserv_run_dir,
            help="full path to qserv_run_dir"
            )

    # meta-configuration file whose parameters will be dispatched in Qserv
    # services configuration files
    args = parser.parse_args()
    default_meta_config_file = os.path.join(args.qserv_run_dir, "qserv.conf")
    parser.add_argument("-C", "--metaconfig",  dest="meta_config_file",
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

def recursive_copy(src, dest, ignore=None):
    if os.path.isdir(src):
        if not os.path.isdir(dest):
            os.makedirs(dest)
        files = os.listdir(src)
        if ignore is not None:
            ignored = ignore(src, files)
        else:
            ignored = set()
        for f in files:
            if f not in ignored:
                recursive_copy(os.path.join(src, f), 
                                    os.path.join(dest, f), 
                                    ignore)
    else:
        shutil.copyfile(src, dest)


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

    if configure.PREPARE in args.step_list:
        template_config_dir = os.path.join( qserv_dir, "admin")

        logging.info("Initializing template configuration in {1} using {0}"
            .format(template_config_dir, args.qserv_run_dir)
        )

        if os.path.exists(args.qserv_run_dir):
            if args.force or configure.user_yes_no_query(
                "WARNING : Do you want to erase all configuration" +
                " data in {0} ?".format(args.qserv_run_dir)
            ):
                shutil.rmtree(args.qserv_run_dir)
            else:
                logging.info("Stopping Qserv configuration, please specify an other configuration directory")
                sys.exit(1)
        
        recursive_copy(template_config_dir, args.qserv_run_dir)

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
        if configure.ETC in args.step_list:
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
                commons.run_command([script])

        if configure.CLIENT in args.step_list:
            template_file = os.path.join(
                config['qserv']['run_base_dir'], "templates", "server", "etc", "qserv-client.conf"
            )
            cfg_file = os.path.join(
                config['qserv']['run_base_dir'], "etc", "qserv-client.conf"
            )
            configure.apply_tpl(
                template_file,
                cfg_file
            )
            logging.info(
                "Client configuration file created : {0}".format(cfg_file)
            )
            homedir = os.path.expanduser("~")
            cfg_link = os.path.join(homedir, ".lsst", "qserv.conf")

            is_symlink_correct = os.path.exists(cfg_link) and os.path.samefile(cfg_link, cfg_file)

            if not is_symlink_correct:
                if args.force or configure.user_yes_no_query(
                    ("Do you want to update user configuration file " +
                     "(currently pointing on {0}) for new run directory?"
                        .format(os.path.realpath(cfg_link))
                    )
                ):
                    os.remove(cfg_link)
                    os.symlink(cfg_file, cfg_link)
                else:
                    logging.info("Client configuration unmodified.")
                    sys.exit(1)

            logging.info(
                "Client configuration is now pointing to : {0}".format(cfg_file)
            )

if __name__ == '__main__':
    main()
