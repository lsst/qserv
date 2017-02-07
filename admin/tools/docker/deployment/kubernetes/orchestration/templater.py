#!/usr/bin/env python

"""
Lightweight template engine used to create
k8s pods configuration files

@author Fabrice Jammes, IN2P3
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import argparse
import ConfigParser
import logging
import sys
import warnings
import yaml

# ----------------------------
# Imports for other modules --
# ----------------------------

# -----------------------
# Exported definitions --
# -----------------------

# --------------------
# Local definitions --
# --------------------


def _config_logger(verbose):
    """
    Configure the logger
    """
    verbosity = len(verbose)
    levels = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}

    warnings.filterwarnings("ignore")

    logger = logging.getLogger()

    # create console handler and set level to debug
    console = logging.StreamHandler()
    # create formatter
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(name)-15s %(message)s')
    # add formatter to ch
    console.setFormatter(formatter)

    logger.handlers = [console]
    logger.setLevel(levels.get(verbosity, logging.DEBUG))


def _add_volume(host_dir, container_dir, volume_name):
    """
    Map host_dir to container_dir in pod configuration
    using volume technology
    @param host_dir: directory on host machine, function exit if None
    @param container_dir: directory in container
    @param volume_name: name of volume made containing host_dir
    """
    if host_dir:

        if 'volumeMounts' not in yaml_data['spec']['containers'][0]:
            yaml_data['spec']['containers'][0]['volumeMounts'] = []

        volume_mounts = yaml_data['spec']['containers'][0]['volumeMounts']
        volume_mount = {'mountPath': container_dir, 'name': volume_name}
        volume_mounts.append(volume_mount)

        if 'volumes' not in yaml_data['spec']:
            yaml_data['spec']['volumes'] = []

        volume = {'hostPath': {'path': host_dir},
                  'name': volume_name}
        volumes = yaml_data['spec']['volumes']
        volumes.append(volume)


if __name__ == "__main__":
    try:

        parser = argparse.ArgumentParser(description='Create k8s pods configuration file from template')

        parser.add_argument('-v', '--verbose', dest='verbose', default=[],
                            action='append_const', const=None,
                            help='More verbose output, can use several times.')
        parser.add_argument('-i', '--ini', dest='iniFile',
                            required=True, metavar='PATH',
                            help='ini file used to fill yaml template')
        parser.add_argument('-t', '--template', dest='templateFile',
                            required=True, metavar='PATH',
                            help='yaml template file')
        parser.add_argument('-o', '--output', dest='yamlFile',
                            required=True, metavar='PATH',
                            help='pod configuration file, in yaml')

        args = parser.parse_args()

        _config_logger(args.verbose)

        config = ConfigParser.RawConfigParser()

        with open(args.iniFile, 'r') as f:
            config.readfp(f)

        with open(args.templateFile, 'r') as f:
            yaml_data = yaml.load(f)

        yaml_data['metadata']['name'] = config.get('spec', 'pod_name')
        yaml_data['spec']['hostname'] = config.get('spec', 'pod_name')

        yaml_data['spec']['containers'][0]['name'] = config.get('spec', 'pod_name')
        yaml_data['spec']['containers'][0]['image'] = config.get('spec', 'image')
        yaml_data['spec']['nodeSelector']['kubernetes.io/hostname'] = config.get('spec', 'host')

        _add_volume(config.get('spec', 'host_data_dir'), '/qserv/data', 'data-volume')
        _add_volume(config.get('spec', 'host_custom_dir'), '/qserv/custom', 'custom-volume')
        _add_volume(config.get('spec', 'host_log_dir'), '/qserv/run/var/log', 'log-volume')
        _add_volume(config.get('spec', 'host_tmp_dir'), '/qserv/run/tmp', 'tmp-volume')

        with open(args.yamlFile, 'w') as f:
            f.write( yaml.dump(yaml_data, default_flow_style=False))

    except Exception as exc:
        logging.critical('Exception occurred: %s', exc, exc_info=True)
        sys.exit(1)
