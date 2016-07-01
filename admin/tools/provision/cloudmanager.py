#!/usr/bin/env python

"""
Tools to ease OpenStack infrastructure configuration
and provisioning

@author  Oualid Achbal, IN2P3

"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import ConfigParser
import logging
import os
import re
from subprocess import CalledProcessError, check_output
import sys
import time
import warnings

# ----------------------------
# Imports for other modules --
# ----------------------------
from novaclient import client
import novaclient.exceptions

# -----------------------
# Exported definitions --
# -----------------------

BASE_IMAGE_KEY='base_image_name'
SNAPSHOT_IMAGE_KEY='snapshot_name'

def get_nova_creds():
    """
    Extract the login information from the environment
    """
    creds = {}
    creds['version'] = 2
    creds['username'] = os.environ['OS_USERNAME']
    creds['api_key'] = os.environ['OS_PASSWORD']
    creds['auth_url'] = os.environ['OS_AUTH_URL']
    creds['project_id'] = os.environ['OS_TENANT_NAME']
    creds['insecure'] = True
    return creds

def add_parser_args(parser):
    """
    Configure the parser
    """
    parser.add_argument('-v', '--verbose', dest='verbose', default=[], action='append_const',
                        const=None, help='More verbose output, can use several times.')
    parser.add_argument('--verbose-all', dest='verboseAll', default=False, action='store_true',
                        help='Apply verbosity to all loggers, by default only loader level is set.')
    # parser = lsst.qserv.admin.logger.add_logfile_opt(parser)
    group = parser.add_argument_group('Cloud configuration options',
                                       'Options defining parameters to access remote cloud-platform')

    group.add_argument('-f', '--config', dest='configFile',
                        required=True, metavar='PATH',
                        help='Add cloud config file which contains instance characteristics')

    return parser

def config_logger(loggerName, verbose, verboseAll):
    """
    Configure the logger
    """
    verbosity = len(verbose)
    levels = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    if not verboseAll:
        # suppress INFO/DEBUG regular messages from other loggers
        # Disable requests and urllib3 package logger and warnings
        logging.getLogger("requests").setLevel(logging.ERROR)
        logging.getLogger("urllib3").setLevel(logging.ERROR)

    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(name)-15s'
                               ' %(message)s',
                        level=levels.get(verbosity, logging.DEBUG))

    warnings.filterwarnings("ignore")


class CloudManager(object):
    """Application class for common definitions for creation of snapshot and provision qserv"""

    def __init__(self, config_file_name, used_image_key=BASE_IMAGE_KEY, add_ssh_key=False):
        """
        Constructor parse all arguments

        @param config_file_name define the configuration file containing cloud parameters
        @param used_image_key Add a key to choose the appropriate image (base image or snapshot)
        @param add_ssh_key Add ssh key only while launching instances in provision qserv.
        """

        logging.debug("Use configuration file: %s", config_file_name)

        config = ConfigParser.ConfigParser({'net-id': None,
                                            'ssh_security_group': None})

        with open(config_file_name, 'r') as config_file:
            config.readfp(config_file)

        self._creds = get_nova_creds()
        logging.debug("Openstack user: %s", self._creds['username'])
        self._safe_username = self._creds['username'].replace('.', '')
        self.nova = client.Client(**self._creds)

        base_image_name = config.get('openstack', used_image_key)
        self.snapshot_name = config.get('openstack', 'snapshot_name')
        self.image = self.nova.images.find(name=base_image_name)

        flavor_name = config.get('openstack', 'flavor_name')
        self.flavor = self.nova.flavors.find(name=flavor_name)

        self.network_name = config.get('openstack', 'network_name')
        if config.get('openstack', 'net-id'):
            self.nics = [{'net-id': config.get('openstack', 'net-id')}]
        else:
            self.nics = []

        self.ssh_security_group = config.get('openstack', 'ssh_security_group')

        # Upload ssh public key
        if add_ssh_key:
            self.key = "{}-qserv".format(self._safe_username)
        else:
            self.key = None

        self.key_filename = '~/.ssh/id_rsa'

    def nova_image_create(self, instance):
        """
        Shutdown instance and snapshot it
        to an image named self.snapshot_name
        """
        instance.stop()
        while instance.status != 'SHUTOFF':
            time.sleep(5)
            instance.get()

        logging.info("Creating Qserv snapshot '%s'", self.snapshot_name)
        qserv_image = instance.create_image(self.snapshot_name)
        status = None
        while status != 'ACTIVE':
            time.sleep(5)
            status = self.nova.images.get(qserv_image).status
        logging.info("SUCCESS: Qserv image '%s' is active", self.snapshot_name)

    def nova_servers_create(self, instance_id, userdata):
        """
        Boot an instance and check status
        """
        instance_name = "{0}-qserv-{1}".format(self._safe_username, instance_id)
        logging.info("Launch an instance %s", instance_name)

        # Launch an instance from an image
        instance = self.nova.servers.create(name=instance_name,
                                            image=self.image,
                                            flavor=self.flavor,
                                            userdata=userdata,
                                            key_name=self.key,
                                            nics=self.nics)
        # Poll at 5 second intervals, until the status is no longer 'BUILD'
        status = instance.status
        while status != 'ACTIVE':
            time.sleep(5)
            instance.get()
            status = instance.status
        logging.info("status: %s", status)
        logging.info("Instance %s is active", instance_name)

        return instance

    def detect_end_cloud_config(self, instance):
        """
        Add clean wait for cloud-init completion
        """
        check_word = "---SYSTEM READY FOR SNAPSHOT---"
        end_word = None
        while not end_word:
            time.sleep(10)
            output = instance.get_console_output()
            logging.debug("console output: %s", output)
            logging.debug("instance: %s",instance)
            end_word = re.search(check_word, output)

    def nova_servers_delete(self, server):
        """
        Shut down and delete a server
        """
        server.delete()

    def manage_ssh_key(self):
        """
        Upload ssh public key
        """
        logging.info('Manage ssh keys: %s', self.key)
        if self.nova.keypairs.findall(name=self.key):
            logging.debug('Remove previous ssh keys')
            self.nova.keypairs.delete(key=self.key)

        with open(os.path.expanduser(self.key_filename + ".pub")) as fpubkey:
            self.nova.keypairs.create(name=self.key, public_key=fpubkey.read())

    def get_floating_ip(self):
        """
        Allocate floating ip address to project
        """
        floating_ips = self.nova.floating_ips.list()
        floating_ip = None
        floating_ip_pool = self.nova.floating_ip_pools.list()[0].name

        # Check for available public ip address in project
        for ip in floating_ips:
            if ip.instance_id is None:
                floating_ip = ip
                logging.debug('Available floating ip found %s', floating_ip)
                break

        # Check for available public ip address in ext-net pool
        if floating_ip is None:
            try:
                logging.debug("Use floating ip pool: %s", floating_ip_pool)
                floating_ip = self.nova.floating_ips.create(floating_ip_pool)
            except novaclient.exceptions.Forbidden as exc:
                logging.fatal("Unable to retrieve public IP: %s", exc)
                sys.exit(1)

        return floating_ip

    def print_ssh_config(self, instances, floating_ip):
        """
        Print ssh client configuration to file
        """
        # ssh config
        ssh_config_tpl = '''
        Host {host}
        HostName {fixed_ip}
        User qserv
        Port 22
        StrictHostKeyChecking no
        UserKnownHostsFile /dev/null
        PasswordAuthentication no
        ProxyCommand ssh -i {key_filename} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -W %h:%p qserv@{floating_ip}
        IdentityFile {key_filename}
        IdentitiesOnly yes
        LogLevel FATAL
        '''
        ssh_config_extract = ""
        for instance in instances:
            fixed_ip = instance.networks[self.network_name][0]
            ssh_config_extract += ssh_config_tpl.format(host=instance.name,
                                                        fixed_ip=fixed_ip,
                                                        floating_ip=floating_ip.ip,
                                                        key_filename=self.key_filename)
        logging.debug("Create SSH client config ")

        f = open("ssh_config", "w")
        f.write(ssh_config_extract)
        f.close()

    def check_ssh_up(self, instances):
        """
        Check if the ssh service started
        """
        for instance in instances:
            cmd = ['ssh', '-t', '-F', './ssh_config', instance.name, 'true']
            success = False
            nb_try = 0
            while not success:
                try:
                    check_output(cmd)
                    success = True
                except CalledProcessError as exc:
                    logging.warn("Waiting for ssh to be available on %s: %s", instance.name, exc.output)
                    nb_try += 1
                    if nb_try > 10:
                        logging.critical("No available ssh on %s OpenStack clean up is required", instance.name)
                        sys.exit(1)
                    time.sleep(2)
            logging.debug("ssh available on %s", instance.name)

    def update_etc_hosts(self, instances):
        """
        Update /etc/hosts file on each virtual machine
        """
        hostfile_tpl = "{ip}    {host}\n"

        hostfile = ""
        for instance in instances:
            # Collect IP adresses
            fixed_ip = instance.networks[self.network_name][0]
            hostfile += hostfile_tpl.format(host=instance.name, ip=fixed_ip)

        for instance in instances:
            cmd = ['ssh', '-t', '-F', './ssh_config', instance.name,
                   'sudo sh -c "echo \'{hostfile}\' >> /etc/hosts"'.format(hostfile=hostfile)]
            try:
                check_output(cmd)
            except CalledProcessError as exc:
                logging.error("ERROR while updating /etc/hosts: %s", exc.output)
                sys.exit(1)


