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
import subprocess
import sys
import time
import warnings

# ----------------------------
# Imports for other modules --
# ----------------------------

from keystoneauth1 import loading
from keystoneauth1 import session
from novaclient import client
import glanceclient
import novaclient.exceptions

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------

_OPENSTACK_API_VERSION = '2'
_OPENSTACK_VERIFY_SSL = False

def _get_nova_creds():
    """
    Extract the login information from the environment
    """
    creds = {}
    creds['username'] = os.environ['OS_USERNAME']
    creds['password'] = os.environ['OS_PASSWORD']
    creds['auth_url'] = os.environ['OS_AUTH_URL']
    creds['project_id'] = os.environ['OS_TENANT_ID']
    return creds

# -----------------------
# Exported definitions --
# -----------------------

BASE_IMAGE_KEY='base_image_name'
SNAPSHOT_IMAGE_KEY='snapshot_name'

# Profile used to build cloud-init file
DOCKER_NODE = "docker_node"
SWARM_MANAGER = "swarm_manager"
SWARM_NODE = "swarm_node"

def add_parser_args(parser):
    """
    Configure the parser
    """
    parser.add_argument('-v', '--verbose', dest='verbose', default=[], action='append_const',
                        const=None, help='More verbose output, can use several times.')
    parser.add_argument('--verbose-all', dest='verboseAll', default=False, action='store_true',
                        help='Apply verbosity to all loggers, by default only loader level is set.')
    parser.add_argument('-C', '--cleanup', dest='cleanup', default=False, action='store_true',
                        help='Clean images and instances potentially created during a previous run')
    # parser = lsst.qserv.admin.logger.add_logfile_opt(parser)
    group = parser.add_argument_group('Cloud configuration options',
                                       'Options related to cloud-platform access and management')
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
        # Disable dependencies loggers and warnings
        for logger_name in ["keystoneauth", "novaclient", "requests", "stevedore", "urllib3"]:
            logging.getLogger(logger_name).setLevel(logging.ERROR)
        warnings.filterwarnings("ignore")

    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(name)-15s'
                               ' %(message)s',
                        level=levels.get(verbosity, logging.DEBUG))



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

        self._creds = _get_nova_creds()
        logging.debug("Openstack user: %s", self._creds['username'])
        self._safe_username = self._creds['username'].replace('.', '')

        default_instance_prefix = "{0}-qserv-".format(self._safe_username)

        config = ConfigParser.ConfigParser({'instance-prefix': default_instance_prefix,
                                            'net-id': None,
                                            'ssh-private-key': '~/.ssh/id_rsa',
                                            'ssh_security_group': None})

        with open(config_file_name, 'r') as config_file:
            config.readfp(config_file)

        self._session = self._create_keystone_session()

        self.nova = client.Client(_OPENSTACK_API_VERSION, session=self._session)
        base_image_name = config.get('openstack', used_image_key)
        self.snapshot_name = config.get('openstack', 'snapshot_name')
        try:
            self.image = self.nova.images.find(name=base_image_name)
        except novaclient.exceptions.NoUniqueMatch:
            logging.critical("Image %s not unique", base_image_name)
            sys.exit(1)
        except novaclient.exceptions.NotFound:
            logging.critical("Image %s do not exist. Create it first", base_image_name)
            sys.exit(1)

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
            self.key_filename = config.get('openstack', 'ssh-private-key')
            if not self.key_filename:
                raise ValueError("Unspecified ssh private key")
            self._manage_ssh_key()
        else:
            self.key = None
            self.key_filename = None

        self._hostname_tpl = config.get('openstack', 'instance-prefix')
        if not self._hostname_tpl:
            raise ValueError("Unspecified server name prefix")

    def get_hostname_tpl(self):
        """
        All instances names created by current CloudManager instance
        start with returned string
        """
        return self._hostname_tpl

    def _create_keystone_session(self):
        """
        Return a keystone session used to
        connect to other Openstack services
        """
        loader = loading.get_plugin_loader('password')
        auth = loader.load_from_options(**self._creds)
        sess = session.Session(auth=auth, verify=_OPENSTACK_VERIFY_SSL)
        return sess

    def get_safe_username(self):
        """
        Returns cleaned Openstack user login (special characters, like -, are removed)
        """
        return self._safe_username

    def nova_snapshot_create(self, instance):
        """
        Shutdown instance and snapshot it to an image named self.snapshot_name
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

    def nova_snapshot_find(self):
        """
        Returns and Openstack image named self.snapshot_name. Exit with error code if image is not unique.
        @throw novaclient.exceptions.NoUniqueMatch if image is not unique.
        """
        try:
            snapshot = self.nova.images.find(name=self.snapshot_name)
        except novaclient.exceptions.NotFound:
            snapshot = None
        except novaclient.exceptions.NoUniqueMatch:
            logging.critical("Qserv snapshot %s not unique, manual cleanup required", self.snapshot_name)
            sys.exit(1)
        return snapshot

    def nova_snapshot_delete(self, snapshot):
        """
        Delete an Openstack image from Glance
        :param snapshot: image to delete
        """
        glance = glanceclient.Client(_OPENSTACK_API_VERSION, session=self._session)
        glance.images.delete(snapshot.id)

    def _build_instance_name(self, instance_id):
        """
        Build Openstack instance.
        First convert instance id to unicode and then concatenate instance name template with it
        :param instance_id: instance id
        :return:            instance name
        """
        if isinstance(instance_id, unicode):
            instance_id = instance_id.encode('ascii', 'ignore')
        instance_name = "{0}{1}".format(self._hostname_tpl, instance_id)
        return instance_name

    def nova_servers_create(self, instance_id, userdata):
        """
        Boot an instance and returns when its status is "ACTIVE"
        """
        instance_name = self._build_instance_name(instance_id)
        logging.info("Launch an instance %s", instance_name)

        # Launch an instance from an image
        instance = self.nova.servers.create(name=instance_name,
                                            image=self.image,
                                            flavor=self.flavor,
                                            userdata=userdata,
                                            key_name=self.key,
                                            nics=self.nics)
        # Poll at 5 second intervals, until the status is 'ACTIVE'
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
        Wait for cloud-init completion
        """
        check_word = "---SYSTEM READY FOR SNAPSHOT---"
        end_word = None
        while not end_word:
            time.sleep(10)
            output = instance.get_console_output()
            logging.debug("console output: %s", output)
            logging.debug("instance: %s",instance)
            end_word = re.search(check_word, output)

    def nova_servers_cleanup(self, last_instance_id):
        """
        Shut down and delete all Qserv servers
        belonging to current Openstack user.

        Raise exception if instance name prefix is empty.
        """
        if not self._hostname_tpl:
            raise ValueError("Instance prefix is empty")
        for server in self.nova.servers.list():
            # server_name must be ascii
            if server.name.startswith(self._hostname_tpl):
                logging.debug("Cleanup existing instance %s", server.name)
                server.delete()


    def _manage_ssh_key(self):
        """
        Upload user ssh public key on Openstack instances
        """
        logging.info('Manage ssh keys: %s', self.key)
        if self.nova.keypairs.findall(name=self.key):
            logging.debug('Remove previous ssh keys')
            self.nova.keypairs.delete(key=self.key)

        with open(os.path.expanduser(self.key_filename + ".pub")) as fpubkey:
            self.nova.keypairs.create(name=self.key, public_key=fpubkey.read())

    def get_floating_ip(self):
        """
        Return an available floating ip address
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
        Print ssh client configuration to local file
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
        Check if the ssh service started on all instances
        """
        for instance in instances:
            cmd = ['ssh', '-t', '-F', './ssh_config', instance.name, 'true']
            success = False
            nb_try = 0
            while not success:
                try:
                    subprocess.check_output(cmd)
                    success = True
                except subprocess.CalledProcessError as exc:
                    logging.warn("Waiting for ssh to be available on %s: %s", instance.name, exc.output)
                    nb_try += 1
                    if nb_try > 10:
                        logging.critical("No available ssh on %s OpenStack clean up is required", instance.name)
                        sys.exit(1)
                    time.sleep(2)
            logging.debug("ssh available on %s", instance.name)

    def update_etc_hosts(self, instances):
        """
        Update /etc/hosts file on each instances.
        Add each instance name and ip address inside it.
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
                subprocess.check_output(cmd)
            except subprocess.CalledProcessError as exc:
                logging.error("ERROR while updating /etc/hosts: %s", exc.output)
                sys.exit(1)

    def build_cloudconfig(self, server_profile=DOCKER_NODE, instance_last_id=""):
        """
        Build cloudconfig configuration for a given server profile

        @param server_profile:      Can be DOCKER_NODE, SWARM_NODE or SWARM_MANAGER
        @param instance_last_id:    Must not be empty if server_profile is SWARM_MANAGER
        """
        # cloud config
        cloud_config = '#cloud-config'

        cloud_config += '''

users:
- name: qserv
  gecos: Qserv daemon
  groups: docker
  lock-passwd: true
  shell: /bin/bash
  ssh-authorized-keys:
  - {key}
  sudo: ALL=(ALL) NOPASSWD:ALL'''

        if server_profile == SWARM_MANAGER:
            cloud_config += '''

packages:
- git'''

        cloud_config += '''

runcmd:
  - [/tmp/detect_end_cloud_config.sh]
'''

        if server_profile == SWARM_NODE:
            cloud_config += '''
  # Option below crash mysqld inside Qservcontainer, but is required by Swarm manager???
  #- [sed, -i, '/--exec-opt native.cgroupdriver=systemd/d', /usr/lib/systemd/system/docker.service]
  - [sed, -i, 's,ExecStart=/usr/bin/docker daemon -H fd://,ExecStart=/usr/bin/docker daemon -H unix:///var/run/docker.sock -H tcp://0.0.0.0:2375 --storage-driver=overlay,', /usr/lib/systemd/system/docker.service]
  # Data and log are stored on Openstack host
  - [mkdir, -p, /qserv/data]
  - [mkdir, -p, /qserv/log]
  - [chown, -R, qserv, /qserv]
'''

        cloud_config += '''
  - [/bin/systemctl, daemon-reload]
  - [/bin/systemctl, restart,  docker.service]'''

        fpubkey = open(os.path.expanduser(self.key_filename + ".pub"))
        public_key = fpubkey.read()

        userdata = cloud_config.format(instance_last_id=instance_last_id,
                                       key=public_key,
                                       hostname_tpl=self._hostname_tpl)

        logging.debug("cloud-config userdata: \n%s", userdata)
        return userdata


