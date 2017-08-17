#!/usr/bin/env python

"""
Create an image containing Docker by taking
a snapshot from an instance,

Script performs these tasks:
  - launch instance from image
  - install docker via cloud-init
  - create a qserv user
  - take a snapshot
  - shut down and delete the instance created

@author  Oualid Achbal, IN2P3

"""

from __future__ import absolute_import, division, print_function

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import argparse
import logging
import sys

# ----------------------------
# Imports for other modules --
# ----------------------------
import cloudmanager

# -----------------------
# Exported definitions --
# -----------------------


def get_cloudconfig():
    """
    Return cloud init configuration in a string
    """
    userdata = '''
#cloud-config

write_files:
- path: "/etc/yum.repos.d/docker.repo"
  permissions: "0544"
  owner: "root"
  content: |
    [dockerrepo]
    name=Docker Repository
    baseurl=https://yum.dockerproject.org/repo/main/centos/7/
    enabled=1
    gpgcheck=1
    gpgkey=https://yum.dockerproject.org/gpg
- path: "/etc/yum.repos.d/kubernetes.repo"
  permissions: "0544"
  owner: "root"
  content: |
    [kubernetes]
    name=Kubernetes
    baseurl=http://yum.kubernetes.io/repos/kubernetes-el7-x86_64
    enabled=1
    gpgcheck=1
    repo_gpgcheck=1
    gpgkey=https://packages.cloud.google.com/yum/doc/yum-key.gpg
           https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg
- path: "/tmp/detect_end_cloud_config.sh"
  permissions: "0544"
  owner: "root"
  content: |
    #!/bin/sh
    (while [ ! -f /var/lib/cloud/instance/boot-finished ] ;
    do
      sleep 2
      echo "---CLOUD-INIT-DETECT RUNNING---"
    done
    sync
    fsfreeze -f / && read x; fsfreeze -u /
    echo "---SYSTEM READY FOR SNAPSHOT---") &

groups:
- docker

packages:
- [docker-engine, 1.12.3-1.el7.centos]
- ebtables
- epel-release
- [kubeadm, 1.6.2-0]
- [kubectl, 1.6.2-0]
- [kubelet, 1.6.2-0]
- [kubernetes-cni, 0.5.1-0]
- parallel
- util-linux

runcmd:
- ['systemctl', 'enable', 'docker']
- ['systemctl', 'enable', 'kubelet']
- ['curl', '-O', 'http://linuxsoft.cern.ch/cern/centos/7/cern/x86_64/Packages/parallel-20150522-1.el7.cern.noarch.rpm']
- ['yum', '--assumeyes', '--nogpgcheck', 'localinstall', 'parallel-20150522-1.el7.cern.noarch.rpm' ]
- ['/tmp/detect_end_cloud_config.sh']

package_upgrade: true
package_reboot_if_required: true
timezone: Europe/Paris

final_message: "The system is finally up, after $UPTIME seconds"
'''

    return userdata


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description='Create Openstack image containing Docker.')

        cloudmanager.add_parser_args(parser)
        args = parser.parse_args()

        cloudmanager.config_logger(args.verbose, args.verboseAll)

        cloudManager = cloudmanager.CloudManager(
            config_file_name=args.configFile,
            create_snapshot=True)

        userdata_snapshot = get_cloudconfig()

        previous_snapshot = cloudManager.nova_snapshot_find()

        if args.cleanup:
            if previous_snapshot is not None:
                logging.debug("Removing previous snapshot: %s", cloudManager.snapshot_name)
                cloudManager.nova_snapshot_delete(previous_snapshot)
        elif previous_snapshot is not None:
            logging.critical("Destination snapshot: %s already exist", cloudManager.snapshot_name)
            sys.exit(1)

        instance_id = "source"
        instance_for_snapshot = cloudManager.nova_servers_create(
            instance_id, userdata_snapshot, cloudManager.snapshot_flavor)

        # Wait for cloud config completion
        cloudManager.detect_end_cloud_config(instance_for_snapshot)

        cloudManager.nova_snapshot_create(instance_for_snapshot)

        # Delete instance after taking a snapshot
        instance_for_snapshot.delete()

    except Exception as exc:
        logging.critical('Exception occured: %s', exc, exc_info=True)
        sys.exit(1)
