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
groups:
- docker

users:
- name: qserv
  gecos: Qserv daemon
  groups: docker
  lock-passwd: true
  shell: /bin/bash
  sudo: ALL=(ALL) NOPASSWD:ALL

packages:
- docker
- util-linux

runcmd:
- ['systemctl', 'enable', 'docker']
- ['/tmp/detect_end_cloud_config.sh']

write_files:
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

package_upgrade: true
package_reboot_if_required: true
timezone: Europe/Paris
'''

    return userdata


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description='Create Openstack image containing Docker.')

        cloudmanager.add_parser_args(parser)
        args = parser.parse_args()

        loggerName = "Provisioner"
        cloudmanager.config_logger(loggerName, args.verbose, args.verboseAll)

        cloudManager = cloudmanager.CloudManager(config_file_name=args.configFile)

        userdata_snapshot = get_cloudconfig()

        instance_id = "source"
        instance_for_snapshot = cloudManager.nova_servers_create(instance_id, userdata_snapshot)

        # Wait for cloud config completion
        cloudManager.detect_end_cloud_config(instance_for_snapshot)

        cloudManager.nova_image_create(instance_for_snapshot)

        # Delete instance after taking a snapshot
        cloudManager.nova_servers_delete(instance_for_snapshot)

    except Exception as exc:
        logging.critical('Exception occured: %s', exc, exc_info=True)
        sys.exit(1)
