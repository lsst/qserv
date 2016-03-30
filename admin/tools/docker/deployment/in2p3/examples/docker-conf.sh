set -x

/opt/shmux/bin/shmux -c "echo \"$PASSWORD\" | sudo -S sh -c 'mkdir -p /etc/systemd/system/docker.service.d \
    && cp ~fjammes/src/qserv-cluster/cluster-node/etc/systemd/system/docker.service.d/docker-opts.conf /etc/systemd/system/docker.service.d/ \
    && cp ~fjammes/src/qserv-cluster/cluster-node/etc/sysconfig/docker /etc/sysconfig/ \
    && systemctl daemon-reload && systemctl restart docker'" ccqserv{125..149}.in2p3.fr
