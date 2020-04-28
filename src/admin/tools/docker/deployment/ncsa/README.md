Check comment in each script header to understand its role.

# Intialize Qserv cluster

```shell
cp env.example.sh env.sh
cd admin
# create /qserv/log on all nodes
./mkdir.sh
cd ..
./run-multinode-test.sh
./stop.sh -K
cd admin
# Copy MySQL/Qserv data
# from container to /qserv/data on all host machines
./extract-data.sh
# Uncomment 'HOST_DATA_DIR=/qserv/data' in env.sh
./stop.sh
# Run multinode tests using /qserv/data on all host machines
./run-multinode-test.sh
```

# Manage Qserv cluster

```shell
# Edit 'BRANCH=master' in env.sh to set tag of your Docker image for Qserv
./stop.sh
./status.sh
# Launch Qserv and run multinode tests
./run-multinode-test.sh
./stop.sh
# Launch Qserv
./run.sh
```
