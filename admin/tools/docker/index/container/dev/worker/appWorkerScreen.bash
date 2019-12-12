#! /bin/bash
# admin/tools/docker/loader/container/dev/worker/appWorkerScreen.bash


screen -dm /home/qserv/dev/qserv/admin/tools/docker/index/container/dev/worker/appWorker.bash

tail -f /dev/null