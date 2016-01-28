MASTER=qserv00.domain.org
WORKERS=$(echo qserv0{1..3}.domain.org)

DOCKER_ORG=qserv
# VERSION can be ticket branch but with _ instead of /
# example: u_fjammes_DM-4295 
VERSION=dev
MASTER_IMAGE="$DOCKER_ORG/qserv:${VERSION}_master_$MASTER"
WORKER_IMAGE="$DOCKER_ORG/qserv:${VERSION}_worker_$MASTER"

CSS_FILE=nodes.css
