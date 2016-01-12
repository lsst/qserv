MASTER=fjammes-qserv-1
WORKERS=$(echo fjammes-qserv-{2..3})

DOCKER_ORG=qserv
VERSION=dev
MASTER_IMAGE="$DOCKER_ORG/qserv:${VERSION}_master_$MASTER"
WORKER_IMAGE="$DOCKER_ORG/qserv:${VERSION}_worker_$MASTER"

CSS_FILE=nodes.css
