# Rename this file to env.sh and edit variables
# Configuration file sourced by other scripts from the directory

# VERSION can be ia git ticket branch but with _ instead of /
# example: u_fjammes_DM-4295
VERSION=dev

# Set nodes names
MASTER=qserv00.domain.org
WORKERS=$(echo qserv0{1..3}.domain.org)

# Set images names
DOCKER_ORG=qserv
MASTER_IMAGE="$DOCKER_ORG/qserv:${VERSION}_master_$MASTER"
WORKER_IMAGE="$DOCKER_ORG/qserv:${VERSION}_worker_$MASTER"

CSS_FILE=nodes.css
